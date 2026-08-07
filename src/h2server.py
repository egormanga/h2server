#!/usr/bin/env python3
# h2server

import abc
import asyncio
import collections
import dataclasses
from http import HTTPMethod, HTTPStatus
import io
import json

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import (
	ConnectionTerminated,
	DataReceived,
	RemoteSettingsChanged,
	RequestReceived,
	StreamEnded,
	StreamReset,
	WindowUpdated,
)
from h2.errors import ErrorCodes
from h2.exceptions import ProtocolError, StreamClosedError
from h2.settings import SettingCodes

class H2Protocol(asyncio.Protocol, abc.ABC):
	class _BaseBody:
		raw: io.BytesIO | None
		encoding: str | None

		@property
		def content(self) -> bytes:
			if (raw := self.raw) is None: return None
			return raw.getvalue()

		@content.setter
		def content(self, content: bytes | bytearray):
			if (raw := self.raw) is None: self.raw = io.BytesIO(content); return
			raw.seek(0)
			raw.truncate()
			raw.write(content)

		@property
		def text(self) -> str:
			if (content := self.content) is None: return None
			return content.decode(self.encoding)

		@text.setter
		def text(self, text: str):
			self.content = text.encode(self.encoding)

		@property
		def json(self):
			if (raw := self.raw) is None: return None
			return json.load(raw)

		@json.setter
		def json(self, data):
			if (raw := self.raw) is None: raw = self.raw = io.BytesIO()
			else: raw.seek(0); raw.truncate()
			json.dump(data, self.raw)

	@dataclasses.dataclass(kw_only=True, slots=True, weakref_slot=True)
	class Request(_BaseBody):
		method: HTTPMethod
		scheme: str | None
		authority: str | None
		path: str | None
		headers: dict[str, str]
		raw: io.BytesIO | None = None
		encoding: str | None = None

	@dataclasses.dataclass(slots=True, weakref_slot=True)
	class Response(_BaseBody):
		status: HTTPStatus
		headers: dict[str, str] = dataclasses.field(default_factory=dict)
		raw: io.BytesIO | None = None
		encoding: str | None = None

	def __init__(self, *, encoding: str = 'utf-8', **kwargs):
		self.encoding = encoding
		self.conn = H2Connection(config=H2Configuration(
			client_side=False,
			header_encoding=self.encoding,
			**kwargs
		))
		self.transport = None
		self.stream_data = {}
		self.flow_control_futures = {}

	@abc.abstractmethod
	def handle_request(self, request: Request) -> Response: ...

	def connection_made(self, transport: asyncio.Transport):
		self.transport = transport
		self.conn.initiate_connection()
		self.transport.write(self.conn.data_to_send())

	def connection_lost(self, exc: BaseException = None):
		if exc is None: exc = ConnectionAbortedError()
		for fut in self.flow_control_futures.values():
			fut.set_exception(exc)
		self.flow_control_futures.clear()

	def eof_received(self) -> bool | None:
		exc = ConnectionResetError()
		for fut in self.flow_control_futures.values():
			fut.set_exception(exc)
		self.flow_control_futures.clear()

	def data_received(self, data: bytes):
		try: events = self.conn.receive_data(data)
		except ProtocolError:
			self.transport.write(self.conn.data_to_send())
			self.transport.close()
		else:
			self.transport.write(self.conn.data_to_send())
			for event in events:
				match event:
					case RequestReceived(): self.request_received(event.stream_id, event.headers, event.stream_ended)
					case DataReceived(): self.receive_data(event.stream_id, event.data, event.flow_controlled_length, event.stream_ended)
					case StreamEnded(): self.stream_complete(event.stream_id)
					case ConnectionTerminated(): self.transport.close()
					case StreamReset(): self.stream_reset(event.stream_id, event.error_code, event.remote_reset)
					case WindowUpdated(): self.window_updated(event.stream_id, event.delta)
					case RemoteSettingsChanged() if SettingCodes.INITIAL_WINDOW_SIZE in event.changed_settings: self.window_updated()

				self.transport.write(self.conn.data_to_send())

	def request_received(self, stream_id: int, headers: list[tuple[str, str]], stream_ended: StreamEnded | None = None):
		headers = collections.OrderedDict(headers)
		self.stream_data[stream_id] = self.Request(
			method=headers.pop(':method'),
			scheme=headers.pop(':scheme', None),
			authority=headers.pop(':authority', None),
			path=headers.pop(':path', None),
			headers=headers,
			encoding=self.encoding,
		)

	def receive_data(self, stream_id: int, data: bytes, flow_controlled_length: int, stream_ended: StreamEnded | None = None):
		try: stream_data = self.stream_data[stream_id]
		except KeyError: self.conn.reset_stream(stream_id, error_code=ErrorCodes.PROTOCOL_ERROR); return

		if stream_data.raw is None: stream_data.raw = io.BytesIO()
		stream_data.raw.write(data)
		self.conn.acknowledge_received_data(flow_controlled_length, stream_id)

	def stream_complete(self, stream_id: int):
		try: request = self.stream_data[stream_id]
		except KeyError: return

		if (raw := request.raw) is not None: raw.seek(0)
		response = self.handle_request(request)

		headers = ((':status', str(response.status)), *response.headers.items())
		self.conn.send_headers(stream_id, headers, end_stream=(response.raw is None))
		if (raw := response.raw) is not None: asyncio.ensure_future(self.send_data(stream_id, raw.getvalue()))
		else: self.transport.write(self.conn.data_to_send())

	def stream_reset(self, stream_id: int, error_code: int, remote_reset: bool = True):
		if (fut := self.flow_control_futures.pop(stream_id, None)) is not None:
			fut.cancel()

	def window_updated(self, stream_id: int = 0, delta: int = 0):
		if not stream_id:
			for fut in self.flow_control_futures.values():
				fut.set_result(delta)
			self.flow_control_futures.clear()
		elif (fut := self.flow_control_futures.pop(stream_id, None)) is not None:
			fut.set_result(delta)

	async def send_data(self, stream_id: int, data: bytes):
		while data:
			while self.conn.local_flow_control_window(stream_id) < 1:
				fut = self.flow_control_futures[stream_id] = asyncio.Future()
				try: await fut
				except asyncio.CancelledError: return

			chunk_size = min(self.conn.local_flow_control_window(stream_id), self.conn.max_outbound_frame_size)
			fragment, data = data[:chunk_size], data[chunk_size:]

			try: self.conn.send_data(stream_id, fragment, end_stream=(not data))
			except (StreamClosedError, ProtocolError): break

			self.transport.write(self.conn.data_to_send())

# by Sdore, 2026
#  www.sdore.me
