from __future__ import annotations

import asyncio
import errno
import queue
import socket
import threading

import wsdatautil


class Connection(threading.Thread):
    server_addr: tuple[str, int]
    sock: socket.socket
    async_reader: asyncio.StreamReader
    response_queue: queue.Queue[bytes]
    ws_stream_reader: wsdatautil.ProgressiveStreamReader

    def __init__(
            self,
            server_host: str,
            server_port: int,
    ):
        threading.Thread.__init__(self)
        self.server_addr = (server_host, server_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.response_queue = queue.Queue()
        self.ws_stream_reader = wsdatautil.ProgressiveStreamReader(False)

    async def read_one_frame(self) -> wsdatautil.Frame:
        """Read one ws frame."""
        while True:
            var: int = 2
            while isinstance(var, int):
                var = self.ws_stream_reader.progressive_read(
                    await self.async_reader.readexactly(var)
                )
            var: wsdatautil.Frame
            match var.opcode:
                case wsdatautil.OPCODES.CLOSE:
                    raise EOFError
                case wsdatautil.OPCODES.PING:
                    pass
                case _:
                    return var

    async def read_frames_until_fin(self) -> list[wsdatautil.Frame]:
        """Read ws frames until a fin flag is reached."""
        frames: list[wsdatautil.Frame] = list()
        while not frames or not frames[-1].fin:
            frames.append(await self.read_one_frame())
        return frames

    async def read_iteration(self) -> None:
        frames = await self.read_frames_until_fin()
        payload = bytes().join(f.payload for f in frames)
        self.response_queue.put(payload)

    async def read_loop(self):
        try:
            while True:
                await self.read_iteration()
        except (asyncio.IncompleteReadError, EOFError):
            self.response_queue.put(b'')

    async def main(self):
        with self.sock:
            reader, _ = await asyncio.open_connection(sock=self.sock)
            self.async_reader = reader
            await self.read_loop()

    def connect(self) -> bytes:
        handshake = wsdatautil.HandshakeRequest()
        self.sock.connect(self.server_addr)
        self.sock.send(handshake.to_streamdata())
        handshake_res = b''
        while not handshake_res.endswith(b'\r\n\r\n'):
            handshake_res += self.sock.recv(32)
        return handshake_res

    def _connect(self) -> bytes:
        try:
            return self.connect()
        except OSError as e:
            if e.errno != errno.EISCONN:  # Transport endpoint is already connected
                raise

    def run(self):
        if not self.is_alive():
            self._connect()
        asyncio.run(self.main())

    def start(self):
        self._connect()
        super().start()

    def send(self, payload: bytes):
        self.sock.sendall(
            wsdatautil.Frame(
                payload,
                wsdatautil.OPCODES.BINARY,
                mask=None  # against RFC6455 (performance)
            ).to_streamdata()
        )

    def recv(self, block: bool = True, timeout: float | None = None) -> bytes:
        return self.response_queue.get(block, timeout)

    def close(self):
        self.sock.sendall(
            wsdatautil.FrameFactory.CloseFrame(wsdatautil.CLOSECODES.NORMAL_CLOSURE).to_streamdata()
        )

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
