from __future__ import annotations

import asyncio
import concurrent.futures
import errno
import socket as _socket
import sys
import threading
from time import sleep
from typing import Callable, Coroutine

import wsdatautil


class Mediation:
    mediator: Mediator
    pendant: Mediation
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    ws_stream_reader: wsdatautil.ProgressiveStreamReader

    def __init__(
            self,
            mediator: Mediator,
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
    ):
        self.mediator = mediator
        self.reader = reader
        self.writer = writer
        self.ws_stream_reader = wsdatautil.ProgressiveStreamReader("auto")
        self._alive = True

    async def read_one_frame(self) -> wsdatautil.Frame:
        """Read one ws frame."""
        while True:
            var: int = 2
            while isinstance(var, int):
                var = self.ws_stream_reader.progressive_read(
                    await self.reader.readexactly(var)
                )
            var: wsdatautil.Frame
            if var.opcode == wsdatautil.OPCODES.CLOSE:
                self.mediator.destroy(self, close_frame=var)
            return var

    async def read_frames_until_fin(self) -> list[wsdatautil.Frame]:
        """Read ws frames until a fin flag is reached."""
        frames: list[wsdatautil.Frame] = list()
        while not frames or not frames[-1].fin:
            frames.append(await self.read_one_frame())
        return frames

    async def run(self) -> None:
        try:
            while self._alive:
                for frame in await self.read_frames_until_fin():
                    frame = wsdatautil.Frame(frame.payload, frame.opcode, None, frame.fin)  # removes the masking
                    self.writer.write(frame.to_streamdata())
                    await self.writer.drain()
        finally:
            self.mediator.destroy(self)

    def close(self, close_frame: wsdatautil.Frame = None) -> bool:
        """close the connections and remove the Mediator object from the parent thread"""
        if v := self._alive:
            close_frame = close_frame or wsdatautil.FrameFactory.CloseFrame(wsdatautil.CLOSECODES.NORMAL_CLOSURE)
            self._alive = False
            try:
                self.reader.feed_data(b'\0\0')
            except AssertionError:
                pass
            self.reader.feed_eof()
            try:
                self.writer.write(close_frame.to_streamdata())
            except Exception as e:
                pass
            self.writer.close()
        return v


class Mediator:
    thread: MediatorsThread

    sock_a: _socket.socket
    sock_b: _socket.socket

    med_x: Mediation
    med_y: Mediation

    _alive: bool
    
    def __init__(
            self,
            thread: MediatorsThread,
            sock_a: _socket.socket,
            sock_b: _socket.socket,
    ):
        self.thread = thread
        self.sock_a = sock_a
        self.sock_b = sock_b
        self._alive = True

    async def run(self) -> None:
        await asyncio.gather(
            self.med_x.run(),
            self.med_y.run(),
            return_exceptions=True
        )

    async def ws_handshake(self, reader_z: asyncio.StreamReader, writer_z: asyncio.StreamWriter) -> None:
        """Wait for a ws handshake header and send the response back to the client."""
        hsdata = await reader_z.readuntil(b'\r\n\r\n')
        writer_z.write(
            wsdatautil.HandshakeRequest.from_streamdata(hsdata).make_response().to_streamdata()
        )
        await writer_z.drain()

    async def start(self) -> None:
        reader_a, writer_a = await asyncio.open_connection(sock=self.sock_a)
        reader_b, writer_b = await asyncio.open_connection(sock=self.sock_b)
        await self.ws_handshake(reader_a, writer_a)
        await self.ws_handshake(reader_b, writer_b)
        self.med_x = Mediation(self, reader_a, writer_b)
        self.med_y = Mediation(self, reader_b, writer_a)
        self.med_x.pendant = self.med_y
        self.med_y.pendant = self.med_x
        await self.run()

    def destroy(self, from_: Mediation = None, close_frame: wsdatautil.Frame = None) -> bool:
        """close the connections and remove the Mediator object from the parent thread"""
        from_ = from_ or self.med_x
        from_.close(close_frame)
        from_.pendant.close(close_frame)
        if v := self._alive:
            self._alive = False
            self.sock_a.close()
            self.sock_b.close()
            self.thread.mediators.discard(self)
        return v
        

class MediatorsThread(threading.Thread):
    mediators: set[Mediator]
    async_loop: asyncio.AbstractEventLoop
    _excl_thread: bool

    def __init__(self, _excl_thread: bool):
        threading.Thread.__init__(
            self,
            daemon=True,
        )
        self.mediators = set()
        self._excl_thread = _excl_thread

    def run(self) -> None:
        """run the async loop in this thread"""
        self.async_loop = asyncio.new_event_loop()
        self.async_loop.run_forever()
        self.async_loop.run_until_complete(self.async_loop.shutdown_asyncgens())

    async def add_med(self, sock_a: _socket.socket, sock_b: _socket.socket) -> None:
        """Add a Mediator to this thread."""
        self.mediators.add(
            med := Mediator(
                self,
                sock_a,
                sock_b,
            )
        )
        self._coro_run(med.start())

    def rm_med(self, med: Mediator):
        self.mediators.discard(med)
        if self._excl_thread:
            self.destroy()

    def destroy(self) -> None:
        """close all connections of the thread and stop the async loop"""
        for task in asyncio.all_tasks(self.async_loop):
            task.cancel()
        for med in self.mediators.copy():
            med.destroy()
        self.async_loop.call_soon_threadsafe(self.async_loop.stop, )  # type-hint-bug: Unpack[_Ts]

    def _coro_run(self, coro):
        fut = asyncio.run_coroutine_threadsafe(
            coro,
            self.async_loop
        )
        fut.add_done_callback(
            self._coro_done
        )
        return fut

    def _coro_done(self, fut: concurrent.futures.Future):
        try:
            fut.result()
        except concurrent.futures.CancelledError:
            pass


class Service(threading.Thread):

    socket: _socket.socket
    address: tuple[str, int]
    threads: set[MediatorsThread]
    _med_req_: Callable[[_socket.socket, _socket.socket], None | Coroutine]
    async_loop: asyncio.AbstractEventLoop
    _alive: bool = True

    def __init__(
            self,
            socket: tuple[str, int] | _socket.socket,
            threads: int = 1,
            mediations_per_thread: int = 0,
    ):
        """
        :param socket: an established IPv4 socket or an address tuple
        :param threads: Number of sub-threads or 0 if a separate thread is to be created for each mediation.
        :param mediations_per_thread: Limit the number of mediation per thread. Numbers less than 1 correspond to no limit (default).
        """
        threading.Thread.__init__(self)
        self.threads = set()

        if isinstance(socket, _socket.socket):
            self.socket = socket
            self.address = self.socket.getsockname()
        else:
            self.socket = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            self.socket.bind(socket)
            self.address = socket

        if threads:
            for _ in range(max(1, threads)):
                ct = MediatorsThread(_excl_thread=False)
                ct.start()
                self.threads.add(ct)
    
            if mediations_per_thread > 0:
                async def _med_req_(sock_a: _socket.socket, sock_b: _socket.socket):
                    for ct in self.threads:
                        if len(ct.mediators) < mediations_per_thread:
                            await ct.add_med(sock_a, sock_b)
                            break
                    else:
                        sock_a.close()
                        sock_b.close()
            else:
                async def _med_req_(sock_a: _socket.socket, sock_b: _socket.socket):
                    await min(self.threads, key=lambda c: len(c.mediators)).add_med(sock_a, sock_b)
        
        else:
            async def _med_req_(sock_a: _socket.socket, sock_b: _socket.socket):
                ct = MediatorsThread(_excl_thread=True)
                ct.start()
                self.threads.add(ct)
                while True:
                    try:
                        _ = ct.async_loop
                        break
                    except AttributeError:
                        sleep(.001)
                await ct.add_med(sock_a, sock_b)

        self._med_req_ = _med_req_

    async def mediation_request(self, sock_a: _socket.socket, sock_b: _socket.socket) -> None:
        """process a mediation request"""
        await self._med_req_(sock_a, sock_b)

    async def serve(self) -> None:
        """open the socket and run the mainloop"""
        self.async_loop = asyncio.get_event_loop()
        self.socket.listen()
        with self.socket:
            while self._alive:
                sock_a, _ = self.socket.accept()
                sock_b, _ = self.socket.accept()
                await self.mediation_request(sock_a, sock_b)

    def run(self) -> None:
        """asyncio.run(self.serve())"""
        try:
            return asyncio.run(self.serve())
        except asyncio.CancelledError:
            pass
        except OSError as e:
            if e.errno != errno.EBADF:  # Bad file descriptor
                raise

    def start(self, wait_iterations: int | bool = False, wait_time: float = .001):
        super().start()
        if wait_iterations:
            for i in range((1000 if isinstance(wait_iterations, bool) else wait_iterations)):
                try:
                    if self.async_loop.is_running():
                        return
                except AttributeError:
                    pass
                sleep(wait_time)
            else:
                raise TimeoutError

    def shutdown(self) -> None:
        """Close all connections and shut down the server."""

        self._alive = False

        async def _med_req_(*args):
            pass

        self._med_req_ = _med_req_
        try:
            with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as sock:
                sock.settimeout(.1)
                sock.connect(self.address)
        except Exception as e:
            print(self, "[ FAIL ]", e, "@ shutdown/connecting to own socket", file=sys.stderr)
        try:
            with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as sock:
                sock.settimeout(.1)
                sock.connect(self.address)
        except Exception as e:
            print(self, "[ FAIL ]", e, "@ shutdown/connecting to own socket", file=sys.stderr)
        try:
            for task in asyncio.all_tasks(self.async_loop):
                task.cancel()
        except RuntimeError as e:
            print(self, "[ FAIL ]", e, "@ shutdown/cancel tasks", file=sys.stderr)
        try:
            self.socket.close()
        except Exception as e:
            print(self, "[ FAIL ]", e, "@ shutdown/close socket", file=sys.stderr)
        for thread in self.threads.copy():
            try:
                thread.destroy()
            except Exception as e:
                print(self, "[ FAIL ]", e, f"@ shutdown/destroy thread {thread}", file=sys.stderr)














































