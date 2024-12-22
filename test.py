from __future__ import annotations

import base64
import sys
import pathlib
from warnings import filterwarnings

try:
    sys.path.insert(0, str(pathlib.Path(__file__).parent))
except Exception:
    raise

import asyncio
import json
import queue
import socket
import sqlite3
import threading
from collections import OrderedDict
from collections.abc import Sequence, Mapping, Iterable
from http.client import responses
from random import randbytes
from traceback import format_exception
from typing import Callable, Coroutine, Any, Generator, Literal, IO, Hashable
from uuid import uuid4
import time
import multiprocessing
import random

import unittest

from src import wsbridge
from src.wsbridge import baseclient, simpleservice


filterwarnings(
    'ignore',
    message=r'^unclosed.*',
    module="base_events",
    category=ResourceWarning
)


def log_ok(*msg: object):
    print("", end="", flush=True)
    print("\x1b[32mOK:", *msg, "\x1b[m", flush=True)


def _intro():
    log_ok(f"""
known warnings:
    - "ResourceWarning: unclosed ..." (conflict with test environment)
    - "ResourceWarning: Enable tracemalloc to get the object allocation traceback" (conflict with test environment)
    - "Task was destroyed but it is pending!" (shutdown bug)
    - "concurrent.futures._base.CancelledError" (shutdown bug)
    """.strip())

    for _ in range(3):
        time.sleep(.5)
        print(".", end="", flush=True)
    print(flush=True)


_intro()


class _TestConnection(baseclient.Connection):

    def __init__(self, server_host: str, server_port: int):
        super().__init__(server_host, server_port)
        self.connected = False
        self.exp = False

    def connect(self) -> bytes:
        try:
            self.connected = super().connect()
            return self.connected
        except Exception as e:
            self.exp = e
            self.connected = -1

    def run(self):
        self.connect()
        super().run()

    def start(self):
        super(baseclient.Connection, self).start()

    def wait_connected(self):
        while not self.connected:
            time.sleep(.001)
        if self.exp:
            raise self.exp


class TestBasics(unittest.TestCase):

    def test_single(self):
        port = 9990
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("localhost", port))
        server = wsbridge.SimpleService(sock, mediations_per_thread=1)
        server.start(wait_iterations=True)
        try:
            client_a = _TestConnection("localhost", port)
            client_a.start()

            client_b = _TestConnection("localhost", port)
            client_b.start()

            client_a.wait_connected()
            client_b.wait_connected()

            client_a.send(b'42')
            self.assertEqual(b'42', client_b.recv())
            client_b.send(b'42')
            self.assertEqual(b'42', client_a.recv())

            client_c = _TestConnection("localhost", port)
            client_c.start()
            client_d = _TestConnection("localhost", port)
            client_d.start()

            with self.assertRaises(ConnectionResetError):
                client_c.wait_connected()
                client_d.wait_connected()

            client_a.send(b'42')
            self.assertEqual(b'42', client_b.recv())
            client_b.send(b'42')
            self.assertEqual(b'42', client_a.recv())

            client_a.close()
            client_b.recv()

            client_c = _TestConnection("localhost", port)
            client_c.start()
            client_d = _TestConnection("localhost", port)
            client_d.start()

            client_c.wait_connected()
            client_d.wait_connected()

            client_c.send(b'42')
            self.assertEqual(b'42', client_d.recv())
            client_d.send(b'42')
            self.assertEqual(b'42', client_c.recv())

        finally:
            server.shutdown()

    def test_two_threads(self):
        port = 9991
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("localhost", port))
        server = wsbridge.SimpleService(sock, threads=2, mediations_per_thread=1)
        server.start(wait_iterations=True)

        try:
            client_a = _TestConnection("localhost", port)
            client_a.start()

            client_b = _TestConnection("localhost", port)
            client_b.start()

            client_a.wait_connected()
            client_b.wait_connected()

            client_a.send(b'42')
            self.assertEqual(b'42', client_b.recv())
            client_b.send(b'42')
            self.assertEqual(b'42', client_a.recv())

            client_c = _TestConnection("localhost", port)
            client_c.start()
            client_d = _TestConnection("localhost", port)
            client_d.start()

            client_c.wait_connected()
            client_d.wait_connected()

            client_e = _TestConnection("localhost", port)
            client_e.start()
            client_f = _TestConnection("localhost", port)
            client_f.start()

            with self.assertRaises(ConnectionResetError):
                client_e.wait_connected()
                client_f.wait_connected()

            client_a.send(b'42')
            self.assertEqual(b'42', client_b.recv())
            client_b.send(b'42')
            self.assertEqual(b'42', client_a.recv())

            client_a.close()
            client_b.recv()

            client_e = _TestConnection("localhost", port)
            client_e.start()
            client_f = _TestConnection("localhost", port)
            client_f.start()

            client_e.wait_connected()
            client_f.wait_connected()

            client_e.send(b'42')
            self.assertEqual(b'42', client_f.recv())
            client_f.send(b'42')
            self.assertEqual(b'42', client_e.recv())

        finally:
            server.shutdown()

    def test_no_limit_per_t(self):
        for t in (4, 1):
            port = 9090 + t
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.bind(("localhost", port))
            server = wsbridge.SimpleService(sock, threads=t, mediations_per_thread=0)
            server.start(wait_iterations=True)

            clients = list()
            for i in range(20):
                client_a = _TestConnection("localhost", port)
                client_a.start()
                client_b = _TestConnection("localhost", port)
                client_b.start()

                clients.append(client_a)
                clients.append(client_b)

                client_a.wait_connected()
                client_b.wait_connected()

            for i in range(20):
                clients[i * 2].close()

            server.shutdown()

    def test_no_t_limit(self):
        port = 9099
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("localhost", port))
        server = wsbridge.SimpleService(sock, threads=0)
        server.start(wait_iterations=True)

        clients = list()
        for i in range(4):
            client_a = _TestConnection("localhost", port)
            client_a.start()
            client_b = _TestConnection("localhost", port)
            client_b.start()

            clients.append(client_a)
            clients.append(client_b)

            client_a.wait_connected()
            client_b.wait_connected()

        mt = 0
        for t in threading.enumerate():
            log_ok(t)
            if isinstance(t, simpleservice.MediatorsThread):
                mt += 1

        for i in range(4):
            clients[i * 2].close()

        server.shutdown()





