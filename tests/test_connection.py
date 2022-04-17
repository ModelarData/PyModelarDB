# Copyright 2021 The PyModelarDB Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest
import socket

from pymodelardb.connection import Connection
from pymodelardb.types import ProgrammingError, NotSupportedError
from pymodelardb.cursors import ArrowCursor, HTTPCursor, SocketCursor


class ConnectionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Mocks the Apache Arrow Flight, HTTP, and socket interface
        cls.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cls.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        cls.socket.bind(("localhost", 9999))
        cls.socket.listen()

    @classmethod
    def tearDownClass(cls):
        cls.socket.close()

    def test_construct_dsn_arrow_correct(self):
        conn = Connection("arrow://localhost")
        cursor = conn.cursor()
        self.assertIsInstance(cursor, ArrowCursor)
        cursor.close()

    def test_construct_dsn_http_correct(self):
        conn = Connection("http://localhost")
        cursor = conn.cursor()
        self.assertIsInstance(cursor, HTTPCursor)
        cursor.close()

    def test_construct_dsn_socket_correct(self):
        conn = Connection("socket://localhost")
        cursor = conn.cursor()
        self.assertIsInstance(cursor, SocketCursor)
        cursor.close()

    def test_construct_dsn_arrow_wrong_separator(self):
        with self.assertRaises(ProgrammingError):
            Connection("arrow:/localhost")

    def test_construct_dsn_http_wrong_separator(self):
        with self.assertRaises(ProgrammingError):
            Connection("http:/localhost")

    def test_construct_dsn_socket_wrong_separator(self):
        with self.assertRaises(ProgrammingError):
            Connection("socket:/localhost")

    def test_construct_dsn_wrong_interface(self):
        with self.assertRaises(ProgrammingError):
            Connection("interface://localhost")

    def test_construct_user(self):
        with self.assertRaises(NotSupportedError):
            Connection(user="username")

    def test_construct_password(self):
        with self.assertRaises(NotSupportedError):
            Connection(password="password")

    def test_construct_database(self):
        with self.assertRaises(NotSupportedError):
            Connection(database="database")

    def test_construct_host_interface_arrow_correct(self):
        conn = Connection(host="localhost", interface="arrow")
        cursor = conn.cursor()
        self.assertIsInstance(cursor, ArrowCursor)
        cursor.close()

    def test_construct_host_interface_http_correct(self):
        conn = Connection(host="localhost", interface="http")
        cursor = conn.cursor()
        self.assertIsInstance(cursor, HTTPCursor)
        cursor.close()

    def test_construct_host_interface_socket_correct(self):
        conn = Connection(host="localhost", interface="socket")
        cursor = conn.cursor()
        self.assertIsInstance(cursor, SocketCursor)
        cursor.close()

    def test_construct_host_interface_wrong_interface(self):
        with self.assertRaises(ProgrammingError):
            Connection(host="localhost", interface="interface")

    def test_close(self):
        Connection("http://localhost").close()

    def test_close_close(self):
        conn = Connection("http://localhost")
        conn.close()
        with self.assertRaises(ProgrammingError):
            conn.close()

    def test_close_cursor(self):
        conn = Connection("http://localhost")
        conn.close()
        with self.assertRaises(ProgrammingError):
            conn.cursor()

    def test_close_commit(self):
        conn = Connection("http://localhost")
        conn.close()
        with self.assertRaises(ProgrammingError):
            conn.commit()

    def test_commit(self):
        Connection("http://localhost").commit()
