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
import time
import locale
import unittest
import threading
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler

from pymodelardb.connection import Connection
from pymodelardb.types import ProgrammingError, TypeOf


class CursorTest(object):
    @classmethod
    def setUpClass(cls):
        cls.thread = threading.Thread(target=cls.start_server, args=())
        cls.run_server = True
        cls.thread.start()
        cls.encoding = locale.getpreferredencoding()

    @classmethod
    def tearDownClass(cls):
        cls.run_server = False
        cls.thread.join()

    def tearDown(self):
        # Ensure the cursor and connection is always closed
        try:
            self.cursor.close()
        except ProgrammingError:
            pass

        try:
            self.connection.close()
        except ProgrammingError:
            pass

    def test_execute_select_error(self):
        self.set_server_response("""
        {
          "time": "PT0.3773S",
          "query": "SELECT * FROM DataPoint",
          "result":  [
            Unknown Exception Occurred
          ]
        }
        """)
        with self.assertRaises(ProgrammingError):
            self.cursor.execute("SELECT * FROM DataPoint")

    def test_execute_select_rows(self):
        self.set_server_response("""
        {
          "time": "PT0.3773S",
          "query": "SELECT * FROM DataPoint",
          "result":  [
            {"TID":1,"TIMESTAMP":"1990-05-01 12:00:00.0","VALUE":0.37},
            {"TID":2,"TIMESTAMP":"1990-05-01 12:00:00.0","VALUE":0.55},
            {"TID":3,"TIMESTAMP":"1990-05-01 12:00:00.0","VALUE":0.73}
          ]
        }
        """)
        self.cursor.execute("SELECT * FROM DataPoint")

    def test_execute_select_rows_metadata(self):
        self.test_execute_select_rows()
        description = self.cursor.description
        self.assertEqual(len(description), 3)
        self.assertEqual(description[0],
                         ('TID', TypeOf.NUMBER, None, None, None, None, False))
        self.assertEqual(description[1],
                         ('TIMESTAMP',
                          TypeOf.NUMBER, None, None, None, None, False))
        self.assertEqual(description[2],
                         ('VALUE',
                          TypeOf.NUMBER, None, None, None, None, False))
        self.assertEqual(self.cursor.rowcount, 3)

    def test_execute_select_rows_fetchone(self):
        self.test_execute_select_rows()
        self.assertEqual(self.cursor.fetchone(),
                         (1, '1990-05-01 12:00:00.0', 0.37))

    def test_execute_select_rows_fetchmany(self):
        self.test_execute_select_rows()
        self.assertEqual(self.cursor.fetchmany(37),
                         [(1, '1990-05-01 12:00:00.0', 0.37),
                          (2, '1990-05-01 12:00:00.0', 0.55),
                          (3, '1990-05-01 12:00:00.0', 0.73)])

    def test_execute_select_rows_fetchall(self):
        self.test_execute_select_rows()
        self.assertEqual(self.cursor.fetchall(),
                         [(1, '1990-05-01 12:00:00.0', 0.37),
                          (2, '1990-05-01 12:00:00.0', 0.55),
                          (3, '1990-05-01 12:00:00.0', 0.73)])

    def test_execute_select_null(self):
        self.set_server_response("""
        {
          "time": "PT0.008S",
          "query": "SELECT MIN(value) FROM DataPoint",
          "result":  [
            {"MIN(VALUE)":"null"}
          ]
        }
        """)
        self.cursor.execute("SELECT MIN(value) FROM DataPoint")

    def test_execute_select_null_metadata(self):
        self.test_execute_select_null()
        description = self.cursor.description
        self.assertEqual(len(description), 1)
        self.assertEqual(description[0],
                         ('MIN(VALUE)',
                          TypeOf.NUMBER, None, None, None, None, False))
        self.assertEqual(self.cursor.rowcount, 1)

    def test_execute_select_empty(self):
        self.set_server_response("""
        {
          "time": "PT7.996S",
          "query": "SELECT MAX(value) FROM DataPoint",
          "result":  [
            {}
          ]
        }
        """)
        self.cursor.execute("SELECT MAX(value) FROM DataPoint")

    def test_execute_select_empty_metadata(self):
        self.test_execute_select_empty()
        self.assertEqual(len(self.cursor.description), 0)
        self.assertEqual(self.cursor.rowcount, 0)

    def test_execute_select_empty_fetchone(self):
        self.test_execute_select_empty()
        self.assertIsNone(self.cursor.fetchone())

    def test_execute_select_empty_fetchmany(self):
        self.test_execute_select_empty()
        self.assertEqual(self.cursor.fetchmany(37), [])

    def test_execute_select_empty_fetchall(self):
        self.test_execute_select_empty()
        self.assertEqual(self.cursor.fetchall(), [])

    def test_close(self):
        self.cursor.close()

    def test_close_execute(self):
        self.cursor.close()
        with self.assertRaises(ProgrammingError):
            self.cursor.execute("SELECT * FROM DataPoint")

    def test_arraysize(self):
        self.assertEqual(self.cursor.arraysize, 1)

    def test_setinputsizes(self):
        self.cursor.setinputsizes(37)

    def test_setoutputsizes(self):
        self.cursor.setoutputsizes(37, 73)


class TestHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(self.response)
        time.sleep(0.25)  # Ensure that the response is read without
                          # adding unnecessary complexity to the tests

    def log_message(self, format, *args):
        pass  # Stop messages being written to stdout


class HTTPCursorTest(CursorTest, unittest.TestCase):
    @classmethod
    def set_server_response(cls, response: str):
        cls.handler.response = response.encode(cls.encoding)

    @classmethod
    def start_server(cls):
        # Mock ModelarDB's HTTP interface
        HTTPServer.allow_reuse_address = True
        cls.handler = TestHTTPRequestHandler
        cls.server = HTTPServer(("localhost", 9999), cls.handler)
        cls.server.timeout = 0.20
        cls.response = b""

        while cls.run_server:
            cls.server.handle_request()
        cls.server.server_close()

    def setUp(self):
        self.connection = Connection("http://localhost")
        self.cursor = self.connection.cursor()


class SocketCursorTest(CursorTest, unittest.TestCase):
    @classmethod
    def set_server_response(cls, response: str):
        cls.response = response.encode(cls.encoding)

    @classmethod
    def start_server(cls):
        # Mock ModelarDB's Socket interface
        cls.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cls.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        cls.socket.bind(("localhost", 9999))
        cls.socket.listen()
        cls.response = b""

        while cls.run_server:
            conn, _ = cls.socket.accept()
            conn.sendall(cls.response)
            time.sleep(0.25)  # Ensure that the response is read without
            conn.close()      # adding unnecessary complexity to the tests
        cls.socket.close()

    def setUp(self):
        self.connection = Connection("socket://localhost")
        self.cursor = self.connection.cursor()
