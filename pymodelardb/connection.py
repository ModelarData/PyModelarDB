"""Implementation of the Connection interface"""

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

from pymodelardb.types import Interface
from pymodelardb.types import NotSupportedError
from pymodelardb.types import ProgrammingError
from pymodelardb.types import DEFAULT_PORT_NUMBER

__all__ = ['Connection']


class Connection(object):
    """Represents one or more connections to ModelarDB through cursors.

       Instances of this class should be created using pymodelardb.connect(),
       and queries run using a Cursor retrieved from Connection.cursor().

       Arguments:

        :param dsn: connecting string as interface://hostname-or-ip where
        interface is Arrow, HTTP, or socket for ModelarDB (depending how it is
        configured) or Arrow for MiniModelarDB.
        :param user: unsupported parameter required by PEP 249.
        :param password: unsupported parameter required by PEP 249.
        :param host: the hostname or IP of the host where ModelarDB or
        MiniModelarDB is running.
        :param interface: the interface used by ModelarDB (Arrow, HTTP, or
        socket) or MiniModelarDB (Arrow).
        :param port: the port number ModelarDB or MiniModelarDB is configured
        to used for its interface (defaults to 9999 if not specified).
    """
    def __init__(self, dsn: str = None, user: str = None, password: str = None,
                 host: str = None, database: str = None, interface: str = None,
                 port: int = DEFAULT_PORT_NUMBER):

        # Ensure the necessary parameters have been provided
        if user or password or database:
            raise NotSupportedError(
                  "only dsn, host, interface, and port is supported")
        elif dsn:   # The connection string is given precedence
            try:
                interface, hostAndMaybePort = dsn.split('://')
                hostAndMaybePortSplit = hostAndMaybePort.split(':')
                host = hostAndMaybePortSplit[0]
                port = int(hostAndMaybePortSplit[1]) \
                    if 1 < len(hostAndMaybePortSplit) \
                    else DEFAULT_PORT_NUMBER
            except ValueError:
                raise ProgrammingError(
                    "dsn must be interface://hostname-or-ip[:port]") from None
        elif host and interface:
            pass
        else:
            raise ProgrammingError("dsn or host and interface is required")

        try:
            interface = Interface[interface.upper()]
        except KeyError:
            raise ProgrammingError(
                "interface must be Arrow, HTTP, or socket") from None

        # Store the host and port so cursor(self) can pass them to the Cursor
        self.__host = host
        self.__port = port

        # Create a cursor that match the requested interface type. The cursors
        # are imported from inside the method to break a circular import
        self.__closed = False
        if interface is Interface.ARROW:
            from pymodelardb.cursors import ArrowCursor
            self.__cursor = ArrowCursor
        elif interface is Interface.HTTP:
            from pymodelardb.cursors import HTTPCursor
            self.__cursor = HTTPCursor
        elif interface is Interface.SOCKET:
            from pymodelardb.cursors import SocketCursor
            self.__cursor = SocketCursor

    def close(self):
        """Mark the connection as closed."""
        self._is_closed("cannot close the connection as it is already closed")
        self.__closed = True

    def commit(self):
        """Unsupported method required by PEP 249."""
        self._is_closed("cannot commit as the connection is closed")

    def cursor(self):
        """Construct a new Cursor based on the Connection's interface type."""
        self._is_closed("cannot create a cursor as the connection is closed")
        return self.__cursor(self, self.__host, self.__port)

    def _is_closed(self, message: str):
        """Check if the connection have been closed."""
        if self.__closed:
            raise ProgrammingError(message)
