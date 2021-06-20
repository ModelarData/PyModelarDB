"""Implementation of Cursors for the interfaces ModelarDB supports."""

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

import itertools
import json
import locale
import re

from telnetlib import Telnet
from typing import Any, Union
from urllib import request

from pymodelardb.connection import Connection
from pymodelardb.types import ProgrammingError, TypeOf

__all__ = ['HTTPCursor', 'SocketCursor']


class Cursor(object):
    """Represents a single connection to ModelarDB.

       Arguments:

        :param connection: the Connection object that created the Cursor.
    """
    def __init__(self, connection: Connection):
        self._connection = connection
        self._encoding = locale.getpreferredencoding()
        self._description = None
        self._rowcount = -1
        self.arraysize = 1
        self._result_set = None
        self.__placeholder_search = re.compile("%\((.*?)\)s")

    @property
    def description(self):
        """A description of the columns in the last query."""
        self._is_closed(
            "cannot return the description as the cursor is closed")
        return self._description

    @property
    def rowcount(self):
        """The number of rows returned by the last query."""
        self._is_closed("cannot return the row count as the cursor is closed")
        return self._rowcount

    def close(self):
        """Mark the cursor as closed."""
        self._is_closed("cannot close the cursor as it is already closed")
        self._connection = None

    def executemany(self, operation: str, seq_of_parameters):
        """Execute operation for each set of parameters."""
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def fetchone(self):
        """Return the next row from the result set."""
        self._is_closed("cannot fetch a row as the cursor is closed")
        self._is_result_set_ready()
        try:
            return next(self._result_set)
        except:
            return None

    def fetchmany(self, size: Union[int, None]=None):
        """Return the next size rows from the result set."""
        self._is_closed("cannot fetch multiple rows as the cursor is closed")
        self._is_result_set_ready()
        if not size:
            size = self.arraysize
        return list(itertools.islice(self._result_set, size))

    def fetchall(self):
        """Return all remaining rows from the result set."""
        self._is_closed("cannot fetch all rows as the cursor is closed")
        self._is_result_set_ready()
        return list(self._result_set)

    def setinputsizes(self, sizes: Any):
        """Unsupported method required by PEP 249."""
        self._is_closed("cannot set the input size as the cursor is closed")

    def setoutputsizes(self, size: Any, column: Any):
        """Unsupported method required by PEP 249."""
        self._is_closed("cannot set the output size as the cursor is closed")

    def _before_execute(self, operation: str, parameters=None):
        """Ensure the cursor is ready and add the parameters to operation."""
        # Parameters are not escaped as ModelarDB is read-only
        if parameters is dict:
            operation % parameters
        elif parameters is list or parameters is tuple:
            placeholders = self.__placeholder_search.findall(operation)
            operation % dict(zip(placeholders, parameters))
        return operation.encode(self._encoding)

    def _after_execute(self, response: bytes):
        """Parse the response received from ModelarDB."""
        result = response.decode(self._encoding)
        try:
            result_set = json.loads(result)['result']
        except json.decoder.JSONDecodeError:
            # Extract the exception thrown by the server's query engine
            start_of_error = result.find('[') + 1
            end_of_error = result.rfind(']')
            message = 'unable to execute query due to:\n' \
                + result[start_of_error:end_of_error].strip()
            raise ProgrammingError(message) from None

        # The engine based on Apache Spark encodes an empty result as a {}
        if len(result_set) == 1 and not result_set[0]:
            result_set = []
        self._result_set = map(lambda result:
                               tuple(result.values()), result_set)

        # Only the name and type_code is mandatory, the rest can be None
        description = []
        if result_set:
            for name, value in result_set[0].items():
                type_code = TypeOf.STRING if value is str else TypeOf.NUMBER
                description \
                    .append((name, type_code, None, None, None, None, False))
        self._description = tuple(description)
        self._rowcount = len(result_set)

    def _is_closed(self, message: str):
        """Check if the cursor or connection have been closed."""
        if not self._connection:
            raise ProgrammingError(message)

    def _is_result_set_ready(self):
        """Check if a result set have been retrieved from the database."""
        if not self._result_set:
            raise ProgrammingError("a result set is currently not available")


class HTTPCursor(Cursor):
    def __init__(self, connection: Connection):
        Cursor.__init__(self, connection)

    def execute(self, operation: str, parameters: Any=None):
        """Execute operation after adding the parameters."""
        self._is_closed("cannot execute queries as the cursor is closed")
        message = self._before_execute(operation.strip(), parameters)
        response = request.urlopen(self._connection._host, message)
        self._after_execute(response.read())
        response.close()


class SocketCursor(Cursor):
    def __init__(self, connection: Connection):
        Cursor.__init__(self, connection)
        self.__telnet = Telnet(self._connection._host, 9999)

    def close(self):
        """Close the socket and mark the cursor as closed."""
        self._is_closed("cannot close the cursor as it is already closed")
        self.__telnet.close()
        super().close()

    def execute(self, operation: str, parameters: Any=None):
        """Execute operation after adding the parameters."""
        self._is_closed("cannot execute queries as the cursor is closed")
        message = self._before_execute(operation.strip() + '\n', parameters)
        self.__telnet.write(message)
        result = []
        response = self.__telnet.read_some()
        while response:
            result.append(response)
            response = self.__telnet.read_very_eager()
        self._after_execute(b''.join(result))
