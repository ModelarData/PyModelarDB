"""Types used throughout the package. They are stored in a separate module
   instead of pymodelardb.__init__ to avoid circular dependencies.
"""

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

from enum import Enum


class Error(Exception):
    """Base error for all other errors to derive from."""


class DatabaseError(Error):
    """Error related to the time series management system."""


class ProgrammingError(DatabaseError):
    """Error related to the use of the package."""


class NotSupportedError(DatabaseError):
    """Error to be raised if a feature required by PEP 249 is not supported."""


class Interface(Enum):
    """The different interfaces ModelarDB and MiniModelarDB supports."""
    ARROW = 1
    SOCKET = 2
    HTTP = 3


class TypeOf(Enum):
    """The different types description.type_of must match."""
    STRING = 1
    BINARY = 2
    NUMBER = 3
    DATETIME = 4
    ROWID = 5
