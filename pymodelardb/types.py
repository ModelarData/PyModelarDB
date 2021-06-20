"""Types used throughout the package. They are stored in a separate module
   instead of pymodelardb.__init__ to avoid circular dependencies.
"""

from enum import Enum

# Public Types
class Error(Exception):
    """Base error for all other errors to derive from."""


class DatabaseError(Error):
    """Error related to the time series management system."""


class ProgrammingError(DatabaseError):
    """Error related to the use of the package."""


class NotSupportedError(DatabaseError):
    """Error to be raised if a feature required by PEP 249 is not supported."""


class Interface(Enum):
    """The different interfaces ModelarDB supports."""
    SOCKET = 1,
    HTTP = 2,

class TypeOf(Enum):
    """The different types description.type_of must match."""
    STRING = 1
    BINARY = 2
    NUMBER = 3
    DATETIME = 4
    ROWID = 5
