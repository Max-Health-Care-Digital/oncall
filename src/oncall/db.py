# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

"""
Database initialization module for Oncall.

Provides global access points for database connections and specific DBAPI types
after being initialized via the `init` function. Includes a wrapper class
to provide a context-managed connection that also supports cursor creation.
"""

import logging
import sys
from typing import Any, Callable, Dict, Generator, Optional, Type

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

log = logging.getLogger(__name__)

# --- Wrapper Class Definition ---


class ContextualRawConnection:
    """
    A wrapper around a raw DBAPI connection obtained from SQLAlchemy's pool.

    Provides context management (__enter__/__exit__) for automatic cleanup
    and delegates cursor(), commit(), rollback() methods to the underlying
    raw DBAPI connection, allowing existing code expecting a cursor() method
    to function within a 'with' block.
    """

    def __init__(self, raw_connection_factory: Callable[[], Any]):
        self._factory = raw_connection_factory
        self._raw_conn: Optional[Any] = (
            None  # Holds the actual connection when active
        )

    def __enter__(self) -> "ContextualRawConnection":
        """Gets a raw connection from the factory and returns self."""
        if self._raw_conn is not None:
            # This guards against nested 'with' statements using the same wrapper instance
            raise RuntimeError("Context manager is not re-entrant.")
        try:
            self._raw_conn = self._factory()
            log.debug(f"Acquired raw DBAPI connection: {type(self._raw_conn)}")
            return self  # Return the wrapper object itself
        except Exception as e:
            log.error(
                f"Failed to acquire raw DBAPI connection: {e}", exc_info=True
            )
            self._raw_conn = None  # Ensure it's None if acquisition failed
            raise  # Re-raise the exception

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> Optional[bool]:
        """Closes the raw connection, returning it to the pool."""
        if self._raw_conn:
            conn_to_close = self._raw_conn
            conn_type = type(conn_to_close)
            self._raw_conn = None  # Mark as inactive *before* closing
            try:
                # Note: commit/rollback should ideally happen *before* __exit__ is called.
                # This block just ensures the connection is closed.
                log.debug(f"Closing raw DBAPI connection: {conn_type}")
                conn_to_close.close()
            except Exception as e:
                log.warning(
                    f"Error closing raw DBAPI connection ({conn_type}): {e}",
                    exc_info=True,
                )
                # Decide if error during close should suppress original exception
                # Returning False (or None) propagates the original exception (if any)
                return False
        # No active connection or closed successfully, propagate original exception (if any)
        return False

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        """Delegates cursor creation to the underlying raw DBAPI connection."""
        if not self._raw_conn:
            raise RuntimeError(
                "Connection not active (must be used within 'with' block)"
            )
        log.debug(
            f"Creating cursor from raw DBAPI connection with args: {args}, kwargs: {kwargs}"
        )
        return self._raw_conn.cursor(*args, **kwargs)

    def commit(self) -> None:
        """Delegates commit to the underlying raw DBAPI connection."""
        if not self._raw_conn:
            raise RuntimeError(
                "Connection not active (must be used within 'with' block)"
            )
        log.debug("Committing transaction on raw DBAPI connection.")
        self._raw_conn.commit()

    def rollback(self) -> None:
        """Delegates rollback to the underlying raw DBAPI connection."""
        if not self._raw_conn:
            raise RuntimeError(
                "Connection not active (must be used within 'with' block)"
            )
        log.debug("Rolling back transaction on raw DBAPI connection.")
        self._raw_conn.rollback()

    def escape(self, value: Any) -> str:
        """
        Delegates escaping to the underlying raw DBAPI connection.

        Tries common method names ('escape', 'escape_string').
        WARNING: Using direct string escaping is discouraged and potentially unsafe.
                 Prefer parameterized queries whenever possible.

        Args:
            value: The value to escape.

        Returns:
            The escaped string representation suitable for SQL literals.

        Raises:
            RuntimeError: If the connection is not active.
            NotImplementedError: If no suitable escape method is found on the DBAPI connection.
        """
        if not self._raw_conn:
            raise RuntimeError(
                "Connection not active (must be used within 'with' block)"
            )

        # DBAPI drivers use different names for the escape method. Try common ones.
        if hasattr(self._raw_conn, "escape"):
            escape_method = self._raw_conn.escape
            log.debug(
                f"Delegating escape call to {type(self._raw_conn)}.escape"
            )
            return escape_method(value)
        elif hasattr(self._raw_conn, "escape_string"):
            escape_method = self._raw_conn.escape_string
            log.debug(
                f"Delegating escape call to {type(self._raw_conn)}.escape_string"
            )
            # Note: Some escape_string methods might have different signatures
            return escape_method(value)
        # Add elif for other potential names if needed (e.g., based on your specific DB driver)
        else:
            log.error(
                f"Underlying DBAPI connection {type(self._raw_conn)} has no recognized 'escape' or 'escape_string' method."
            )
            # Avoid providing a default insecure implementation. Fail explicitly.
            raise NotImplementedError(
                f"Escape method not supported by underlying DBAPI driver: {type(self._raw_conn)}"
            )

    # Add other necessary delegated methods if your code uses them directly on the connection
    # e.g., set_session, info, etc. Only add what's needed.
    # def __getattr__(self, name):
    #     # Optional: Generic delegation for other attributes if needed, but explicit is safer
    #     if not self._raw_conn:
    #         raise RuntimeError("Connection not active")
    #     return getattr(self._raw_conn, name)


class UnsafeContextualRawConnection:
    """
    WARNING: THIS IS AN UNSAFE IMPLEMENTATION.
    It allows methods like .cursor() to be called outside a 'with' block
    by acquiring NEW, UNMANAGED connections that WILL LEAK, leading to
    resource exhaustion and application failure. This is only provided
    because modifying consuming API code was forbidden.
    DO NOT USE IN PRODUCTION unless the underlying issues are fixed.

    Wraps a raw DBAPI connection, provides context management, and delegates
    methods, but includes dangerous fallbacks for calls outside 'with'.
    """

    def __init__(self, raw_connection_factory: Callable[[], Any]):
        self._factory = raw_connection_factory
        self._raw_conn: Optional[Any] = (
            None  # Holds the managed connection when inside 'with'
        )

    def __enter__(self) -> "UnsafeContextualRawConnection":
        """Gets a raw connection from the factory (MANAGED)."""
        if self._raw_conn is not None:
            raise RuntimeError("Context manager is not re-entrant.")
        try:
            self._raw_conn = self._factory()
            log.debug(
                f"Acquired MANAGED raw DBAPI connection via __enter__: {type(self._raw_conn)}"
            )
            return self
        except Exception as e:
            log.error(
                f"Failed to acquire raw DBAPI connection via __enter__: {e}",
                exc_info=True,
            )
            self._raw_conn = None
            raise

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> Optional[bool]:
        """Closes the MANAGED raw connection."""
        if self._raw_conn:
            conn_to_close = self._raw_conn
            conn_type = type(conn_to_close)
            self._raw_conn = None  # Mark as inactive *before* closing
            try:
                log.debug(
                    f"Closing MANAGED raw DBAPI connection via __exit__: {conn_type}"
                )
                conn_to_close.close()
            except Exception as e:
                log.warning(
                    f"Error closing MANAGED raw DBAPI connection ({conn_type}): {e}",
                    exc_info=True,
                )
                return False
        return False

    def _get_conn_dangerously(self) -> Any:
        """
        Internal helper to get a connection.
        Returns the managed connection if inside 'with'.
        Otherwise, acquires and returns a NEW, UNMANAGED, LEAKY connection.
        """
        if self._raw_conn:
            # We are inside the 'with' block, return the managed connection
            return self._raw_conn
        else:
            # We are outside the 'with' block. Acquire a new connection.
            # !!! THIS CONNECTION WILL NOT BE CLOSED AUTOMATICALLY !!!
            # !!! IT WILL LEAK RESOURCES FROM THE POOL !!!
            log.warning(
                "UNSAFE: Acquiring new raw DBAPI connection outside 'with' block. THIS WILL LEAK CONNECTIONS!"
            )
            try:
                # Get a new connection directly from the factory
                leaky_conn = self._factory()
                log.debug(
                    f"Acquired UNMANAGED/LEAKY connection: {type(leaky_conn)}"
                )
                return leaky_conn
            except Exception as e:
                log.error(
                    f"Failed to acquire UNMANAGED/LEAKY connection: {e}",
                    exc_info=True,
                )
                raise RuntimeError(
                    f"Failed to get necessary DB connection: {e}"
                )

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        """Delegates cursor creation. Uses managed conn if active, otherwise LEAKS a new conn."""
        conn = self._get_conn_dangerously()
        log.debug(
            f"Creating cursor from {('managed' if self._raw_conn else 'UNMANAGED/LEAKY')} connection."
        )
        # Note: If conn is leaky, whether closing the cursor closes conn is driver-dependent and unreliable.
        return conn.cursor(*args, **kwargs)

    def commit(self) -> None:
        """Delegates commit. Uses managed conn if active, otherwise LEAKS a new conn."""
        conn = self._get_conn_dangerously()
        log.debug(
            f"Committing on {('managed' if self._raw_conn else 'UNMANAGED/LEAKY')} connection."
        )
        conn.commit()
        # If conn was leaky, it remains open and unmanaged after commit.

    def rollback(self) -> None:
        """Delegates rollback. Uses managed conn if active, otherwise LEAKS a new conn."""
        conn = self._get_conn_dangerously()
        log.debug(
            f"Rolling back on {('managed' if self._raw_conn else 'UNMANAGED/LEAKY')} connection."
        )
        conn.rollback()
        # If conn was leaky, it remains open and unmanaged after rollback.

    def escape(self, value: Any) -> str:
        """Delegates escaping. Uses managed conn if active, otherwise LEAKS a new conn."""
        conn = self._get_conn_dangerously()
        log.debug(
            f"Escaping value on {('managed' if self._raw_conn else 'UNMANAGED/LEAKY')} connection."
        )

        if hasattr(conn, "escape"):
            escape_method = conn.escape
            return escape_method(value)
        elif hasattr(conn, "escape_string"):
            escape_method = conn.escape_string
            return escape_method(value)
        else:
            log.error(
                f"Underlying DBAPI connection {type(conn)} has no recognized 'escape' or 'escape_string' method."
            )
            raise NotImplementedError(
                f"Escape method not supported by underlying DBAPI driver: {type(conn)}"
            )

    def close(self) -> None:
        """
        Placeholder method to prevent AttributeError from legacy code.

        WARNING: This method does NOT reliably close database connections,
                 especially those acquired unsafely outside a 'with' block.
                 Proper resource management only occurs when using this object
                 as a context manager (via 'with db.connect()').
                 Calling this manually may have no effect on leaked connections.
        """
        # This method primarily exists to prevent the AttributeError.
        # It does NOT guarantee closure of leaked connections.
        log.warning(
            "UnsafeContextualRawConnection.close() called manually. "
            "This method is a placeholder and does NOT guarantee "
            "connection closure or prevent leaks. Use 'with db.connect()' "
            "for proper resource management."
        )
        # Option 1: Do nothing (safest for not interfering with __exit__)
        # pass

        # Option 2 (Slightly more active, but potentially problematic):
        # Attempt to close the *managed* connection if it exists.
        # This does NOT close connections leaked by _get_conn_dangerously.
        # It might also interfere with __exit__ if called at the wrong time.
        # Generally safer to just 'pass'.
        if self._raw_conn:
            try:
                log.debug(
                    "Attempting to close potentially managed connection via manual .close()"
                )
                self._raw_conn.close()
                self._raw_conn = None  # Clear ref if closed manually
            except Exception as e:
                log.warning(f"Error during manual .close() attempt: {e}")


# --- Global Variables ---
connect_factory: Optional[Callable[[], ContextualRawConnection]] = None
DictCursor: Optional[Type[Any]] = None
IntegrityError: Optional[Type[Exception]] = None
Error: Optional[Type[Exception]] = None  # Base DBAPI Error class
db_engine: Optional[Engine] = None


# Provide a direct callable 'connect' for convenience
def connect() -> ContextualRawConnection:
    """Factory function to get a database connection wrapper."""
    if connect_factory is None:
        raise RuntimeError(
            "Database connection not initialized. Call db.init() first."
        )
    return connect_factory()


# --- Initialization Function ---
def init(config: Dict[str, Any]) -> None:
    """
    Initializes the database connection using SQLAlchemy based on the provided config.
    Sets up a factory (`db.connect`) that returns a context-managed wrapper
    around a raw DBAPI connection, which also supports cursor creation.
    """
    global connect_factory, DictCursor, IntegrityError, Error, db_engine

    log.info("Initializing database connection...")
    # ... (try/except block, engine creation, dbapi loading, Error/IntegrityError mapping - remain the same) ...
    try:
        conn_config = config["conn"]
        engine_kwargs = config.get("kwargs", {})
        connection_string = conn_config["str"] % conn_config.get("kwargs", {})

        log.info(f"Creating SQLAlchemy engine with kwargs: {engine_kwargs}")
        engine = create_engine(connection_string, **engine_kwargs)
        db_engine = engine  # Store engine globally if needed
        log.info(
            f"SQLAlchemy engine created for dialect: {engine.dialect.name}"
        )

        dbapi = engine.dialect.dbapi
        if not dbapi:
            log.critical("Failed to get DBAPI module from SQLAlchemy dialect.")
            sys.exit(1)
        log.info(f"Using DBAPI module: {dbapi.__name__}")

        # --- Get DBAPI Exception Classes (Same as before) ---
        dbapi_error_cls = getattr(dbapi, "Error", None) or getattr(
            dbapi, "DatabaseError", None
        )
        if dbapi_error_cls is None:
            log.critical(
                f"DBAPI module '{dbapi.__name__}' does not provide a standard 'Error' or 'DatabaseError' class."
            )
            sys.exit(1)
        Error = dbapi_error_cls
        log.info(
            f"Mapped global 'db.Error' to '{dbapi.__name__}.{Error.__name__}'"
        )

        integrity_error_cls = getattr(dbapi, "IntegrityError", None)
        if integrity_error_cls is None:
            log.warning(
                f"DBAPI module '{dbapi.__name__}' does not provide an 'IntegrityError' class."
            )
            IntegrityError = Error  # Fallback to base error
        else:
            IntegrityError = integrity_error_cls
            log.info(
                f"Mapped global 'db.IntegrityError' to '{dbapi.__name__}.{IntegrityError.__name__}'"
            )

        # --- Get DictCursor (Same as before) ---
        dict_cursor_cls = None
        if hasattr(dbapi, "cursors") and hasattr(dbapi.cursors, "DictCursor"):
            dict_cursor_cls = dbapi.cursors.DictCursor
        elif hasattr(dbapi, "DictCursor"):
            dict_cursor_cls = dbapi.DictCursor

        if dict_cursor_cls:
            DictCursor = dict_cursor_cls
            log.info(
                f"Mapped global 'db.DictCursor' to '{dbapi.__name__}...{DictCursor.__name__}'"
            )
        else:
            DictCursor = None
            log.warning(
                f"DBAPI module '{dbapi.__name__}' does not provide a standard 'DictCursor'."
            )

        # --- Assign Connection Factory using the Wrapper ---
        # 'connect_factory' will create a new wrapper instance each time it's called
        # connect_factory = lambda: UnsafeContextualRawConnection(
        #     db_engine.raw_connection
        # )

        connect_factory = lambda: ContextualRawConnection(
            db_engine.raw_connection
        )

        log.info(
            "Assigned 'db.connect' factory to produce ContextualRawConnection wrapper."
        )

        # Optional: Test connection using the new wrapper
        try:
            log.debug("Attempting test connection using wrapper...")
            with connect() as test_conn_wrapper:  # Use the factory function `connect()`
                # Test cursor creation and a simple query
                with test_conn_wrapper.cursor() as test_cursor:
                    test_cursor.execute("SELECT 1")  # Use raw SQL with cursor
                    test_cursor.fetchone()
            log.info("Database connection wrapper test successful.")
        except Exception as test_e:
            log.error(
                f"Database connection wrapper test failed: {test_e}",
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to establish initial database connection using wrapper: {test_e}"
            )

        log.info("Database initialization complete.")

    # ... (except blocks remain the same) ...
    except KeyError as e:
        log.critical(
            f"Database configuration missing key: {e}. Check config file structure.",
            exc_info=True,
        )
        sys.exit(1)
    except SQLAlchemyError as e:
        log.critical(
            f"SQLAlchemy error during engine creation or connection: {e}",
            exc_info=True,
        )
        sys.exit(1)
    # ... other specific exceptions like ImportError ...
    except RuntimeError as e:  # Catch the re-raised connection test error
        log.critical(str(e))
        sys.exit(1)
    except Exception as e:
        log.critical(
            f"Unexpected error during database initialization: {e}",
            exc_info=True,
        )
        sys.exit(1)
