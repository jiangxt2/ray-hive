# Copyright 2026 ray-hive contributors
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

"""Finite-deadline native Thrift transports and socket-only abort handling."""

from __future__ import annotations

import inspect
import math
import socket
import ssl
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeout
from contextlib import suppress
from dataclasses import dataclass
from importlib import import_module
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from typing import Any, cast

from ray_hive._errors import HiveCompatibilityError, HiveTransportError
from ray_hive._models import HiveConnectionOptions, HiveTransport

TBinaryProtocol: Any = import_module("thrift.protocol.TBinaryProtocol")
TSocket: Any = import_module("thrift.transport.TSocket")
TTransport: Any = import_module("thrift.transport.TTransport")

_DNS_RESOLUTION_SLOT = threading.BoundedSemaphore(value=1)
_SUPPORTED_THRIFT_VERSION = "0.22.0"


@dataclass(frozen=True, slots=True)
class SocketAbortHandle:
    """A thread-safe, idempotent handle that closes the live socket without RPCs."""

    thrift_socket: Any

    def abort(self) -> None:
        handle = getattr(self.thrift_socket, "handle", None)
        if handle is None:
            return
        with suppress(AttributeError, OSError, ValueError):
            handle.shutdown(socket.SHUT_RDWR)
        with suppress(AttributeError, OSError, ValueError):
            handle.close()
        self.thrift_socket.handle = None


@dataclass(slots=True)
class NativeTransport:
    """Opened Thrift transport, protocol, and independent abort boundary."""

    transport: Any
    protocol: Any
    abort_handle: SocketAbortHandle
    _closed: bool = False

    def close(self) -> None:
        if self._closed:
            return
        close_error: Exception | None = None
        try:
            self.transport.close()
        except Exception as exc:
            close_error = exc
        finally:
            handle = getattr(self.abort_handle.thrift_socket, "handle", None)
            if handle is not None:
                with suppress(AttributeError, OSError, ValueError):
                    handle.close()
                self.abort_handle.thrift_socket.handle = None
            self._closed = True
        if close_error is not None and not isinstance(close_error, AttributeError | ValueError):
            raise close_error


def _timeout_milliseconds(seconds: float) -> int:
    """Convert a positive seconds value without creating a non-blocking socket."""

    return max(1, math.ceil(seconds * 1000))


def _remaining_connect_time(deadline: float, *, action: str) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise HiveTransportError(f"HiveServer2 {action} deadline expired")
    return remaining


def _resolve_addresses(host: str, port: int, *, deadline: float) -> list[Any]:
    """Resolve one endpoint without allowing unbounded stuck resolver threads."""

    remaining = _remaining_connect_time(deadline, action="address resolution")
    if not _DNS_RESOLUTION_SLOT.acquire(timeout=remaining):
        raise HiveTransportError("HiveServer2 address resolution deadline expired")

    result: Future[list[Any]] = Future()

    def resolve() -> None:
        try:
            result.set_result(
                socket.getaddrinfo(
                    host,
                    port,
                    socket.AF_UNSPEC,
                    socket.SOCK_STREAM,
                    0,
                    0,
                )
            )
        except Exception as exc:
            result.set_exception(exc)
        finally:
            _DNS_RESOLUTION_SLOT.release()

    resolver = threading.Thread(target=resolve, name="ray-hive-dns", daemon=True)
    try:
        resolver.start()
    except Exception as exc:
        _DNS_RESOLUTION_SLOT.release()
        raise HiveTransportError("Unable to start HiveServer2 address resolution") from exc

    try:
        addresses = result.result(
            timeout=_remaining_connect_time(deadline, action="address resolution")
        )
    except HiveTransportError:
        raise
    except FutureTimeout as exc:
        raise HiveTransportError("HiveServer2 address resolution deadline expired") from exc
    except Exception as exc:
        raise HiveTransportError("Unable to resolve the HiveServer2 endpoint") from exc
    if not addresses:
        raise HiveTransportError("Unable to resolve the HiveServer2 endpoint")
    return addresses


def _require_supported_thrift_version() -> None:
    try:
        installed_version = package_version("thrift")
    except PackageNotFoundError as exc:
        raise HiveCompatibilityError("Unable to verify the installed Thrift version") from exc
    if installed_version != _SUPPORTED_THRIFT_VERSION:
        raise HiveCompatibilityError(
            f"ray-hive requires Thrift {_SUPPORTED_THRIFT_VERSION}, found {installed_version}"
        )


def _require_thrift_socket_hooks(thrift_socket: Any) -> Callable[[int, int], Any]:
    resolve_addresses = getattr(thrift_socket, "_resolveAddr", None)
    open_socket = getattr(thrift_socket, "_do_open", None)
    if not callable(resolve_addresses) or not callable(open_socket):
        raise HiveCompatibilityError(
            "Unsupported Thrift TSocket hook contract: expected callable "
            "_resolveAddr and _do_open hooks"
        )

    try:
        resolve_parameters = tuple(inspect.signature(resolve_addresses).parameters.values())
        open_parameters = tuple(inspect.signature(open_socket).parameters.values())
    except (TypeError, ValueError) as exc:
        raise HiveCompatibilityError("Unsupported Thrift TSocket hook signature") from exc

    positional_kinds = {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }
    if (
        resolve_parameters
        or len(open_parameters) != 2
        or any(parameter.kind not in positional_kinds for parameter in open_parameters)
    ):
        raise HiveCompatibilityError(
            "Unsupported Thrift TSocket hook signature: expected bound "
            "_resolveAddr() and _do_open(family, socket_type)"
        )
    return cast(Callable[[int, int], Any], open_socket)


def _install_connect_deadline(
    thrift_socket: Any,
    addresses: list[Any],
    *,
    deadline: float,
) -> None:
    """Supply pre-resolved addresses and refresh timeout before every address attempt."""

    open_socket = _require_thrift_socket_hooks(thrift_socket)

    def resolved_addresses() -> list[Any]:
        return addresses

    def open_with_remaining_time(family: int, socket_type: int) -> Any:
        thrift_socket.setTimeout(
            _timeout_milliseconds(_remaining_connect_time(deadline, action="connection"))
        )
        return open_socket(family, socket_type)

    # Thrift 0.22 invokes these instance hooks from TSocket.open(). Supplying
    # resolved addresses avoids a second blocking lookup, while wrapping socket
    # creation refreshes the shared deadline before every address attempt.
    thrift_socket._resolveAddr = resolved_addresses
    thrift_socket._do_open = open_with_remaining_time


def open_native_transport(connection: HiveConnectionOptions) -> NativeTransport:
    """Open the currently verified native binary/NOSASL transport."""

    if connection.transport is not HiveTransport.BINARY:
        raise HiveCompatibilityError(
            "Native HTTP transport is not yet in the verified compatibility profile"
        )
    if connection.auth != "NOSASL":
        raise HiveCompatibilityError("The initial native transport supports binary + NOSASL only")

    _require_supported_thrift_version()
    connect_deadline = time.monotonic() + (_timeout_milliseconds(connection.connect_timeout) / 1000)
    addresses = _resolve_addresses(
        connection.host,
        connection.port,
        deadline=connect_deadline,
    )

    if connection.tls.enabled:
        ssl_socket_module: Any = import_module("thrift.transport.TSSLSocket")
        if connection.tls.verify:
            context = ssl.create_default_context(cafile=connection.tls.ca_file)
        else:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        thrift_socket = ssl_socket_module.TSSLSocket(
            connection.host,
            connection.port,
            ssl_context=context,
            server_hostname=connection.tls.server_name or connection.host,
        )
    else:
        thrift_socket = TSocket.TSocket(connection.host, connection.port)
    _install_connect_deadline(thrift_socket, addresses, deadline=connect_deadline)
    buffered = TTransport.TBufferedTransport(thrift_socket)
    try:
        buffered.open()
    except Exception as exc:
        try:
            buffered.close()
        except Exception as cleanup_error:
            exc.add_note(
                f"Transport cleanup also failed after connect: {type(cleanup_error).__name__}"
            )
        handle = getattr(thrift_socket, "handle", None)
        if handle is not None:
            with suppress(AttributeError, OSError, ValueError):
                handle.close()
            thrift_socket.handle = None
        if isinstance(exc, HiveTransportError):
            raise
        raise HiveTransportError("Unable to connect to the HiveServer2 endpoint") from exc
    thrift_socket.setTimeout(_timeout_milliseconds(connection.rpc_timeout))
    protocol = TBinaryProtocol.TBinaryProtocol(buffered)
    return NativeTransport(buffered, protocol, SocketAbortHandle(thrift_socket))
