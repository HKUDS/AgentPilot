"""
boxd Sandbox implementation.

This module provides a concrete implementation of BaseSandbox using boxd
(https://boxd.sh/?ref=AgentPilot) machines.
"""

import asyncio
import os
import re
import shlex
import threading
import uuid
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse

from openspace.utils.logging import Logger
from .sandbox import BaseSandbox
from ..types import SandboxOptions

logger = Logger.get_logger(__name__)

# Import boxd SDK components (optional dependency)
try:
    logger.debug("Attempting to import boxd...")
    from boxd import Boxd  # type: ignore

    logger.debug("Successfully imported boxd")
    BOXD_AVAILABLE = True
except ImportError as e:
    logger.debug(f"Failed to import boxd: {e}")
    Boxd = None  # type: ignore
    BOXD_AVAILABLE = False


def _proxy_name(port: int) -> str:
    return f"p{port}"


DEFAULT_DELETE_AFTER = 3600
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _with_env(command: str, envs: Dict[str, str]) -> str:
    """Export envs in the shell first, so every part of the command sees them."""
    if not envs:
        return command
    bad = [k for k in envs if not _ENV_NAME.match(str(k))]
    if bad:
        raise ValueError(f"Invalid environment variable name(s): {', '.join(map(repr, bad))}")
    exports = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in envs.items())
    return f"export {exports}; {command}"


class BoxdCommandHandle:
    """A command running in a boxd machine.

    Background commands stream their output to the ``on_stdout`` and
    ``on_stderr`` callbacks from a daemon thread. ``kill()`` ends the exec
    session, which stops the remote process group.
    """

    def __init__(
        self,
        stream: Any,
        on_stdout: Optional[Callable[[str], None]] = None,
        on_stderr: Optional[Callable[[str], None]] = None,
    ):
        self._stream = stream
        self._on_stdout = on_stdout
        self._on_stderr = on_stderr
        self.exit_code: Optional[int] = None
        self.error: Optional[BaseException] = None
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._pump, daemon=True)
        self._thread.start()

    def _emit(self, callback: Optional[Callable[[str], None]], data: str) -> None:
        if callback is None:
            return
        try:
            callback(data)
        except Exception as e:
            logger.debug(f"boxd sandbox output callback failed: {e}")

    def _pump(self) -> None:
        try:
            for chunk in self._stream.iter_chunks():
                text = chunk.data.decode("utf-8", errors="replace")
                self._emit(self._on_stderr if chunk.is_stderr else self._on_stdout, text)
            self.exit_code = self._stream.exit_code
        except Exception as e:
            self.error = e
        finally:
            self._done.set()

    @property
    def finished(self) -> bool:
        return self._done.is_set()

    def wait(self, timeout: Optional[float] = None) -> Optional[int]:
        """Wait for the command to end and return its exit code."""
        self._done.wait(timeout)
        return self.exit_code

    def kill(self) -> None:
        """Stop the command."""
        try:
            self._stream.close()
        finally:
            self._done.wait(5)


class BoxdSandbox(BaseSandbox):
    """boxd sandbox implementation for secure code execution.

    Each sandbox is one isolated boxd machine. ``stop()`` deletes it.
    """

    def __init__(self, options: SandboxOptions):
        """Initialize boxd sandbox.

        Args:
            options: Sandbox configuration options including:
                - api_key: boxd API key (or use BOXD_API_KEY env var)
                - org: boxd org to create the machine in (or BOXD_ORG env var)
                - region: boxd region, e.g. "fra" or "nyc" (or BOXD_REGION env var)
                - vcpu / memory: machine size, e.g. 2 and "8G" (org default if unset)
                - name_prefix: machine name prefix (default: "openspace-")
                - from_snapshot: create the machine from this snapshot
                - labels: labels to put on the machine (dict)
                - auto_suspend_timeout: seconds of no network traffic before the
                  machine goes to standby (org default if unset)
                - delete_after: delete the machine this many seconds after
                  creation, even if stop() is never called (default: 3600;
                  0 or None turns it off)
                - port: port served on the machine's HTTPS address (default: 3000,
                  the port SandboxConnector uses)
                - timeout: Command execution timeout in seconds
        """
        super().__init__(options)

        if not BOXD_AVAILABLE:
            raise ImportError("boxd SDK not found. Please install it with 'pip install \"openspace[boxd]\"'.")

        # Get API key from options or environment
        self.api_key = options.get("api_key") or os.environ.get("BOXD_API_KEY")
        if not self.api_key:
            raise ValueError(
                "boxd API key is required. Provide it via 'options.api_key'"
                " or the BOXD_API_KEY environment variable."
            )

        # Get sandbox configuration
        self.org = options.get("org") or os.environ.get("BOXD_ORG") or None
        self.region = options.get("region") or os.environ.get("BOXD_REGION") or None
        self.vcpu = options.get("vcpu")
        self.memory = options.get("memory")
        self.name_prefix = options.get("name_prefix", "openspace-")
        self.from_snapshot = options.get("from_snapshot")
        self.labels = options.get("labels") or None
        self.auto_suspend_timeout = options.get("auto_suspend_timeout")
        self.delete_after = options.get("delete_after", DEFAULT_DELETE_AFTER) or None
        self.port = int(options.get("port", 3000))
        self.timeout = options.get("timeout", 600)  # Default 10 minutes

        self._client: Any = None
        self._machine: Any = None
        self._process: Optional[BoxdCommandHandle] = None
        self._hosts: Dict[int, str] = {}

    async def start(self) -> bool:
        """Create a boxd machine and wait until it runs commands.

        Returns:
            True if sandbox started successfully, False otherwise.
        """
        if self._active:
            logger.debug("boxd sandbox already active")
            return True

        name = f"{self.name_prefix}{uuid.uuid4().hex[:12]}"
        try:
            self._client = Boxd(api_key=self.api_key, region=self.region)
            logger.debug(f"Creating boxd machine {name}")
            self._machine = await asyncio.to_thread(
                self._client.machines.create,
                name,
                org=self.org,
                isolated=True,
                from_snapshot=self.from_snapshot,
                vcpu=self.vcpu,
                memory=self.memory,
                region=self.region,
                labels=self.labels,
                auto_suspend_timeout=self.auto_suspend_timeout,
                delete_after=self.delete_after,
            )
            await asyncio.to_thread(self._client.machines.wait_until_ready, self._machine.id)
            # Serve `port` on the machine's own HTTPS address. MCP clients are
            # not browsers, so the address must not ask for a browser check.
            await asyncio.to_thread(self._client.machines.proxies.set_port, self._machine.id, self.port)
            await asyncio.to_thread(self._client.machines.set_bot_protection, self._machine.id, mode="off")
            host = urlparse(self._machine.access.url).hostname
            if not host:
                raise RuntimeError(f"boxd machine {self._machine.name} has no HTTPS address")
            self._hosts = {self.port: host}
            self._active = True
            logger.info(f"boxd sandbox started successfully (machine: {self._machine.name})")
            return True

        except Exception as e:
            logger.error(f"Failed to start boxd sandbox: {e}")
            await self._cleanup_failed_start(name)
            return False
        except BaseException:
            # Cancelled (timeout, shutdown): don't leave the machine behind.
            await self._cleanup_failed_start(name)
            raise

    async def _cleanup_failed_start(self, name: str) -> None:
        self._active = False
        if self._machine is None and self._client is not None:
            # create may have failed after the machine was made; find it by its unique name.
            try:
                found = await asyncio.to_thread(self._client.machines.get, name)
                if found.name == name:
                    self._machine = found
            except Exception:
                pass
        await self._delete_machine()
        self._close_client()

    def _close_client(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception as e:
                logger.debug(f"Error closing boxd client: {e}")
            self._client = None

    async def _delete_machine(self) -> None:
        if self._machine is None or self._client is None:
            return
        machine = self._machine
        try:
            logger.debug(f"Deleting boxd machine {machine.name}")
            await asyncio.to_thread(self._client.machines.delete, machine.id)
        except Exception as e:
            logger.warning(f"Error deleting boxd machine {machine.name} ({machine.id}): {e}")
        finally:
            self._machine = None
            self._hosts = {}

    async def stop(self) -> None:
        """Stop the boxd sandbox and delete its machine."""
        if not self._active and self._machine is None:
            logger.debug("boxd sandbox not active")
            return

        # Terminate any running process
        if self._process:
            try:
                logger.debug("Terminating sandbox process")
                await asyncio.to_thread(self._process.kill)
            except Exception as e:
                logger.warning(f"Error terminating sandbox process: {e}")
            finally:
                self._process = None

        await self._delete_machine()
        self._close_client()
        self._active = False
        logger.info("boxd sandbox stopped successfully")

    async def execute_safe(self, command: str, **kwargs) -> Any:
        """Execute a command safely in the boxd sandbox.

        Args:
            command: The command to execute
            **kwargs: Additional options:
                - envs: Environment variables (dict)
                - timeout: Command timeout in milliseconds (foreground only;
                  a background command runs until kill() or stop())
                - background: Run in background (bool)
                - on_stdout: Stdout callback function
                - on_stderr: Stderr callback function

        Returns:
            A BoxdCommandHandle for background commands, otherwise the
            boxd ExecResult (stdout, stderr, exit_code). A non-zero exit
            code is returned, not raised.
        """
        if not self._active or not self._machine:
            raise RuntimeError("boxd sandbox is not active. Call start() first.")

        envs = kwargs.get("envs") or {}
        timeout = kwargs.get("timeout", self.timeout * 1000)  # milliseconds
        background = kwargs.get("background", False)
        on_stdout = kwargs.get("on_stdout")
        on_stderr = kwargs.get("on_stderr")

        logger.debug(f"Executing command in boxd sandbox: {command}")
        command = _with_env(command, envs)

        if background:
            # stdin stays open until kill(): servers such as supergateway
            # exit as soon as their stdin closes.
            stream = await asyncio.to_thread(
                self._client.machines.stream_exec,
                self._machine.id,
                command=command,
            )
            self._process = BoxdCommandHandle(stream, on_stdout, on_stderr)
            return self._process

        result = await asyncio.to_thread(
            self._client.machines.exec,
            self._machine.id,
            command,
            timeout=timeout / 1000 if timeout else None,
        )
        if on_stdout and result.stdout:
            on_stdout(result.stdout)
        if on_stderr and result.stderr:
            on_stderr(result.stderr)
        return result

    def get_connector(self) -> Any:
        """Get the boxd client.

        Returns:
            The boxd client, or None if not active.
        """
        return self._client if self._active else None

    def get_host(self, port: int) -> str:
        """Get the public HTTPS host that forwards to a port in the machine.

        Args:
            port: The port number to get the host for

        Returns:
            The host name: ``<machine>.boxd.sh`` for the sandbox port,
            ``p<port>.<machine>.boxd.sh`` for any other port. Another port
            gets a new route here (a blocking call); it can take a while
            before that route serves HTTPS.

        Raises:
            RuntimeError: If sandbox is not active
        """
        if not self._active or not self._machine:
            raise RuntimeError("boxd sandbox is not active. Call start() first.")

        if port in self._hosts:
            return self._hosts[port]

        name = _proxy_name(port)
        proxies = self._client.machines.proxies.list(self._machine.id)
        if not any(p.name == name for p in proxies):
            self._client.machines.proxies.create(self._machine.id, name, port)
            proxies = self._client.machines.proxies.list(self._machine.id)
        for p in proxies:
            if p.name == name:
                self._hosts[port] = p.domain
                return p.domain
        raise RuntimeError(f"boxd proxy for port {port} was not created")

    @property
    def machine(self) -> Any:
        """Get the underlying boxd Machine."""
        return self._machine

    @property
    def process(self) -> Any:
        """Get the current running process handle."""
        return self._process
