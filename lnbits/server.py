import asyncio
import functools
import logging
import os
import platform
import socket
import sys
from typing import Sequence

import uvloop
from uvicorn import Server
from uvicorn.supervisors import ChangeReload

uvloop.install()

import multiprocessing as mp
import time
from pathlib import Path

import click
import uvicorn

from lnbits.settings import set_cli_settings, settings


logger = logging.getLogger("uvicorn.error")


class CustomServer(uvicorn.Server):
    async def startup(self, sockets: list = None) -> None:
        await self.lifespan.startup()
        if self.lifespan.should_exit:
            self.should_exit = True
            return

        config = self.config

        config.bind_socket()

        create_protocol = functools.partial(
            config.http_protocol_class, config=config, server_state=self.server_state
        )
        loop = asyncio.get_running_loop()

        listeners: Sequence[socket.SocketType] = []
        self.servers = []
        if sockets is not None:
            # Explicitly passed a list of open sockets.
            # We use this when the server is run from a Gunicorn worker.

            def _share_socket(sock: socket.SocketType) -> socket.SocketType:
                # Windows requires the socket be explicitly shared across
                # multiple workers (processes).
                from socket import fromshare  # type: ignore

                sock_data = sock.share(os.getpid())  # type: ignore
                return fromshare(sock_data)

            for sock in sockets:
                if config.workers > 1 and platform.system() == "Windows":
                    sock = _share_socket(sock)
                server = await loop.create_server(
                    create_protocol, sock=sock, ssl=config.ssl, backlog=config.backlog
                )
                self.servers.append(server)
            listeners.extend(sockets)

        if config.fd is not None:
            # Use an existing socket, from a file descriptor.
            sock = socket.fromfd(config.fd, socket.AF_UNIX, socket.SOCK_STREAM)
            server = await loop.create_server(
                create_protocol, sock=sock, ssl=config.ssl, backlog=config.backlog
            )
            assert server.sockets is not None  # mypy
            listeners.extend(server.sockets)
            self.servers.append(server)

        if config.uds is not None:
            # Create a socket using UNIX domain socket.
            uds_perms = 0o666
            if os.path.exists(config.uds):
                uds_perms = os.stat(config.uds).st_mode
            server = await loop.create_unix_server(
                create_protocol, path=config.uds, ssl=config.ssl, backlog=config.backlog
            )
            os.chmod(config.uds, uds_perms)
            assert server.sockets is not None  # mypy
            listeners.extend(server.sockets)
            self.servers.append(server)

        if not self.servers or (config.host or config.port):
            # Standard case. Create a socket from a host/port pair.
            try:
                server = await loop.create_server(
                    create_protocol,
                    host=config.host,
                    port=config.port,
                    ssl=config.ssl,
                    backlog=config.backlog,
                )
            except OSError as exc:
                logger.error(exc)
                await self.lifespan.shutdown()
                sys.exit(1)

            assert server.sockets is not None
            listeners.extend(server.sockets)
            self.servers.append(server)

        if sockets is None:
            self._log_started_message(listeners)
        else:
            # We're most likely running multiple workers, so a message has already been
            # logged by `config.bind_socket()`.
            pass

        self.started = True

    def _log_started_message(self, listeners: Sequence[socket.SocketType]) -> None:
        config = self.config

        socket_names = []

        if config.fd is not None:
            sock = listeners[0]
            socket_names.append(f"socket {sock.getsockname()}")

        if config.uds is not None:
            socket_names.append(f"unix socket {config.uds}")

        # if above conditions were false or host / port were specified explicitly
        if not socket_names or config.host or config.port:
            addr_format = "%s://%s:%d"
            host = "0.0.0.0" if config.host is None else config.host
            if ":" in host:
                # It's an IPv6 address.
                addr_format = "%s://[%s]:%d"

            port = config.port
            if port == 0:
                port = listeners[-1].getsockname()[1]

            protocol_name = "https" if config.ssl else "http"
            socket_names.append(addr_format % (protocol_name, host, port))

        socket_names = ', '.join(socket_names)
        message = f"Uvicorn running on {socket_names} (Press CTRL+C to quit)"
        color_message = (
            "Uvicorn running on "
            + click.style(socket_names, bold=True)
            + " (Press CTRL+C to quit)"
        )
        logger.info(
            message,
            extra={"color_message": color_message},
        )


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.option("--port", default=settings.port, help="Port to listen on")
@click.option("--host", default=settings.host, help="Host to run LNBits on")
@click.option(
    "--forwarded-allow-ips",
    default=settings.forwarded_allow_ips,
    help="Allowed proxy servers",
)
@click.option("--ssl-keyfile", default=None, help="Path to SSL keyfile")
@click.option("--ssl-certfile", default=None, help="Path to SSL certificate")
@click.pass_context
def main(
    ctx,
    port: int,
    host: str,
    forwarded_allow_ips: str,
    ssl_keyfile: str,
    ssl_certfile: str,
):
    """Launched with `poetry run lnbits` at root level"""

    # create data dir if it does not exist
    Path(settings.lnbits_data_folder).mkdir(parents=True, exist_ok=True)

    set_cli_settings(host=host, port=port, forwarded_allow_ips=forwarded_allow_ips)

    # this beautiful beast parses all command line arguments and passes them to the uvicorn server
    d = dict()
    for a in ctx.args:
        item = a.split("=")
        if len(item) > 1:  # argument like --key=value
            print(a, item)
            d[item[0].strip("--").replace("-", "_")] = (
                int(item[1])  # need to convert to int if it's a number
                if item[1].isdigit()
                else item[1]
            )
        else:
            d[a.strip("--")] = True  # argument like --key

    while True:
        config = uvicorn.Config(
            "lnbits.__main__:app",
            loop="uvloop",
            port=port,
            host=host,
            forwarded_allow_ips=forwarded_allow_ips,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            **d
        )

        uds = Path(settings.lnbits_socket_path, "lnbits.sock")

        if uds.exists():
            os.remove(uds)

        uds_config = uvicorn.Config(
            "lnbits.__main__:app", loop="uvloop", uds=str(uds), **d
        )

        sockets = [config.bind_socket(), uds_config.bind_socket()]

        server = Server(config=config)

        if config.should_reload:
            run = ChangeReload(config, target=server.run, sockets=sockets).run
        else:
            run = lambda: server.run(sockets=sockets)

        process = mp.Process(target=run)

        process.start()
        server_restart.wait()
        server_restart.clear()
        server.should_exit = True
        server.force_exit = True
        time.sleep(3)
        process.terminate()
        process.join()
        time.sleep(1)


server_restart = mp.Event()

if __name__ == "__main__":
    main()
