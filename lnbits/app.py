import asyncio
import glob
import importlib
import logging
import os
import shutil
import signal
import sys
import traceback
from http import HTTPStatus
from typing import Callable, List, Optional
from uuid import UUID

import httpx
from fastapi import Depends, FastAPI, Query, Request, Security
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger
from pydantic import UUID4
from starlette.background import BackgroundTask
from starlette.datastructures import MultiDict
from starlette.responses import Response, StreamingResponse

from lnbits.core.crud import get_installed_extensions
from lnbits.core.helpers import migrate_extension_database
from lnbits.core.tasks import register_task_listeners
from lnbits.settings import get_wallet_class, set_wallet_class, settings

from .commands import db_versions, load_disabled_extension_list, migrate_databases
from .core import (
    add_installed_extension,
    core_app,
    core_app_extra,
    update_installed_extension_state,
)
from .core.services import check_admin_settings
from .core.views.generic import core_html_routes
from .decorators import (
    WalletTypeInfo,
    api_key_header,
    api_key_query,
    check_user_exists,
    get_key_type,
    require_admin_key,
    require_invoice_key,
)
from .extension_manager import (
    Extension,
    InstallableExtension,
    RunningExtension,
    extension_manager,
    get_valid_extensions,
    run_process,
)
from .helpers import template_renderer
from .middleware import (
    ExtensionPermissionMiddleware,
    ExtensionsRedirectMiddleware,
    InstalledExtensionMiddleware,
)
from .requestvars import g
from .tasks import (
    catch_everything_and_restart,
    check_pending_payments,
    internal_invoice_listener,
    invoice_listener,
    webhook_handler,
)


def create_app() -> FastAPI:
    configure_logger()

    app = FastAPI(
        title="LNbits API",
        description="API for LNbits, the free and open source bitcoin wallet and accounts system with plugins.",
        version=settings.version,
        license_info={
            "name": "MIT License",
            "url": "https://raw.githubusercontent.com/lnbits/lnbits/main/LICENSE",
        },
    )

    app.mount("/static", StaticFiles(packages=[("lnbits", "static")]), name="static")
    app.mount(
        "/core/static",
        StaticFiles(packages=[("lnbits.core", "static")]),
        name="core_static",
    )

    g().base_url = f"http://{settings.host}:{settings.port}"

    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
    )

    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # order of these two middlewares is important
    app.add_middleware(InstalledExtensionMiddleware)
    app.add_middleware(ExtensionsRedirectMiddleware)
    app.add_middleware(ExtensionPermissionMiddleware)

    register_startup(app)
    register_routes(app)
    register_async_tasks(app)
    register_exception_handlers(app)
    register_shutdown(app)

    # Allow registering new extensions routes without direct access to the `app` object
    setattr(core_app_extra, "register_new_ext_routes", register_new_ext_routes(app))

    return app


async def check_funding_source() -> None:
    original_sigint_handler = signal.getsignal(signal.SIGINT)

    def signal_handler(signal, frame):
        logger.debug("SIGINT received, terminating LNbits.")
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)

    WALLET = get_wallet_class()

    # fallback to void after 30 seconds of failures
    sleep_time = 5
    timeout = int(30 / sleep_time)

    balance = 0
    retry_counter = 0

    while True:
        try:
            error_message, balance = await WALLET.status()
            if not error_message:
                retry_counter = 0
                break

            logger.error(
                f"The backend for {WALLET.__class__.__name__} isn't working properly: '{error_message}'",
                RuntimeWarning,
            )
        except:
            pass

        if settings.lnbits_admin_ui and retry_counter == timeout:
            logger.warning(
                f"Fallback to VoidWallet, because the backend for {WALLET.__class__.__name__} isn't working properly"
            )
            set_wallet_class("VoidWallet")
            WALLET = get_wallet_class()
            break
        else:
            logger.warning(f"Retrying connection to backend in {sleep_time} seconds...")
            retry_counter += 1
            await asyncio.sleep(sleep_time)

    signal.signal(signal.SIGINT, original_sigint_handler)

    logger.info(
        f"✔️ Backend {WALLET.__class__.__name__} connected and with a balance of {balance} msat."
    )


async def check_installed_extensions(app: FastAPI):
    """
    Check extensions that have been installed, but for some reason no longer present in the 'lnbits/extensions' directory.
    One reason might be a docker-container that was re-created.
    The 'data' directory (where the '.zip' files live) is expected to persist state.
    Zips that are missing will be re-downloaded.
    """
    shutil.rmtree(os.path.join("lnbits", "upgrades"), True)
    await load_disabled_extension_list()
    installed_extensions = await build_all_installed_extensions_list()

    for ext in installed_extensions:
        try:
            installed = check_installed_extension_files(ext)
            if not installed:
                await restore_installed_extension(app, ext)
                logger.info(
                    f"✔️ Successfully re-installed extension: {ext.id} ({ext.installed_version})"
                )
        except Exception as e:
            logger.warning(e)
            logger.warning(
                f"Failed to re-install extension: {ext.id} ({ext.installed_version})"
            )


async def build_all_installed_extensions_list() -> List[InstallableExtension]:
    """
    Returns a list of all the installed extensions plus the extensions that
    MUST be installed by default (see LNBITS_EXTENSIONS_DEFAULT_INSTALL).
    """
    installed_extensions = await get_installed_extensions()

    installed_extensions_ids = [e.id for e in installed_extensions]
    for ext_id in settings.lnbits_extensions_default_install:
        if ext_id in installed_extensions_ids:
            continue

        ext_releases = await InstallableExtension.get_extension_releases(ext_id)
        release = ext_releases[0] if len(ext_releases) else None

        if release:
            ext_info = InstallableExtension(
                id=ext_id, name=ext_id, installed_release=release, icon=release.icon
            )
            installed_extensions.append(ext_info)

    return installed_extensions


def check_installed_extension_files(ext: InstallableExtension) -> bool:
    if ext.has_installed_version:
        return True

    zip_files = glob.glob(
        os.path.join(settings.lnbits_data_folder, "extensions", "*.zip")
    )

    if f"./{str(ext.zip_path)}" not in zip_files:
        ext.download_archive()
    ext.extract_archive()

    return False


async def restore_installed_extension(app: FastAPI, ext: InstallableExtension):
    await add_installed_extension(ext)
    await update_installed_extension_state(ext_id=ext.id, active=True)

    extension = Extension.from_installable_ext(ext)
    register_ext_routes(app, extension)

    current_version = (await db_versions()).get(ext.id, 0)
    await migrate_extension_database(extension, current_version)

    # mount routes for the new version
    core_app_extra.register_new_ext_routes(extension)
    if extension.upgrade_hash:
        ext.nofiy_upgrade()


def register_routes(app: FastAPI) -> None:
    """Register FastAPI routes / LNbits extensions."""
    app.include_router(core_app)
    app.include_router(core_html_routes)

    for ext in get_valid_extensions():
        try:
            register_ext_routes(app, ext)
        except Exception as e:
            logger.error(str(e))
            raise ImportError(
                f"Please make sure that the extension `{ext.code}` follows conventions."
            )

    @app.put("/extensions")
    async def register_extension():
        pass

    @app.api_route("/{extension}/{path:path}", include_in_schema=False)
    async def route_extensions(
        extension: str,
        path: str,
        request: Request,
        usr: UUID4 = Query(default=None),
        api_key_header: str = Security(api_key_header),
        api_key_query: str = Security(api_key_query),
    ):
        running = extension_manager.get_running(extension)

        if not running:
            raise HTTPException(status_code=404)

        if not running.ready:
            raise HTTPException(status_code=503)

        spec = await running.openapi()

        info = spec["paths"].get("/" + path, {})
        if info:
            openapi_path = info.get(request.method.lower(), {})
            info = openapi_path.get("lnbits")
        else:
            openapi_path = {}

        params = MultiDict(request.query_params)

        if any(
            param["required"] and param["name"] == "usr" and param["in"] == "query"
            for param in openapi_path.get("parameters", [])
        ):
            assert usr
            user = await check_user_exists(usr)
            params["user"] = user.json()

        wallet_info: Optional[WalletTypeInfo] = None
        if "require-key" in info:
            wallet_info = await get_key_type(request, api_key_header, api_key_query)
        elif "require-admin-key" in info:
            wallet_info = await require_admin_key(
                request, api_key_header, api_key_query
            )
        elif "require-invoice-key" in info:
            wallet_info = await require_invoice_key(
                request, api_key_header, api_key_query
            )

        if wallet_info:
            params["wallet"] = wallet_info.wallet
            params["wallet_type"] = wallet_info.wallet_type

        forward = running.client.build_request(
            request.method,
            f"/{path}",
            content=await request.body(),
            headers=dict(request.headers),
            params=params,
        )

        if info and "template-response" in info:
            response = await running.client.send(forward)
            response.raise_for_status()
            json = response.json()
            context = json["context"] or {}
            context["request"] = request
            context["user"] = user.dict()
            return template_renderer(
                [f"lnbits/extensions/{extension}/templates"], extension=extension
            ).TemplateResponse(json["template"], context)

        r = await running.client.send(forward, stream=True)

        return StreamingResponse(
            r.aiter_raw(),
            background=BackgroundTask(r.aclose),
            status_code=r.status_code,
            headers=r.headers,
        )


def register_new_ext_routes(app: FastAPI) -> Callable:
    # Returns a function that registers new routes for an extension.
    # The returned function encapsulates (creates a closure around) the `app` object but does expose it.
    def register_new_ext_routes_fn(ext: Extension):
        register_ext_routes(app, ext)

    return register_new_ext_routes_fn


def register_ext_routes(app: FastAPI, ext: Extension) -> None:
    """Register FastAPI routes for extension."""
    ext_module = importlib.import_module(ext.module_name)

    ext_route = getattr(ext_module, f"{ext.code}_ext")

    if hasattr(ext_module, f"{ext.code}_start"):
        ext_start_func = getattr(ext_module, f"{ext.code}_start")
        ext_start_func()

    if hasattr(ext_module, f"{ext.code}_static_files"):
        ext_statics = getattr(ext_module, f"{ext.code}_static_files")
        for s in ext_statics:
            app.mount(s["path"], s["app"], s["name"])

    if hasattr(ext_module, f"{ext.code}_redirect_paths"):
        ext_redirects = getattr(ext_module, f"{ext.code}_redirect_paths")
        settings.lnbits_extensions_redirects = [
            r for r in settings.lnbits_extensions_redirects if r["ext_id"] != ext.code
        ]
        for r in ext_redirects:
            r["ext_id"] = ext.code
            settings.lnbits_extensions_redirects.append(r)

    logger.trace(f"adding route for extension {ext_module}")

    prefix = f"/upgrades/{ext.upgrade_hash}" if ext.upgrade_hash != "" else ""
    app.include_router(router=ext_route, prefix=prefix)


def register_startup(app: FastAPI):
    @app.on_event("startup")
    async def lnbits_startup():
        try:
            # wait till migration is done
            await migrate_databases()

            # setup admin settings
            await check_admin_settings()

            log_server_info()

            # initialize WALLET
            set_wallet_class()

            # initialize funding source
            await check_funding_source()

            # check extensions after restart
            await check_installed_extensions(app)

            await extension_manager.start_all()

        except Exception as e:
            logger.error(str(e))
            raise ImportError("Failed to run 'startup' event.")


def register_shutdown(app: FastAPI):
    @app.on_event("shutdown")
    async def lnbits_shutdown():
        await extension_manager.stop_all()


def log_server_info():
    logger.info("Starting LNbits")
    logger.info(f"Version: {settings.version}")
    logger.info(f"Baseurl: {settings.lnbits_baseurl}")
    logger.info(f"Host: {settings.host}")
    logger.info(f"Port: {settings.port}")
    logger.info(f"Debug: {settings.debug}")
    logger.info(f"Site title: {settings.lnbits_site_title}")
    logger.info(f"Funding source: {settings.lnbits_backend_wallet_class}")
    logger.info(f"Data folder: {settings.lnbits_data_folder}")
    logger.info(f"Git version: {settings.lnbits_commit}")
    logger.info(f"Database: {get_db_vendor_name()}")
    logger.info(f"Service fee: {settings.lnbits_service_fee}")


def get_db_vendor_name():
    db_url = settings.lnbits_database_url
    return (
        "PostgreSQL"
        if db_url and db_url.startswith("postgres://")
        else "CockroachDB"
        if db_url and db_url.startswith("cockroachdb://")
        else "SQLite"
    )


def register_async_tasks(app):
    @app.route("/wallet/webhook")
    async def webhook_listener():
        return await webhook_handler()

    @app.on_event("startup")
    async def listeners():
        loop = asyncio.get_event_loop()
        loop.create_task(catch_everything_and_restart(check_pending_payments))
        loop.create_task(catch_everything_and_restart(invoice_listener))
        loop.create_task(catch_everything_and_restart(internal_invoice_listener))
        await register_task_listeners()
        # await run_deferred_async() # calle: doesn't do anyting?

    @app.on_event("shutdown")
    async def stop_listeners():
        pass


def register_exception_handlers(app: FastAPI):
    @app.exception_handler(Exception)
    async def exception_handler(request: Request, exc: Exception):
        etype, _, tb = sys.exc_info()
        traceback.print_exception(etype, exc, tb)
        logger.error(f"Exception: {str(exc)}")
        # Only the browser sends "text/html" request
        # not fail proof, but everything else get's a JSON response
        if (
            request.headers
            and "accept" in request.headers
            and "text/html" in request.headers["accept"]
        ):
            return template_renderer().TemplateResponse(
                "error.html", {"request": request, "err": f"Error: {str(exc)}"}
            )

        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={"detail": str(exc)},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ):
        logger.error(f"RequestValidationError: {str(exc)}")
        # Only the browser sends "text/html" request
        # not fail proof, but everything else get's a JSON response

        if (
            request.headers
            and "accept" in request.headers
            and "text/html" in request.headers["accept"]
        ):
            return template_renderer().TemplateResponse(
                "error.html",
                {"request": request, "err": f"Error: {str(exc)}"},
            )

        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={"detail": str(exc)},
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        logger.error(f"HTTPException {exc.status_code}: {exc.detail}")
        # Only the browser sends "text/html" request
        # not fail proof, but everything else get's a JSON response

        if (
            request.headers
            and "accept" in request.headers
            and "text/html" in request.headers["accept"]
        ):
            return template_renderer().TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "err": f"HTTP Error {exc.status_code}: {exc.detail}",
                },
            )

        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
        )


def configure_logger() -> None:
    logger.remove()
    log_level: str = "DEBUG" if settings.debug else "INFO"
    formatter = Formatter()
    logger.add(sys.stderr, level=log_level, format=formatter.format)

    logging.getLogger("uvicorn").handlers = [InterceptHandler()]
    logging.getLogger("uvicorn.access").handlers = [InterceptHandler()]


class Formatter:
    def __init__(self):
        self.padding = 0
        self.minimal_fmt: str = "<green>{time:YYYY-MM-DD HH:mm:ss.SS}</green> | <level>{level}</level> | <level>{message}</level>\n"
        if settings.debug:
            self.fmt: str = (
                "<green>{time:YYYY-MM-DD HH:mm:ss.SS}</green> | <level>{level: <4}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <level>{message}</level>\n"
            )
        else:
            self.fmt: str = self.minimal_fmt

    def format(self, record):
        function = "{function}".format(**record)  # pylint: disable=C0209
        if function == "emit":  # uvicorn logs
            return self.minimal_fmt
        return self.fmt


class InterceptHandler(logging.Handler):
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        logger.log(level, record.getMessage())
