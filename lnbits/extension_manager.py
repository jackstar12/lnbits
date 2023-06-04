import asyncio
import hashlib
import json
import os
import secrets
import shutil
import sys
import zipfile
from copy import copy
from http import HTTPStatus
from pathlib import Path
from typing import Any, Awaitable, List, NamedTuple, Optional, Tuple
from urllib import request

import docker
import httpx
from docker.models.containers import Container
from fastapi import HTTPException
from lnbits.db import SQLITE, Database
from lnbits.settings import settings
from loguru import logger
from packaging import version
from pydantic import BaseModel, Field
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.exc import ProgrammingError


async def run_process(*args, on_line=None, name: str = "", **kwargs):
    """
    Call a subprocess, waiting for it to finish. If it exits with a non-zero code, an exception is thrown.
    """
    # kwargs["stdout"] = kwargs["stderr"] = asyncio.subprocess.PIPE
    process = await asyncio.create_subprocess_shell(
        *args, **kwargs, env=os.environ.copy()
    )

    try:
        # await process.communicate()
        await asyncio.gather(
            _read_stream(process.stdout, on_line=on_line, name=name),
            _read_stream(process.stderr, on_line=on_line, name=name),
        )

        code = process.returncode
        if code != 0:
            raise ValueError(f"Non-zero exit code by {process}")
    except asyncio.CancelledError:
        process.kill()
        raise


async def _read_stream(stream, on_line=None, name: str = ""):
    while True:
        raw = await stream.readline()
        if raw:
            line = raw.decode()
            split = line.split("]")

            if "ERROR" in line or "WARNING" in line or "INFO" in line:
                msg = name + split[2]
            else:
                msg = line

            msg = msg.strip("\n").strip()

            if "ERROR" in line:
                logger.error(msg)
            elif "WARNING" in line:
                logger.warning(msg)
            elif "INFO" in line:
                logger.info(msg)
            else:
                print(msg)

            if on_line:
                on_line(line)
        else:
            break


class ExplicitRelease(BaseModel):
    id: str
    name: str
    version: str
    archive: str
    hash: str
    dependencies: List[str] = []
    icon: Optional[str]
    short_description: Optional[str]
    min_lnbits_version: Optional[str]
    html_url: Optional[str]  # todo: release_url
    warning: Optional[str]
    info_notification: Optional[str]
    critical_notification: Optional[str]

    def is_version_compatible(self):
        if not self.min_lnbits_version:
            return True
        return version.parse(self.min_lnbits_version) <= version.parse(settings.version)


class GitHubRelease(BaseModel):
    id: str
    organisation: str
    repository: str


class Manifest(BaseModel):
    featured: List[str] = []
    extensions: List["ExplicitRelease"] = []
    repos: List["GitHubRelease"] = []


class GitHubRepoRelease(BaseModel):
    name: str
    tag_name: str
    zipball_url: str
    html_url: str


class GitHubRepo(BaseModel):
    stargazers_count: str
    html_url: str
    default_branch: str


class ExtensionConfig(BaseModel):
    name: str
    short_description: str
    tile: str = ""
    warning: Optional[str] = ""
    min_lnbits_version: Optional[str]

    def is_version_compatible(self):
        if not self.min_lnbits_version:
            return True
        return version.parse(self.min_lnbits_version) <= version.parse(settings.version)


def download_url(url, save_path):
    with request.urlopen(url) as dl_file:
        with open(save_path, "wb") as out_file:
            out_file.write(dl_file.read())


def file_hash(filename):
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()


async def fetch_github_repo_info(
    org: str, repository: str
) -> Tuple[GitHubRepo, GitHubRepoRelease, ExtensionConfig]:
    repo_url = f"https://api.github.com/repos/{org}/{repository}"
    error_msg = "Cannot fetch extension repo"
    repo = await gihub_api_get(repo_url, error_msg)
    github_repo = GitHubRepo.parse_obj(repo)

    lates_release_url = (
        f"https://api.github.com/repos/{org}/{repository}/releases/latest"
    )
    error_msg = "Cannot fetch extension releases"
    latest_release: Any = await gihub_api_get(lates_release_url, error_msg)

    config_url = f"https://raw.githubusercontent.com/{org}/{repository}/{github_repo.default_branch}/config.json"
    error_msg = "Cannot fetch config for extension"
    config = await gihub_api_get(config_url, error_msg)

    return (
        github_repo,
        GitHubRepoRelease.parse_obj(latest_release),
        ExtensionConfig.parse_obj(config),
    )


async def fetch_manifest(url) -> Manifest:
    error_msg = "Cannot fetch extensions manifest"
    manifest = await gihub_api_get(url, error_msg)
    return Manifest.parse_obj(manifest)


async def fetch_github_releases(org: str, repo: str) -> List[GitHubRepoRelease]:
    releases_url = f"https://api.github.com/repos/{org}/{repo}/releases"
    error_msg = "Cannot fetch extension releases"
    releases = await gihub_api_get(releases_url, error_msg)
    return [GitHubRepoRelease.parse_obj(r) for r in releases]


async def fetch_github_release_config(
    org: str, repo: str, tag_name: str
) -> Optional[ExtensionConfig]:
    config_url = (
        f"https://raw.githubusercontent.com/{org}/{repo}/{tag_name}/config.json"
    )
    error_msg = "Cannot fetch GitHub extension config"
    config = await gihub_api_get(config_url, error_msg)
    return ExtensionConfig.parse_obj(config)


async def gihub_api_get(url: str, error_msg: Optional[str]) -> Any:
    async with httpx.AsyncClient() as client:
        headers = (
            {"Authorization": "Bearer " + settings.lnbits_ext_github_token}
            if settings.lnbits_ext_github_token
            else None
        )
        resp = await client.get(
            url,
            headers=headers,
        )
        if resp.status_code != 200:
            logger.warning(f"{error_msg} ({url}): {resp.text}")
        resp.raise_for_status()
        return resp.json()


def icon_to_github_url(source_repo: str, path: Optional[str]) -> str:
    if not path:
        return ""
    _, _, *rest = path.split("/")
    tail = "/".join(rest)
    return f"https://github.com/{source_repo}/raw/main/{tail}"


class Extension(NamedTuple):
    code: str
    is_valid: bool
    is_admin_only: bool
    name: Optional[str] = None
    short_description: Optional[str] = None
    tile: Optional[str] = None
    contributors: Optional[List[str]] = None
    hidden: bool = False
    migration_module: Optional[str] = None
    db_name: Optional[str] = None
    upgrade_hash: Optional[str] = ""
    entrypoint: Optional[str] = None
    dockerfile: Optional[str] = None

    @property
    def module_name(self):
        return (
            f"lnbits.extensions.{self.code}"
            if self.upgrade_hash == ""
            else f"lnbits.upgrades.{self.code}-{self.upgrade_hash}.{self.code}"
        )

    @property
    def docker_name(self):
        return f"lnbits-{self.code}"

    @property
    def uds(self):
        return f"{settings.lnbits_socket_path}/{self.docker_name}.sock"

    @classmethod
    def from_installable_ext(cls, ext_info: "InstallableExtension") -> "Extension":
        return Extension(
            code=ext_info.id,
            is_valid=True,
            is_admin_only=False,  # todo: is admin only
            name=ext_info.name,
            upgrade_hash=ext_info.hash if ext_info.module_installed else "",
        )


# All subdirectories in the current directory, not recursive.


class RunningExtension(BaseModel):
    extension: Extension
    container: Optional[Container]
    client: Optional[httpx.AsyncClient]
    openapi_schema: Optional[dict] = None
    secret: str

    @property
    def ready(self):
        return self.client is not None

    async def openapi(self):
        if not self.openapi_schema:
            response = await self.client.get(f"/openapi.json")
            response.raise_for_status()
            self.openapi_schema = response.json()
        return self.openapi_schema

    class Config:
        arbitrary_types_allowed = True


class ExtensionManager:
    def __init__(self):
        p = Path(settings.lnbits_path, "extensions")
        Path(p).mkdir(parents=True, exist_ok=True)
        self._extension_folders: List[Path] = [f for f in p.iterdir() if f.is_dir()]
        self._running_extensions: dict[str, RunningExtension] = {}
        self.docker = docker.from_env()

    def get_extension(self, code: str) -> Optional[Extension]:
        for extension in self.extensions:
            if extension.code == code:
                return extension
        return None

    @property
    def extensions(self) -> List[Extension]:
        output: List[Extension] = []

        for extension_folder in self._extension_folders:
            extension_code = extension_folder.parts[-1]
            try:
                with open(extension_folder / "config.json") as json_file:
                    config = json.load(json_file)
                is_valid = True
                is_admin_only = extension_code in settings.lnbits_admin_extensions
            except Exception:
                config = {}
                is_valid = False
                is_admin_only = False

            output.append(
                Extension(
                    extension_code,
                    is_valid,
                    is_admin_only,
                    config.get("name"),
                    config.get("short_description"),
                    config.get("tile"),
                    config.get("contributors"),
                    config.get("hidden") or False,
                    config.get("migration_module"),
                    config.get("db_name"),
                    entrypoint=config.get("entrypoint"),
                    dockerfile=config.get("dockerfile"),
                )
            )

        return output

    @property
    def runnable_extensions(self) -> List[Extension]:
        return [e for e in self.extensions if e.is_valid and e.entrypoint]

    def get_running(self, code: str) -> Optional[RunningExtension]:
        return self._running_extensions.get(code)

    def get_running_by_secret(self, secret: str) -> Optional[RunningExtension]:
        for running in self._running_extensions.values():
            if running.secret == secret:
                return running
        return None

    async def start_all(self):
        for extension in self.extensions:
            await self.start_extension(extension)

    async def stop_all(self):
        for extension in self.extensions:
            await self.stop_extension(extension)

    async def start_extension(self, extension: Extension):
        if extension.entrypoint:
            existing: list[Container] = self.docker.containers.list(
                filters={"name": extension.docker_name}
            )
            if existing:
                existing[0].stop(timeout=5)

            db = Database("database")

            volumes = [
                f"{settings.lnbits_path}/extensions/{extension.code}:/app",  # for development purposes
                f"{settings.lnbits_socket_path.resolve()}:/app/{settings.lnbits_socket_path}",
            ]

            secret = secrets.token_hex()

            if db.type == SQLITE:
                db_path = Path(settings.lnbits_data_folder, f"{extension.name}.sqlite3")
                volumes.append(f"{db_path.resolve()}:/app/{db_path}")
                db_url = f"sqlite:///{db_path}"
            else:
                try:
                    await db.execute(
                        f"""
                        CREATE USER {extension.code} WITH PASSWORD '{secret}';
                        GRANT ALL ON SCHEMA {extension.code} TO {extension.code};
                        """,
                    )
                except ProgrammingError:
                    await db.execute(
                        f"ALTER USER {extension.code} WITH PASSWORD ?;", (secret,)
                    )

                db_url = make_url(settings.lnbits_database_url)
                db_url.username = extension.code
                db_url.password = secret
                db_url.host = "host.docker.internal"
                db_url = str(db_url)

            if Path(extension.uds).exists():
                os.remove(extension.uds)

            container = self.docker.containers.run(
                image=extension.docker_name,
                remove=True,
                name=extension.docker_name,
                environment={
                    "LNBITS_UDS": f"{settings.lnbits_socket_path}/lnbits.sock",
                    "LNBITS_DB_URL": db_url,
                    "LNBITS_EXTENSION_UDS": extension.uds,
                    "LNBITS_EXTENSION_SECRET": secret,
                },
                volumes=volumes,
                detach=True,
            )

            def watcher():
                for line in container.logs(stream=True):
                    print(f'{extension.code}: {line.decode("utf-8").strip()}')

            if False:
                proc = run_process(
                    f"""
                        docker run --rm \\
                        --name {extension.code} \\
                        --env LNBITS_UDS={settings.lnbits_socket_path}/main.sock \\
                        --env LNBITS_DB_URL={settings.lnbits_data_folder}/db.sqlite3 \\
                        --env LNBITS_EXTENSION_UDS={extension.uds} \\
                        -v {settings.lnbits_socket_path.resolve()}:/app/{settings.lnbits_socket_path} \\
                        lnbits-{extension.code} \\
                        """,
                    name=extension.code,
                )
            elif False:
                proc = run_process(
                    f"""
                    cd {settings.lnbits_path}/extensions/{extension.code} &&
                    export LNBITS_UDS={settings.lnbits_socket_path.resolve()}/main.sock &&
                    export LNBITS_DB_URL={settings.lnbits_data_folder}/db.sqlite3 &&
                    export LNBITS_EXTENSION_UDS={extension.uds} &&
                    {extension.entrypoint}
                    """,
                    name=extension.code,
                )
            else:
                asyncio.get_running_loop().run_in_executor(None, watcher)
            self._running_extensions[extension.code] = RunningExtension(
                extension=extension,
                container=container,
                client=httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(uds=extension.uds),
                    base_url=f"http://{extension.code}",
                ),
                secret=secret,
            )

    async def stop_extension(self, extension: Extension):
        if extension.code in self._running_extensions:
            logger.info("Stopping extension: " + extension.code)
            running = self._running_extensions.pop(extension.code)
            if running.container:
                running.container.stop()
            if running.task:
                running.task.cancel()
            await running.client.aclose()


class ExtensionRelease(BaseModel):
    name: str
    version: str
    archive: str
    source_repo: str
    is_github_release: bool = False
    hash: Optional[str] = None
    min_lnbits_version: Optional[str] = None
    is_version_compatible: Optional[bool] = True
    html_url: Optional[str] = None
    description: Optional[str] = None
    warning: Optional[str] = None
    icon: Optional[str] = None

    @classmethod
    def from_github_release(
        cls, source_repo: str, r: "GitHubRepoRelease"
    ) -> "ExtensionRelease":
        return ExtensionRelease(
            name=r.name,
            description=r.name,
            version=r.tag_name,
            archive=r.zipball_url,
            source_repo=source_repo,
            is_github_release=True,
            # description=r.body, # bad for JSON
            html_url=r.html_url,
        )

    @classmethod
    def from_explicit_release(
        cls, source_repo: str, e: "ExplicitRelease"
    ) -> "ExtensionRelease":
        return ExtensionRelease(
            name=e.name,
            version=e.version,
            archive=e.archive,
            hash=e.hash,
            source_repo=source_repo,
            description=e.short_description,
            min_lnbits_version=e.min_lnbits_version,
            is_version_compatible=e.is_version_compatible(),
            warning=e.warning,
            html_url=e.html_url,
            icon=e.icon,
        )

    @classmethod
    async def all_releases(cls, org: str, repo: str) -> List["ExtensionRelease"]:
        try:
            github_releases = await fetch_github_releases(org, repo)
            return [
                ExtensionRelease.from_github_release(f"{org}/{repo}", r)
                for r in github_releases
            ]
        except Exception as e:
            logger.warning(e)
            return []


class InstallableExtension(BaseModel):
    id: str
    name: str
    short_description: Optional[str] = None
    icon: Optional[str] = None
    dependencies: List[str] = []
    is_admin_only: bool = False
    stars: int = 0
    featured = False
    latest_release: Optional[ExtensionRelease] = None
    installed_release: Optional[ExtensionRelease] = None
    archive: Optional[str] = None

    @property
    def hash(self) -> str:
        if self.installed_release:
            if self.installed_release.hash:
                return self.installed_release.hash
            m = hashlib.sha256()
            m.update(f"{self.installed_release.archive}".encode())
            return m.hexdigest()
        return "not-installed"

    @property
    def zip_path(self) -> Path:
        extensions_data_dir = Path(settings.lnbits_data_folder, "extensions")
        Path(extensions_data_dir).mkdir(parents=True, exist_ok=True)
        return Path(extensions_data_dir, f"{self.id}.zip")

    @property
    def ext_dir(self) -> Path:
        return Path(settings.lnbits_path, "extensions", self.id)

    @property
    def ext_upgrade_dir(self) -> Path:
        return Path("lnbits", "upgrades", f"{self.id}-{self.hash}")

    @property
    def module_name(self) -> str:
        return f"lnbits.extensions.{self.id}"

    @property
    def module_installed(self) -> bool:
        return self.module_name in sys.modules

    @property
    def has_installed_version(self) -> bool:
        if not self.ext_dir.is_dir():
            return False
        return Path(self.ext_dir, "config.json").is_file()

    @property
    def installed_version(self) -> str:
        if self.installed_release:
            return self.installed_release.version
        return ""

    def download_archive(self):
        logger.info(f"Downloading extension {self.name} ({self.installed_version}).")
        ext_zip_file = self.zip_path
        if ext_zip_file.is_file():
            os.remove(ext_zip_file)
        try:
            assert self.installed_release, "installed_release is none."
            download_url(self.installed_release.archive, ext_zip_file)
        except Exception as ex:
            logger.warning(ex)
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail="Cannot fetch extension archive file",
            )

        archive_hash = file_hash(ext_zip_file)
        if self.installed_release.hash and self.installed_release.hash != archive_hash:
            # remove downloaded archive
            if ext_zip_file.is_file():
                os.remove(ext_zip_file)
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail="File hash missmatch. Will not install.",
            )

    def extract_archive(self):
        logger.info(f"Extracting extension {self.name} ({self.installed_version}).")
        Path("lnbits", "upgrades").mkdir(parents=True, exist_ok=True)
        shutil.rmtree(self.ext_upgrade_dir, True)
        with zipfile.ZipFile(self.zip_path, "r") as zip_ref:
            zip_ref.extractall(self.ext_upgrade_dir)
        generated_dir_name = os.listdir(self.ext_upgrade_dir)[0]
        os.rename(
            Path(self.ext_upgrade_dir, generated_dir_name),
            Path(self.ext_upgrade_dir, self.id),
        )

        # Pre-packed extensions can be upgraded
        # Mark the extension as installed so we know it is not the pre-packed version
        with open(
            Path(self.ext_upgrade_dir, self.id, "config.json"), "r+"
        ) as json_file:
            config_json = json.load(json_file)

            self.name = config_json.get("name")
            self.short_description = config_json.get("short_description")

            if (
                self.installed_release
                and self.installed_release.is_github_release
                and config_json.get("tile")
            ):
                self.icon = icon_to_github_url(
                    self.installed_release.source_repo, config_json.get("tile")
                )

        shutil.rmtree(self.ext_dir, True)
        shutil.copytree(
            Path(self.ext_upgrade_dir, self.id),
            Path(settings.lnbits_path, "extensions", self.id),
        )
        logger.success(f"Extension {self.name} ({self.installed_version}) installed.")

    def nofiy_upgrade(self) -> None:
        """Update the list of upgraded extensions. The middleware will perform redirects based on this"""

        clean_upgraded_exts = list(
            filter(
                lambda old_ext: not old_ext.endswith(f"/{self.id}"),
                settings.lnbits_upgraded_extensions,
            )
        )
        settings.lnbits_upgraded_extensions = clean_upgraded_exts + [
            f"{self.hash}/{self.id}"
        ]

    def clean_extension_files(self):
        # remove downloaded archive
        if self.zip_path.is_file():
            os.remove(self.zip_path)

        # remove module from extensions
        shutil.rmtree(self.ext_dir, True)

        shutil.rmtree(self.ext_upgrade_dir, True)

    def check_latest_version(self, release: Optional[ExtensionRelease]):
        if not release:
            return
        if not self.latest_release:
            self.latest_release = release
            return
        if version.parse(self.latest_release.version) < version.parse(release.version):
            self.latest_release = release

    @classmethod
    def from_row(cls, data: dict) -> "InstallableExtension":
        meta = json.loads(data["meta"])
        ext = InstallableExtension(**data)
        if "installed_release" in meta:
            ext.installed_release = ExtensionRelease(**meta["installed_release"])
        return ext

    @classmethod
    async def from_github_release(
        cls, github_release: GitHubRelease
    ) -> Optional["InstallableExtension"]:
        try:
            repo, latest_release, config = await fetch_github_repo_info(
                github_release.organisation, github_release.repository
            )

            return InstallableExtension(
                id=github_release.id,
                name=config.name,
                short_description=config.short_description,
                stars=int(repo.stargazers_count),
                icon=icon_to_github_url(
                    f"{github_release.organisation}/{github_release.repository}",
                    config.tile,
                ),
                latest_release=ExtensionRelease.from_github_release(
                    repo.html_url, latest_release
                ),
            )
        except Exception as e:
            logger.warning(e)
        return None

    @classmethod
    def from_explicit_release(cls, e: ExplicitRelease) -> "InstallableExtension":
        return InstallableExtension(
            id=e.id,
            name=e.name,
            archive=e.archive,
            short_description=e.short_description,
            icon=e.icon,
            dependencies=e.dependencies,
        )

    @classmethod
    async def get_installable_extensions(
        cls,
    ) -> List["InstallableExtension"]:
        extension_list: List[InstallableExtension] = []
        extension_id_list: List[str] = []

        for url in settings.lnbits_extensions_manifests:
            try:
                manifest = await fetch_manifest(url)

                for r in manifest.repos:
                    ext = await InstallableExtension.from_github_release(r)
                    if not ext:
                        continue
                    existing_ext = next(
                        (ee for ee in extension_list if ee.id == r.id), None
                    )
                    if existing_ext:
                        existing_ext.check_latest_version(ext.latest_release)
                        continue

                    ext.featured = ext.id in manifest.featured
                    extension_list += [ext]
                    extension_id_list += [ext.id]

                for e in manifest.extensions:
                    release = ExtensionRelease.from_explicit_release(url, e)
                    existing_ext = next(
                        (ee for ee in extension_list if ee.id == e.id), None
                    )
                    if existing_ext:
                        existing_ext.check_latest_version(release)
                        continue
                    ext = InstallableExtension.from_explicit_release(e)
                    ext.check_latest_version(release)
                    ext.featured = ext.id in manifest.featured
                    extension_list += [ext]
                    extension_id_list += [e.id]
            except Exception as e:
                logger.warning(f"Manifest {url} failed with '{str(e)}'")

        return extension_list

    @classmethod
    async def get_extension_releases(cls, ext_id: str) -> List["ExtensionRelease"]:
        extension_releases: List[ExtensionRelease] = []

        for url in settings.lnbits_extensions_manifests:
            try:
                manifest = await fetch_manifest(url)
                for r in manifest.repos:
                    if r.id == ext_id:
                        repo_releases = await ExtensionRelease.all_releases(
                            r.organisation, r.repository
                        )
                        extension_releases += repo_releases

                for e in manifest.extensions:
                    if e.id == ext_id:
                        extension_releases += [
                            ExtensionRelease.from_explicit_release(url, e)
                        ]

            except Exception as e:
                logger.warning(f"Manifest {url} failed with '{str(e)}'")

        return extension_releases

    @classmethod
    async def get_extension_release(
        cls, ext_id: str, source_repo: str, archive: str
    ) -> Optional["ExtensionRelease"]:
        all_releases: List[
            ExtensionRelease
        ] = await InstallableExtension.get_extension_releases(ext_id)
        selected_release = [
            r
            for r in all_releases
            if r.archive == archive and r.source_repo == source_repo
        ]

        return selected_release[0] if len(selected_release) != 0 else None


class CreateExtension(BaseModel):
    ext_id: str
    archive: str
    source_repo: str


def get_valid_extensions() -> List[Extension]:
    return [
        extension for extension in ExtensionManager().extensions if extension.is_valid
    ]


extension_manager = ExtensionManager()
