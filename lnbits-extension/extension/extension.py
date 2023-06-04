import json
from typing import Any

import httpx
import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute
from lnbits.core import User
from pydantic import UUID4, BaseSettings
from starlette.requests import Request
from starlette.responses import JSONResponse


def check_user_exists(self, usr: UUID4, request: Request) -> User:
    return User(**json.loads(request.query_params["user"]))


class ExtensionApiRouter(APIRouter):
    def add_api_route(self, *args, **kwargs):
        super().add_api_route(*args, **kwargs)
        route: APIRoute = self.routes[-1]
        for dep in route.dependencies:
            if dep.dependency == check_user_exists:
                if "lnbits" not in route.openapi_extra:
                    route.openapi_extra = {"lnbits": []}
                route.openapi_extra["lnbits"].append("require-user")


class ViewsRouter(APIRouter):
    def add_api_route(self, *args, **kwargs):
        kwargs["openapi_extra"] = {"lnbits": {"template-response": True}}
        route = super().add_api_route(*args, **kwargs)


class EnvSettings(BaseSettings):
    lnbits_uds: str
    lnbits_db_url: str
    lnbits_extension_uds: str
    lnbits_extension_secret: str

    class Config:
        case_sensitive = False


class LnbitsExtension:
    def __init__(self, *args, name: str, **kwargs):
        self.app = FastAPI(*args, **kwargs)
        self.settings = EnvSettings()

        self.openapi_url = f"/{name}/openapi.json"

        self.views = ViewsRouter()
        self.api = APIRouter(prefix=f"/api/v1")

        self.client = httpx.AsyncClient(
            base_url="http://host.docker.internal:5000",
            headers={
                "X-Lnbits-Extension-Secret": self.settings.lnbits_extension_secret
            },
        )

        @self.app.on_event("startup")
        async def startup():
            await self.client.post("/api/v1/extension/register")

        @self.app.on_event("shutdown")
        async def shutdown():
            await self.client.aclose()

    def run(self):
        uvicorn.run("main:app", uds=self.settings.lnbits_extension_uds, reload=True)

    def get_app(self):
        self.app.include_router(self.views)
        self.app.include_router(self.api)
        return self.app

    async def check_user_exists(self, usr: UUID4):
        resp = await self.client.get(f"/api/v1/users", params={"usr": str(usr)})
        resp.raise_for_status()
        return resp.json()


class LnbitsTemplateResponse(JSONResponse):
    def __init__(self, *args, template: str, context: Any = None, **kwargs):
        super().__init__(
            *args, content={"template": template, "context": context}, **kwargs
        )
