import asyncio
import json
from pathlib import Path
from typing import Any, List, Optional, Type

import jinja2
import shortuuid
from lnbits.jinja2_templating import Jinja2Templates
from lnbits.requestvars import g
from lnbits.settings import settings
from loguru import logger
from pydantic.schema import (
    field_schema,
    get_flat_models_from_fields,
    get_model_name_map,
)

from .db import FilterModel


def urlsafe_short_hash() -> str:
    return shortuuid.uuid()


def url_for(endpoint: str, external: Optional[bool] = False, **params: Any) -> str:
    base = g().base_url if external else ""
    url_params = "?"
    for key, value in params.items():
        url_params += f"{key}={value}&"
    url = f"{base}{endpoint}{url_params}"
    return url


def generate_filter_params_openapi(model: Type[FilterModel], keep_optional=False):
    """
    Generate openapi documentation for Filters. This is intended to be used along parse_filters (see example)
    :param model: Filter model
    :param keep_optional: If false, all parameters will be optional, otherwise inferred from model
    """
    fields = list(model.__fields__.values())
    models = get_flat_models_from_fields(fields, set())
    namemap = get_model_name_map(models)
    params = []
    for field in fields:
        schema, definitions, _ = field_schema(field, model_name_map=namemap)

        # Support nested definition
        if "$ref" in schema:
            name = schema["$ref"].split("/")[-1]
            schema = definitions[name]

        description = "Supports Filtering"
        if schema["type"] == "object":
            description += f". Nested attributes can be filtered too, e.g. `{field.alias}.[additional].[attributes]`"
        if (
            hasattr(model, "__search_fields__")
            and field.name in model.__search_fields__
        ):
            description += ". Supports Search"

        parameter = {
            "name": field.alias,
            "in": "query",
            "required": field.required if keep_optional else False,
            "schema": schema,
            "description": description,
        }
        params.append(parameter)

    return {
        "parameters": params,
    }
