# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, cast, TYPE_CHECKING

from flask import current_app
from flask_babel import gettext as __
from marshmallow import fields, Schema
from marshmallow.validate import Range
from sqlalchemy import types
from sqlalchemy.engine.url import URL
from urllib3.exceptions import NewConnectionError

from superset.databases.utils import make_url_safe
from superset.db_engine_specs.base import (
    BaseEngineSpec,
    BasicParametersMixin,
    BasicParametersType,
    BasicPropertiesType,
)
from superset.db_engine_specs.exceptions import SupersetDBAPIDatabaseError
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.extensions import cache_manager
from superset.utils.core import GenericDataType
from superset.utils.hashing import md5_sha_from_str
from superset.utils.network import is_hostname_valid, is_port_open

if TYPE_CHECKING:
    import pandas as pd

    from superset.models.core import Database
    from superset.sql_parse import Table

logger = logging.getLogger(__name__)


class ClickHouseBaseEngineSpec(BaseEngineSpec):
    """Shared engine spec for ClickHouse."""

    time_groupby_inline = True

    _time_grain_expressions = {
        None: "{col}",
        "PT1M": "toStartOfMinute(toDateTime({col}))",
        "PT5M": "toDateTime(intDiv(toUInt32(toDateTime({col})), 300)*300)",
        "PT10M": "toDateTime(intDiv(toUInt32(toDateTime({col})), 600)*600)",
        "PT15M": "toDateTime(intDiv(toUInt32(toDateTime({col})), 900)*900)",
        "PT30M": "toDateTime(intDiv(toUInt32(toDateTime({col})), 1800)*1800)",
        "PT1H": "toStartOfHour(toDateTime({col}))",
        "P1D": "toStartOfDay(toDateTime({col}))",
        "P1W": "toMonday(toDateTime({col}))",
        "P1M": "toStartOfMonth(toDateTime({col}))",
        "P3M": "toStartOfQuarter(toDateTime({col}))",
        "P1Y": "toStartOfYear(toDateTime({col}))",
    }

    column_type_mappings = (
        (
            re.compile(r".*Enum.*", re.IGNORECASE),
            types.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r".*Array.*", re.IGNORECASE),
            types.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r".*UUID.*", re.IGNORECASE),
            types.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r".*Bool.*", re.IGNORECASE),
            types.Boolean(),
            GenericDataType.BOOLEAN,
        ),
        (
            re.compile(r".*String.*", re.IGNORECASE),
            types.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r".*Int\d+.*", re.IGNORECASE),
            types.INTEGER(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r".*Decimal.*", re.IGNORECASE),
            types.DECIMAL(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r".*DateTime.*", re.IGNORECASE),
            types.DateTime(),
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile(r".*Date.*", re.IGNORECASE),
            types.Date(),
            GenericDataType.TEMPORAL,
        ),
    )

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return "{col}"

    @classmethod
    def convert_dttm(
        cls, target_type: str, dttm: datetime, db_extra: dict[str, Any] | None = None
    ) -> str | None:
        sqla_type = cls.get_sqla_column_type(target_type)

        if isinstance(sqla_type, types.Date):
            return f"toDate('{dttm.date().isoformat()}')"
        if isinstance(sqla_type, types.DateTime):
            return f"""toDateTime('{dttm.isoformat(sep=" ", timespec="seconds")}')"""
        return None


class ClickHouseEngineSpec(ClickHouseBaseEngineSpec):
    """Engine spec for clickhouse_sqlalchemy connector"""

    engine = "clickhouse"
    engine_name = "ClickHouse"

    _show_functions_column = "name"
    supports_file_upload = False

    @classmethod
    def get_dbapi_exception_mapping(cls) -> dict[type[Exception], type[Exception]]:
        return {NewConnectionError: SupersetDBAPIDatabaseError}

    @classmethod
    def get_dbapi_mapped_exception(cls, exception: Exception) -> Exception:
        new_exception = cls.get_dbapi_exception_mapping().get(type(exception))
        if new_exception == SupersetDBAPIDatabaseError:
            return SupersetDBAPIDatabaseError("Connection failed")
        if not new_exception:
            return exception
        return new_exception(str(exception))

    @classmethod
    @cache_manager.cache.memoize()
    def get_function_names(cls, database: Database) -> list[str]:
        """
        Get a list of function names that are able to be called on the database.
        Used for SQL Lab autocomplete.

        :param database: The database to get functions for
        :return: A list of function names usable in the database
        """
        system_functions_sql = "SELECT name FROM system.functions"
        try:
            df = database.get_df(system_functions_sql)
            if cls._show_functions_column in df:
                return df[cls._show_functions_column].tolist()
            columns = df.columns.values.tolist()
            logger.error(
                "Payload from `%s` has the incorrect format. "
                "Expected column `%s`, found: %s.",
                system_functions_sql,
                cls._show_functions_column,
                ", ".join(columns),
                exc_info=True,
            )
            # if the results have a single column, use that
            if len(columns) == 1:
                return df[columns[0]].tolist()
        except Exception as ex:  # pylint: disable=broad-except
            logger.error(
                "Query `%s` fire error %s. ",
                system_functions_sql,
                str(ex),
                exc_info=True,
            )
            return []

        # otherwise, return no function names to prevent errors
        return []


class ClickHouseParametersSchema(Schema):
    username = fields.String(allow_none=True, metadata={"description": __("Username")})
    password = fields.String(allow_none=True, metadata={"description": __("Password")})
    host = fields.String(
        required=True, metadata={"description": __("Hostname or IP address")}
    )
    port = fields.Integer(
        allow_none=True,
        metadata={"description": __("Database port")},
        validate=Range(min=0, max=65535),
    )
    database = fields.String(
        allow_none=True, metadata={"description": __("Database name")}
    )
    encryption = fields.Boolean(
        dump_default=True,
        metadata={"description": __("Use an encrypted connection to the database")},
    )
    query = fields.Dict(
        keys=fields.Str(),
        values=fields.Raw(),
        metadata={"description": __("Additional parameters")},
    )
    ssh = fields.Boolean(
        required=False,
        metadata={"description": __("Use an ssh tunnel connection to the database")},
    )


try:
    from clickhouse_connect.common import set_setting
    from clickhouse_connect.datatypes.format import set_default_formats

    # override default formats for compatibility
    set_default_formats(
        "FixedString",
        "string",
        "IPv*",
        "string",
        "UInt64",
        "signed",
        "UUID",
        "string",
        "*Int256",
        "string",
        "*Int128",
        "string",
    )
    set_setting(
        "product_name",
        f"superset/{current_app.config.get('VERSION_STRING', 'dev')}",
    )
except ImportError:  # ClickHouse Connect not installed, do nothing
    pass


class ClickHouseConnectEngineSpec(BasicParametersMixin, ClickHouseEngineSpec):
    """Engine spec for clickhouse-connect connector"""

    engine = "clickhousedb"
    engine_name = "ClickHouse Connect (Superset)"

    default_driver = "connect"
    _function_names: list[str] = []

    # The clickhouse-connect driver can insert data, so the file upload flow that
    # the parent ClickHouseEngineSpec disables is re-enabled here. It needs the
    # df_to_sql override below to work — see the docstring there.
    supports_file_upload = True

    sqlalchemy_uri_placeholder = (
        "clickhousedb://user:password@host[:port][/dbname][?secure=value&=value...]"
    )
    parameters_schema = ClickHouseParametersSchema()
    encryption_parameters = {"secure": "true"}

    @classmethod
    def get_dbapi_exception_mapping(cls) -> dict[type[Exception], type[Exception]]:
        return {}

    @classmethod
    def get_dbapi_mapped_exception(cls, exception: Exception) -> Exception:
        new_exception = cls.get_dbapi_exception_mapping().get(type(exception))
        if new_exception == SupersetDBAPIDatabaseError:
            return SupersetDBAPIDatabaseError("Connection failed")
        if not new_exception:
            return exception
        return new_exception(str(exception))

    @classmethod
    def get_function_names(cls, database: Database) -> list[str]:
        # pylint: disable=import-outside-toplevel, import-error
        from clickhouse_connect.driver.exceptions import ClickHouseError

        if cls._function_names:
            return cls._function_names
        try:
            names = database.get_df(
                "SELECT name FROM system.functions UNION ALL "  # noqa: S608
                + "SELECT name FROM system.table_functions LIMIT 10000"
            )["name"].tolist()
            cls._function_names = names
            return names
        except ClickHouseError:
            logger.exception("Error retrieving system.functions")
            return []

    @classmethod
    def get_datatype(cls, type_code: str) -> str:
        # keep it lowercase, as ClickHouse types aren't typical SHOUTCASE ANSI SQL
        return type_code

    @classmethod
    def _upload_column_type(cls, series: pd.Series, nullable: bool) -> Any:
        """
        Map a pandas dtype to a native ClickHouse column type.

        The types must come from ``cc_sqlalchemy`` rather than ``sqlalchemy.types``:
        the ClickHouse DDL compiler only wraps a column in ``Nullable()`` when its
        type is a ``ChSqlaType``, so generic SQLAlchemy types silently produce a
        non-nullable column and the insert then fails on the first empty cell.
        """
        # pylint: disable=import-outside-toplevel, import-error
        import pandas as pd
        from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import (
            Bool,
            DateTime64,
            Float64,
            Int64,
            Nullable,
            String,
        )

        dtype = series.dtype
        if pd.api.types.is_bool_dtype(dtype):
            base = Bool()
        elif pd.api.types.is_integer_dtype(dtype):
            base = Int64()
        elif pd.api.types.is_float_dtype(dtype):
            # Float64, not Float32: CSV floats are parsed as float64 and mapping
            # them to Float32 loses precision silently.
            base = Float64()
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            base = DateTime64(3)
        elif pd.api.types.infer_dtype(series, skipna=True) in {
            "datetime",
            "datetime64",
            "date",
        }:
            # Object columns may still hold temporal values, e.g. parsed dates.
            base = DateTime64(3)
        else:
            base = String()
        # DateTime64 rather than DateTime throughout: DateTime only spans
        # 1970-2106, so a date outside it would fail or wrap.
        return Nullable(base) if nullable else base

    @classmethod
    def df_to_sql(
        cls,
        database: Database,
        table: Table,
        df: pd.DataFrame,
        to_sql_kwargs: dict[str, Any],
    ) -> None:
        """
        Upload a DataFrame to ClickHouse.

        ClickHouse requires every table to declare a table engine, which the
        ``CREATE TABLE`` emitted by pandas' ``to_sql`` does not — it fails with
        "requires an engine" before a single row is written. So the table is
        created here with a MergeTree engine and the rows are then appended.

        The engine can be tuned per database through its ``extra`` JSON::

            {"clickhouse_file_upload": {
                "order_by": ["col1", "col2"],
                "partition_by": "toYYYYMM(col1)",
                "primary_key": "col1",
                "settings": {"index_granularity": 8192}
            }}

        ``order_by`` defaults to ``tuple()`` (no sort key) so an upload works with
        no configuration. Columns named in ``order_by`` are created non-nullable,
        because a sort key cannot be nullable — an empty cell in one of those
        columns will fail the upload.
        """
        # pylint: disable=import-outside-toplevel, import-error
        import pandas as pd
        from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
        from sqlalchemy import Column, inspect, MetaData, Table as SqlaTable, text

        if_exists = to_sql_kwargs.get("if_exists", "fail")

        if to_sql_kwargs.get("index"):
            # Fold the index into the columns so the table we create matches what
            # gets inserted; the append below always passes index=False. Preserve
            # the uploader's requested index_label as the new column name(s).
            df = df.reset_index(names=to_sql_kwargs.get("index_label"))

        config = (database.get_extra() or {}).get("clickhouse_file_upload", {})
        order_by = config.get("order_by")
        if order_by:
            key_columns = set(
                order_by if isinstance(order_by, (list, tuple)) else [order_by]
            )
            engine_kwargs: dict[str, Any] = {"order_by": order_by}
        else:
            # MergeTree still requires an ORDER BY; an empty tuple means "none".
            key_columns = set()
            engine_kwargs = {"order_by": text("tuple()")}
        for key in ("partition_by", "primary_key", "settings"):
            if config.get(key):
                engine_kwargs[key] = config[key]

        with cls.get_engine(
            database, catalog=table.catalog, schema=table.schema
        ) as engine:
            has_table = inspect(engine).has_table(table.table, schema=table.schema)
            if has_table and if_exists == "fail":
                # Raise ValueError so the uploader surfaces its friendly
                # "table already exists" message (see UploadCommand).
                raise ValueError(f"Table {table.table} already exists.")
            if has_table and if_exists == "replace":
                SqlaTable(table.table, MetaData(), schema=table.schema).drop(
                    engine, checkfirst=True
                )
                has_table = False

            if not has_table:
                columns = [
                    Column(
                        str(name),
                        cls._upload_column_type(
                            df[name], nullable=str(name) not in key_columns
                        ),
                    )
                    for name in df.columns
                ]
                SqlaTable(
                    table.table,
                    MetaData(),
                    *columns,
                    schema=table.schema,
                    clickhousedb_engine=MergeTree(**engine_kwargs),
                ).create(engine)

            # clickhouse-connect writes NaN/NaT as-is and the server rejects them;
            # converting to None first is what produces real NULLs.
            df = df.astype(object).where(pd.notnull(df), None)

            insert_kwargs = {
                **to_sql_kwargs,
                "name": table.table,
                "if_exists": "append",
                "index": False,
            }
            insert_kwargs.pop("index_label", None)
            if table.schema:
                insert_kwargs["schema"] = table.schema
            if (
                engine.dialect.supports_multivalues_insert
                or cls.supports_multivalues_insert
            ):
                insert_kwargs["method"] = "multi"
            df.to_sql(con=engine, **insert_kwargs)

    @classmethod
    def build_sqlalchemy_uri(
        cls,
        parameters: BasicParametersType,
        encrypted_extra: dict[str, str] | None = None,
    ) -> str:
        url_params = parameters.copy()
        if url_params.get("encryption"):
            query = parameters.get("query", {}).copy()
            query.update(cls.encryption_parameters)
            url_params["query"] = query
        if not url_params.get("database"):
            url_params["database"] = "__default__"

        return str(
            URL.create(
                f"{cls.engine}+{cls.default_driver}",
                username=url_params.get("username"),
                password=url_params.get("password"),
                host=url_params.get("host"),
                port=url_params.get("port"),
                database=url_params.get("database"),
                query=url_params.get("query"),
            )
        )

    @classmethod
    def get_parameters_from_uri(
        cls, uri: str, encrypted_extra: dict[str, Any] | None = None
    ) -> BasicParametersType:
        url = make_url_safe(uri)
        query = dict(url.query)
        if "secure" in query:
            encryption = query.get("secure") == "true"
            query.pop("secure")
        else:
            encryption = False
        return BasicParametersType(
            username=url.username,
            password=url.password,
            host=url.host,
            port=url.port,
            database="" if url.database == "__default__" else cast(str, url.database),
            query=query,
            encryption=encryption,
        )

    @classmethod
    def validate_parameters(
        cls, properties: BasicPropertiesType
    ) -> list[SupersetError]:
        # pylint: disable=import-outside-toplevel, import-error
        from clickhouse_connect.driver import default_port

        parameters = properties.get("parameters", {})
        host = parameters.get("host", None)
        if not host:
            return [
                SupersetError(
                    "Hostname is required",
                    SupersetErrorType.CONNECTION_MISSING_PARAMETERS_ERROR,
                    ErrorLevel.WARNING,
                    {"missing": ["host"]},
                )
            ]
        if not is_hostname_valid(host):
            return [
                SupersetError(
                    "The hostname provided can't be resolved.",
                    SupersetErrorType.CONNECTION_INVALID_HOSTNAME_ERROR,
                    ErrorLevel.ERROR,
                    {"invalid": ["host"]},
                )
            ]
        port = parameters.get("port")
        if port is None:
            port = default_port("http", parameters.get("encryption", False))
        try:
            port = int(port)
        except (ValueError, TypeError):
            port = -1
        if port <= 0 or port >= 65535:
            return [
                SupersetError(
                    "Port must be a valid integer between 0 and 65535 (inclusive).",
                    SupersetErrorType.CONNECTION_INVALID_PORT_ERROR,
                    ErrorLevel.ERROR,
                    {"invalid": ["port"]},
                )
            ]
        if not is_port_open(host, port):
            return [
                SupersetError(
                    "The port is closed.",
                    SupersetErrorType.CONNECTION_PORT_CLOSED_ERROR,
                    ErrorLevel.ERROR,
                    {"invalid": ["port"]},
                )
            ]
        return []

    @staticmethod
    def _mutate_label(label: str) -> str:
        """
        Suffix with the first six characters from the md5 of the label to avoid
        collisions with original column names

        :param label: Expected expression label
        :return: Conditionally mutated label
        """
        return f"{label}_{md5_sha_from_str(label)[:6]}"
