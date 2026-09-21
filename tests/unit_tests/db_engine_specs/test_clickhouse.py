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

from datetime import date, datetime
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import Mock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from sqlalchemy.types import (
    Boolean,
    Date,
    DateTime,
    DECIMAL,
    Float,
    Integer,
    String,
    TypeEngine,
)
from urllib3.connection import HTTPConnection
from urllib3.exceptions import NewConnectionError

from superset.sql_parse import Table
from superset.utils.core import GenericDataType
from tests.unit_tests.db_engine_specs.utils import (
    assert_column_spec,
    assert_convert_dttm,
)
from tests.unit_tests.fixtures.common import dttm  # noqa: F401


@pytest.mark.parametrize(
    "target_type,expected_result",
    [
        ("Date", "toDate('2019-01-02')"),
        ("DateTime", "toDateTime('2019-01-02 03:04:05')"),
        ("UnknownType", None),
    ],
)
def test_convert_dttm(
    target_type: str,
    expected_result: Optional[str],
    dttm: datetime,  # noqa: F811
) -> None:
    from superset.db_engine_specs.clickhouse import (
        ClickHouseEngineSpec as spec,  # noqa: N813
    )

    assert_convert_dttm(spec, target_type, expected_result, dttm)


def test_execute_connection_error() -> None:
    from superset.db_engine_specs.clickhouse import ClickHouseEngineSpec
    from superset.db_engine_specs.exceptions import SupersetDBAPIDatabaseError

    database = Mock()
    cursor = Mock()
    cursor.execute.side_effect = NewConnectionError(
        HTTPConnection("localhost"), "Exception with sensitive data"
    )
    with pytest.raises(SupersetDBAPIDatabaseError) as excinfo:
        ClickHouseEngineSpec.execute(cursor, "SELECT col1 from table1", database)
    assert str(excinfo.value) == "Connection failed"


@pytest.mark.parametrize(
    "target_type,expected_result",
    [
        ("Date", "toDate('2019-01-02')"),
        ("DateTime", "toDateTime('2019-01-02 03:04:05')"),
        ("UnknownType", None),
    ],
)
def test_connect_convert_dttm(
    target_type: str,
    expected_result: Optional[str],
    dttm: datetime,  # noqa: F811
) -> None:
    from superset.db_engine_specs.clickhouse import (
        ClickHouseEngineSpec as spec,  # noqa: N813
    )

    assert_convert_dttm(spec, target_type, expected_result, dttm)


@pytest.mark.parametrize(
    "native_type,sqla_type,attrs,generic_type,is_dttm",
    [
        ("String", String, None, GenericDataType.STRING, False),
        ("LowCardinality(String)", String, None, GenericDataType.STRING, False),
        ("Nullable(String)", String, None, GenericDataType.STRING, False),
        (
            "LowCardinality(Nullable(String))",
            String,
            None,
            GenericDataType.STRING,
            False,
        ),
        ("Array(UInt8)", String, None, GenericDataType.STRING, False),
        ("Enum('hello', 'world')", String, None, GenericDataType.STRING, False),
        ("Enum('UInt32', 'Bool')", String, None, GenericDataType.STRING, False),
        (
            "LowCardinality(Enum('hello', 'world'))",
            String,
            None,
            GenericDataType.STRING,
            False,
        ),
        (
            "Nullable(Enum('hello', 'world'))",
            String,
            None,
            GenericDataType.STRING,
            False,
        ),
        (
            "LowCardinality(Nullable(Enum('hello', 'world')))",
            String,
            None,
            GenericDataType.STRING,
            False,
        ),
        ("FixedString(16)", String, None, GenericDataType.STRING, False),
        ("Nullable(FixedString(16))", String, None, GenericDataType.STRING, False),
        (
            "LowCardinality(Nullable(FixedString(16)))",
            String,
            None,
            GenericDataType.STRING,
            False,
        ),
        ("UUID", String, None, GenericDataType.STRING, False),
        ("Int8", Integer, None, GenericDataType.NUMERIC, False),
        ("Int16", Integer, None, GenericDataType.NUMERIC, False),
        ("Int32", Integer, None, GenericDataType.NUMERIC, False),
        ("Int64", Integer, None, GenericDataType.NUMERIC, False),
        ("Int128", Integer, None, GenericDataType.NUMERIC, False),
        ("Int256", Integer, None, GenericDataType.NUMERIC, False),
        ("Nullable(Int256)", Integer, None, GenericDataType.NUMERIC, False),
        (
            "LowCardinality(Nullable(Int256))",
            Integer,
            None,
            GenericDataType.NUMERIC,
            False,
        ),
        ("UInt8", Integer, None, GenericDataType.NUMERIC, False),
        ("UInt16", Integer, None, GenericDataType.NUMERIC, False),
        ("UInt32", Integer, None, GenericDataType.NUMERIC, False),
        ("UInt64", Integer, None, GenericDataType.NUMERIC, False),
        ("UInt128", Integer, None, GenericDataType.NUMERIC, False),
        ("UInt256", Integer, None, GenericDataType.NUMERIC, False),
        ("Nullable(UInt256)", Integer, None, GenericDataType.NUMERIC, False),
        (
            "LowCardinality(Nullable(UInt256))",
            Integer,
            None,
            GenericDataType.NUMERIC,
            False,
        ),
        ("Float32", Float, None, GenericDataType.NUMERIC, False),
        ("Float64", Float, None, GenericDataType.NUMERIC, False),
        ("Decimal(1, 2)", DECIMAL, None, GenericDataType.NUMERIC, False),
        ("Decimal32(2)", DECIMAL, None, GenericDataType.NUMERIC, False),
        ("Decimal64(2)", DECIMAL, None, GenericDataType.NUMERIC, False),
        ("Decimal128(2)", DECIMAL, None, GenericDataType.NUMERIC, False),
        ("Decimal256(2)", DECIMAL, None, GenericDataType.NUMERIC, False),
        ("Bool", Boolean, None, GenericDataType.BOOLEAN, False),
        ("Nullable(Bool)", Boolean, None, GenericDataType.BOOLEAN, False),
        ("Date", Date, None, GenericDataType.TEMPORAL, True),
        ("Nullable(Date)", Date, None, GenericDataType.TEMPORAL, True),
        ("LowCardinality(Nullable(Date))", Date, None, GenericDataType.TEMPORAL, True),
        ("Date32", Date, None, GenericDataType.TEMPORAL, True),
        ("Datetime", DateTime, None, GenericDataType.TEMPORAL, True),
        ("Nullable(Datetime)", DateTime, None, GenericDataType.TEMPORAL, True),
        (
            "LowCardinality(Nullable(Datetime))",
            DateTime,
            None,
            GenericDataType.TEMPORAL,
            True,
        ),
        ("Datetime('UTC')", DateTime, None, GenericDataType.TEMPORAL, True),
        ("Datetime64(3)", DateTime, None, GenericDataType.TEMPORAL, True),
        ("Datetime64(3, 'UTC')", DateTime, None, GenericDataType.TEMPORAL, True),
    ],
)
def test_connect_get_column_spec(
    native_type: str,
    sqla_type: type[TypeEngine],
    attrs: Optional[dict[str, Any]],
    generic_type: GenericDataType,
    is_dttm: bool,
) -> None:
    from superset.db_engine_specs.clickhouse import (
        ClickHouseConnectEngineSpec as spec,  # noqa: N813
    )

    assert_column_spec(spec, native_type, sqla_type, attrs, generic_type, is_dttm)


@pytest.mark.parametrize(
    "column_name,expected_result",
    [
        ("time", "time_07cc69"),
        ("count", "count_e2942a"),
    ],
)
def test_connect_make_label_compatible(column_name: str, expected_result: str) -> None:
    from superset.db_engine_specs.clickhouse import (
        ClickHouseConnectEngineSpec as spec,  # noqa: N813
    )

    label = spec.make_label_compatible(column_name)
    assert label == expected_result


def test_connect_supports_file_upload() -> None:
    """
    File upload stays disabled on the legacy clickhouse-sqlalchemy spec but is
    enabled on the clickhouse-connect spec, whose driver can insert data.
    """
    from superset.db_engine_specs.clickhouse import (
        ClickHouseConnectEngineSpec,
        ClickHouseEngineSpec,
    )

    assert ClickHouseEngineSpec.supports_file_upload is False
    assert ClickHouseConnectEngineSpec.supports_file_upload is True
    assert (
        ClickHouseConnectEngineSpec.get_public_information()["supports_file_upload"]
        is True
    )


@pytest.mark.parametrize(
    "series,nullable,expected",
    [
        (pd.Series([1, 2, 3]), True, "Nullable(Int64)"),
        (pd.Series([1, 2, 3]), False, "Int64"),
        (pd.Series([1.5, 2.5]), True, "Nullable(Float64)"),
        (pd.Series([True, False]), True, "Nullable(Bool)"),
        (pd.Series(["a", "b"]), True, "Nullable(String)"),
        (pd.Series(["a", None]), True, "Nullable(String)"),
        (
            pd.Series(pd.to_datetime(["2026-01-01", "2026-02-01"])),
            True,
            "Nullable(DateTime64(3))",
        ),
        (
            pd.Series([date(1950, 5, 1), date(1999, 12, 31)]),
            True,
            "Nullable(DateTime64(3))",
        ),
    ],
)
def test_connect_upload_column_type(
    series: pd.Series, nullable: bool, expected: str
) -> None:
    """
    Upload columns must use native ClickHouse types.

    Generic SQLAlchemy types compile without the ``Nullable()`` wrapper — the
    ClickHouse DDL compiler only applies it to a ``ChSqlaType`` — which yields a
    non-nullable column and an insert that fails on the first empty cell.
    ``Float64``/``DateTime64`` rather than ``Float32``/``DateTime`` so float
    precision and pre-1970 dates survive.
    """
    pytest.importorskip("clickhouse_connect")
    from superset.db_engine_specs.clickhouse import ClickHouseConnectEngineSpec

    column_type = ClickHouseConnectEngineSpec._upload_column_type(  # noqa: SLF001
        series, nullable=nullable
    )
    assert column_type.compile() == expected


@pytest.fixture
def upload_mocks(mocker: MockerFixture) -> Any:
    """
    Wire up ``ClickHouseConnectEngineSpec.df_to_sql`` for unit testing without a
    live ClickHouse: ``get_engine``/``inspect`` are stubbed, the SQLAlchemy
    ``Table`` call is captured, and rows are "inserted" via a mocked
    clickhouse-connect client reached through ``engine.raw_connection()``
    rather than a real ``insert_df``.
    """
    pytest.importorskip("clickhouse_connect")
    from superset.db_engine_specs.clickhouse import ClickHouseConnectEngineSpec

    engine = mocker.MagicMock()
    engine.dialect.supports_multivalues_insert = False
    engine_ctx = mocker.MagicMock()
    engine_ctx.__enter__.return_value = engine
    mocker.patch.object(
        ClickHouseConnectEngineSpec, "get_engine", return_value=engine_ctx
    )

    inspector = mocker.MagicMock()
    inspector.has_table.return_value = False
    mocker.patch("sqlalchemy.inspect", return_value=inspector)

    client = mocker.MagicMock()
    engine.raw_connection.return_value.connection.client = client

    return SimpleNamespace(
        spec=ClickHouseConnectEngineSpec,
        engine=engine,
        inspector=inspector,
        table_factory=mocker.patch("sqlalchemy.Table"),
        insert_df=client.insert_df,
    )


@pytest.fixture
def upload_df() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2], "name": ["alpha", None]})


def test_connect_df_to_sql_default_engine(
    upload_mocks: Any, upload_df: pd.DataFrame
) -> None:
    """
    With no ``extra`` config the table is created with a MergeTree ordered by
    ``tuple()`` (no sort key), then the rows are appended.
    """
    database = Mock()
    database.get_extra.return_value = {}

    upload_mocks.spec.df_to_sql(
        database, Table("t"), upload_df, {"if_exists": "fail", "index": False}
    )

    upload_mocks.table_factory.return_value.create.assert_called_once()
    engine_arg = upload_mocks.table_factory.call_args.kwargs["clickhousedb_engine"]
    assert engine_arg.compile() == "Engine MergeTree ORDER BY tuple()"

    # Every column is nullable when there is no sort key.
    columns = upload_mocks.table_factory.call_args.args[2:]
    assert [c.type.compile() for c in columns] == [
        "Nullable(Int64)",
        "Nullable(String)",
    ]

    # Rows are appended via the native client, not re-created.
    args = upload_mocks.insert_df.call_args.args
    assert args[0] == "t"
    assert args[1]["id"].tolist() == [1, 2]


def test_connect_df_to_sql_configurable_engine(
    upload_mocks: Any, upload_df: pd.DataFrame
) -> None:
    """
    ``order_by``/``partition_by`` come from the database ``extra``, and columns
    named in ``order_by`` are created non-nullable because a MergeTree sort key
    cannot be nullable.
    """
    database = Mock()
    database.get_extra.return_value = {
        "clickhouse_file_upload": {
            "order_by": ["id"],
            "partition_by": "toYYYYMM(id)",
        }
    }

    upload_mocks.spec.df_to_sql(
        database, Table("t"), upload_df, {"if_exists": "fail", "index": False}
    )

    engine_arg = upload_mocks.table_factory.call_args.kwargs["clickhousedb_engine"]
    compiled = engine_arg.compile()
    assert "ORDER BY (id)" in compiled
    assert "PARTITION BY toYYYYMM(id)" in compiled

    columns = upload_mocks.table_factory.call_args.args[2:]
    assert [c.type.compile() for c in columns] == ["Int64", "Nullable(String)"]


def test_connect_df_to_sql_stringifies_mixed_type_string_columns(
    upload_mocks: Any,
) -> None:
    """
    A column with no single numpy dtype (e.g. IDs that are sometimes numeric,
    sometimes text) falls through ``_upload_column_type`` to String, but
    pandas leaves its cells as whatever raw Python objects it parsed.
    ``insert_df``'s String serializer fails on the first non-string cell with
    an opaque ``'int' object has no attribute 'encode'``, so those cells must
    be stringified (nulls excepted) before it sees them.
    """
    database = Mock()
    database.get_extra.return_value = {}
    df = pd.DataFrame({"emp_id": [123, "E456", None, 789]})

    upload_mocks.spec.df_to_sql(
        database, Table("t"), df, {"if_exists": "fail", "index": False}
    )

    inserted = upload_mocks.insert_df.call_args.args[1]
    assert inserted["emp_id"].tolist() == ["123", "E456", None, "789"]


def test_connect_df_to_sql_passes_df_unmodified(
    upload_mocks: Any, upload_df: pd.DataFrame
) -> None:
    """
    NaN/NaT scrubbing is delegated to clickhouse-connect's ``insert_df``, which
    handles it per column using the DataFrame's native numpy dtypes. The
    DataFrame must reach it as-is: coercing it to ``dtype=object`` first (as a
    manual None-conversion would) defeats those dtype checks.
    """
    database = Mock()
    database.get_extra.return_value = {}

    upload_mocks.spec.df_to_sql(
        database, Table("t"), upload_df, {"if_exists": "fail", "index": False}
    )

    inserted = upload_mocks.insert_df.call_args.args[1]
    assert inserted["name"].tolist() == ["alpha", None]
    assert inserted["id"].dtype == upload_df["id"].dtype


def test_connect_df_to_sql_if_exists_fail(
    upload_mocks: Any, upload_df: pd.DataFrame
) -> None:
    """
    ``if_exists='fail'`` on an existing table raises ValueError, which the
    uploader turns into its "table already exists" message.
    """
    database = Mock()
    database.get_extra.return_value = {}
    upload_mocks.inspector.has_table.return_value = True

    with pytest.raises(ValueError, match="already exists"):
        upload_mocks.spec.df_to_sql(
            database, Table("t"), upload_df, {"if_exists": "fail", "index": False}
        )

    upload_mocks.insert_df.assert_not_called()


def test_connect_df_to_sql_if_exists_replace(
    upload_mocks: Any, upload_df: pd.DataFrame
) -> None:
    """``if_exists='replace'`` drops the table before recreating it."""
    database = Mock()
    database.get_extra.return_value = {}
    upload_mocks.inspector.has_table.return_value = True

    upload_mocks.spec.df_to_sql(
        database, Table("t"), upload_df, {"if_exists": "replace", "index": False}
    )

    upload_mocks.table_factory.return_value.drop.assert_called_once()
    upload_mocks.table_factory.return_value.create.assert_called_once()
    upload_mocks.insert_df.assert_called_once()


def test_connect_df_to_sql_if_exists_append(
    upload_mocks: Any, upload_df: pd.DataFrame
) -> None:
    """``if_exists='append'`` on an existing table skips creation entirely."""
    database = Mock()
    database.get_extra.return_value = {}
    upload_mocks.inspector.has_table.return_value = True

    upload_mocks.spec.df_to_sql(
        database, Table("t"), upload_df, {"if_exists": "append", "index": False}
    )

    upload_mocks.table_factory.return_value.create.assert_not_called()
    upload_mocks.insert_df.assert_called_once()


def test_connect_df_to_sql_index_folded_into_columns(
    upload_mocks: Any, upload_df: pd.DataFrame
) -> None:
    """
    When the uploader asks for the index to be stored it becomes a real column,
    so the created table matches what is inserted.
    """
    database = Mock()
    database.get_extra.return_value = {}

    upload_mocks.spec.df_to_sql(
        database,
        Table("t"),
        upload_df,
        {"if_exists": "fail", "index": True, "index_label": "row_no"},
    )

    columns = upload_mocks.table_factory.call_args.args[2:]
    assert [c.name for c in columns] == ["row_no", "id", "name"]

    inserted = upload_mocks.insert_df.call_args.args[1]
    assert list(inserted.columns) == ["row_no", "id", "name"]


def test_connect_df_to_sql_batches_large_uploads(
    upload_mocks: Any, mocker: MockerFixture
) -> None:
    """
    Rows are written in ``UPLOAD_INSERT_BATCH_SIZE``-sized native inserts
    rather than the generic uploader's small ``chunksize`` — each
    ``insert_df`` call becomes its own MergeTree part, so batching this way
    (instead of one call per 1000-row chunk) is what avoids creating
    thousands of parts on a large upload.
    """
    from superset.db_engine_specs import clickhouse as clickhouse_module

    mocker.patch.object(clickhouse_module, "UPLOAD_INSERT_BATCH_SIZE", 2)
    database = Mock()
    database.get_extra.return_value = {}
    df = pd.DataFrame({"id": range(5), "name": list("abcde")})

    upload_mocks.spec.df_to_sql(
        database, Table("t"), df, {"if_exists": "fail", "index": False}
    )

    assert upload_mocks.insert_df.call_count == 3
    sizes = [len(call.args[1]) for call in upload_mocks.insert_df.call_args_list]
    assert sizes == [2, 2, 1]
