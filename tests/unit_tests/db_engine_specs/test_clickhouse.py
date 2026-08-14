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
    live ClickHouse: ``get_engine``/``inspect`` are stubbed and the SQLAlchemy
    ``Table`` and ``DataFrame.to_sql`` calls are captured instead of executed.
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

    return SimpleNamespace(
        spec=ClickHouseConnectEngineSpec,
        engine=engine,
        inspector=inspector,
        table_factory=mocker.patch("sqlalchemy.Table"),
        # autospec so the receiving DataFrame is recorded as the first arg,
        # which is how the NaN -> None conversion gets asserted.
        to_sql=mocker.patch.object(pd.DataFrame, "to_sql", autospec=True),
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

    # Rows are appended, not re-created, and the index is dropped.
    kwargs = upload_mocks.to_sql.call_args.kwargs
    assert kwargs["name"] == "t"
    assert kwargs["if_exists"] == "append"
    assert kwargs["index"] is False


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


def test_connect_df_to_sql_nulls_become_none(
    upload_mocks: Any, upload_df: pd.DataFrame
) -> None:
    """
    NaN/NaT must reach the driver as None, or ClickHouse rejects the insert.
    """
    database = Mock()
    database.get_extra.return_value = {}

    upload_mocks.spec.df_to_sql(
        database, Table("t"), upload_df, {"if_exists": "fail", "index": False}
    )

    inserted = upload_mocks.to_sql.call_args.args[0]
    assert inserted["name"].tolist() == ["alpha", None]


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

    upload_mocks.to_sql.assert_not_called()


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
    assert upload_mocks.to_sql.call_args.kwargs["if_exists"] == "append"


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
    assert upload_mocks.to_sql.call_args.kwargs["if_exists"] == "append"


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

    kwargs = upload_mocks.to_sql.call_args.kwargs
    assert kwargs["index"] is False
    assert "index_label" not in kwargs
