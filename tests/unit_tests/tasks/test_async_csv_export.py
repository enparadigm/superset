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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from types import SimpleNamespace
from unittest import mock

from superset.tasks.async_csv_export import _write_query_to_csv


def test_write_query_to_csv_streams_rows_and_uses_verbose_headers(
    app_context, tmp_path
):
    result = mock.MagicMock()
    result.keys.return_value = ["user_id", "total"]
    result.fetchmany.side_effect = [[(1, 10), (2, None)], []]

    connection = mock.MagicMock()
    connection.execution_options.return_value.execute.return_value = result
    engine = mock.MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    database = mock.MagicMock()
    database.get_sqla_engine.return_value.__enter__.return_value = engine

    query_object = mock.MagicMock()
    query_object.to_dict.return_value = {"columns": ["user_id", "total"]}
    datasource = SimpleNamespace(
        database=database,
        data={"verbose_map": {"user_id": "User ID"}},
        get_query_str_extended=mock.MagicMock(
            return_value=SimpleNamespace(
                prequeries=["SET max_threads = 2"],
                sql="SELECT user_id, total FROM users",
            )
        ),
    )
    query_context = SimpleNamespace(datasource=datasource, queries=[query_object])
    output_path = tmp_path / "export.csv"

    with (
        mock.patch("superset.tasks.async_csv_export.ChartDataCommand") as command_class,
        mock.patch(
            "superset.tasks.async_csv_export.db.session.merge", return_value=database
        ),
    ):
        _write_query_to_csv(query_context, str(output_path))

    command_class.return_value.validate.assert_called_once_with()
    connection.execute.assert_any_call(mock.ANY)
    connection.execution_options.return_value.execute.assert_called_once_with(mock.ANY)
    assert output_path.read_text() == "User ID,total\n1,10\n2,\n"
    result.fetchmany.assert_called_with(1000)
