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
"""Asynchronous, object-storage-backed CSV exports for chart data."""

from __future__ import annotations

import csv
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any

from celery.exceptions import SoftTimeLimitExceeded
from flask import current_app, g
from sqlalchemy import text

from superset.charts.schemas import ChartDataQueryContextSchema
from superset.commands.chart.data.get_data_command import ChartDataCommand
from superset.extensions import celery_app, db, security_manager
from superset.utils.core import override_user, send_email_smtp

logger = logging.getLogger(__name__)


def _send_email(
    recipient: str,
    filename: str,
    download_url: str | None,
    expires_at: datetime | None = None,
) -> None:
    if download_url:
        expiry = expires_at.strftime("%Y-%m-%d %H:%M UTC") if expires_at else ""
        subject = f"Your CSV export is ready: {filename}"
        body = (
            f"<p>Your CSV export <strong>{escape(filename)}</strong> is ready.</p>"
            f'<p><a href="{escape(download_url)}">Download CSV</a></p>'
            f"<p>This link expires at {escape(expiry)}.</p>"
        )
    else:
        subject = f"CSV export failed: {filename}"
        body = (
            f"<p>The CSV export <strong>{escape(filename)}</strong> failed. "
            "Please retry or contact your administrator.</p>"
        )
    send_email_smtp(
        recipient,
        subject,
        body,
        current_app.config,
        dryrun=current_app.config.get("ALERT_REPORTS_NOTIFICATION_DRY_RUN", False),
    )


def _write_query_to_csv(query_context: Any, path: str) -> None:
    """Execute the chart's primary query and stream rows to a local CSV file."""
    command = ChartDataCommand(query_context)
    command.validate()

    datasource = query_context.datasource
    query_object = query_context.queries[0]
    query = datasource.get_query_str_extended(query_object.to_dict())
    database = db.session.merge(datasource.database)
    encoding = current_app.config.get("CSV_EXPORT", {}).get("encoding", "utf-8")

    with (
        database.get_sqla_engine() as engine,
        engine.connect() as connection,
        open(path, "w", encoding=encoding, newline="") as output,
    ):
        for prequery in query.prequeries:
            connection.execute(text(prequery))
        result = connection.execution_options(stream_results=True).execute(
            text(query.sql)
        )
        columns = list(result.keys())
        verbose_map = getattr(datasource, "data", {}).get("verbose_map", {})
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        writer.writerow([verbose_map.get(column, column) for column in columns])
        while rows := result.fetchmany(1000):
            writer.writerows(rows)


@celery_app.task(name="export_chart_csv", bind=True, max_retries=0)
def export_chart_csv(
    self: Any,  # pylint: disable=unused-argument
    query_context_payload: dict[str, Any],
    user_id: int,
    user_email: str,
    filename: str,
    job_id: str,
) -> None:
    """Create a CSV under the requesting user's permissions and email its URL."""
    temporary_path: str | None = None
    user = security_manager.get_user_by_id(user_id)
    try:
        if user is None:
            raise ValueError(f"User {user_id} not found")

        with override_user(user, force=False):
            g.form_data = query_context_payload
            query_context = ChartDataQueryContextSchema().load(query_context_payload)
            descriptor, temporary_path = tempfile.mkstemp(
                prefix=f"chart-export-{job_id}-", suffix=".csv"
            )
            os.close(descriptor)
            _write_query_to_csv(query_context, temporary_path)

            # Import lazily so Superset can still start with the feature disabled
            # when the optional dependency is not installed.
            import boto3  # pylint: disable=import-outside-toplevel,import-error

            bucket = current_app.config["ASYNC_CSV_EXPORT_S3_BUCKET"]
            prefix = current_app.config["ASYNC_CSV_EXPORT_S3_KEY_PREFIX"].strip("/")
            key = (
                f"{prefix}/{user_id}/{job_id}/{filename}"
                if prefix
                else (f"{user_id}/{job_id}/{filename}")
            )
            client = boto3.client(
                "s3", **current_app.config["ASYNC_CSV_EXPORT_S3_CLIENT_KWARGS"]
            )
            client.upload_file(
                temporary_path,
                bucket,
                key,
                ExtraArgs={
                    "ContentType": "text/csv",
                    "ContentDisposition": f'attachment; filename="{filename}"',
                },
            )
            ttl = current_app.config["ASYNC_CSV_EXPORT_LINK_TTL_SECONDS"]
            download_url = client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=ttl,
            )
            expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=ttl)
            _send_email(user_email, filename, download_url, expires_at)
    except SoftTimeLimitExceeded:
        logger.warning("Asynchronous CSV export %s timed out", job_id)
        _send_email(user_email, filename, None)
        raise
    except Exception:
        logger.exception("Asynchronous CSV export %s failed", job_id)
        try:
            _send_email(user_email, filename, None)
        except Exception:  # pylint: disable=broad-except
            logger.exception("Failed to send CSV export failure email")
        raise
    finally:
        if temporary_path and os.path.exists(temporary_path):
            os.remove(temporary_path)
