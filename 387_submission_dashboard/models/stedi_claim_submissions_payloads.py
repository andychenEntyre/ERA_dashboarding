import json
import os
import typing as t
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName


def _load_dotenv() -> Path | None:
    env_candidates = [
        Path(__file__).resolve().parent.parent / ".env",
        Path(__file__).resolve().parents[2] / ".env",
    ]

    for env_path in env_candidates:
        if not env_path.exists():
            continue

        for raw_line in env_path.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

        return env_path

    return None


def _fetch_existing_transaction_ids(context: ExecutionContext) -> set[str]:
    query = """
        SELECT transaction_id
        FROM raw.stedi_claim_submissions_payloads
        WHERE transaction_id IS NOT NULL
    """

    try:
        existing_df = context.fetchdf(query)
    except Exception:
        return set()

    if existing_df.empty:
        return set()

    return {str(value) for value in existing_df["transaction_id"].dropna().tolist()}


def _document_format(payload_text: str) -> str:
    try:
        json.loads(payload_text)
        return "json"
    except json.JSONDecodeError:
        return "x12"


_load_dotenv()


@model(
    "raw.stedi_claim_submissions_payloads",
    kind={
        "name": ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        "unique_key": ["transaction_id"],
    },
    columns={
        "transaction_id": "text",
        "document_format": "text",
        "document_payload": "text",
        "fetched_at": "timestamp",
        "error_message": "text",
    },
    depends_on=["raw.stedi_837_submissions"],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> t.Iterator[pd.DataFrame]:
    api_key = os.getenv("STEDI_API_KEY")
    if not api_key:
        raise ValueError("Missing required environment variable: STEDI_API_KEY")

    upstream_table = context.resolve_table("raw.stedi_837_submissions")
    transaction_df = context.fetchdf(
        f"""
        SELECT DISTINCT transaction_id
        FROM {upstream_table}
        WHERE transaction_id IS NOT NULL
        """
    )

    if transaction_df.empty:
        yield from ()
        return

    existing_transaction_ids = _fetch_existing_transaction_ids(context)
    pending_transaction_ids = [
        str(transaction_id)
        for transaction_id in transaction_df["transaction_id"].tolist()
        if str(transaction_id) not in existing_transaction_ids
    ]

    if not pending_transaction_ids:
        yield from ()
        return

    headers = {"Authorization": api_key}
    rows = []

    for transaction_id in pending_transaction_ids:
        url = f"https://core.us.stedi.com/2023-08-01/transactions/{transaction_id}/input"

        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            payload_text = response.text.strip()

            rows.append(
                {
                    "transaction_id": transaction_id,
                    "document_format": _document_format(payload_text),
                    "document_payload": payload_text,
                    "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    "error_message": None,
                }
            )
        except Exception as ex:
            rows.append(
                {
                    "transaction_id": transaction_id,
                    "document_format": None,
                    "document_payload": None,
                    "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    "error_message": str(ex),
                }
            )

    if not rows:
        yield from ()
        return

    yield pd.DataFrame(rows)
