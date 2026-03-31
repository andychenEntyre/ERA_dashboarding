import hashlib
import json
import typing as t
from datetime import datetime, timezone

import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName


def _first_non_empty(*values: t.Any) -> str | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
            if value:
                return value
            continue
        if isinstance(value, (int, float)):
            return str(value)
    return None


def _to_float(value: t.Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _as_dict(value: t.Any) -> dict[str, t.Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: t.Any) -> list[t.Any]:
    return value if isinstance(value, list) else []


def _claim_row_id(
    transaction_id: str,
    patient_control_number_01: str | None,
    payer_claim_control_number_07: str | None,
    row_index: int,
) -> str:
    key = "|".join(
        [
            transaction_id,
            patient_control_number_01 or "",
            payer_claim_control_number_07 or "",
            str(row_index),
        ]
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _error_row(transaction_id: str, error_message: str) -> dict[str, t.Any]:
    return {
        "claim_row_id": hashlib.sha256(f"{transaction_id}|error".encode("utf-8")).hexdigest(),
        "transaction_id": transaction_id,
        "check_issue_or_eft_effective_date_16": None,
        "check_or_eft_trace_number_02": None,
        "patient_control_number_01": None,
        "payer_claim_control_number_07": None,
        "patient_first_name_04": None,
        "patient_last_name_03": None,
        "total_claim_charge_amount_03": None,
        "claim_payment_amount_04": None,
        "parse_error": error_message,
        "parsed_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }


def _extract_claim_rows(payload: dict[str, t.Any], transaction_id: str) -> list[dict[str, t.Any]]:
    rows: list[dict[str, t.Any]] = []

    heading = _as_dict(payload.get("heading"))
    heading_bpr = _as_dict(heading.get("financial_information_BPR"))
    heading_trn = _as_dict(heading.get("reassociation_trace_number_TRN"))

    default_effective_date = _first_non_empty(heading_bpr.get("check_issue_or_eft_effective_date_16"))
    default_trace_number = _first_non_empty(heading_trn.get("check_or_eft_trace_number_02"))

    transactions = _as_list(payload.get("transactions"))
    if transactions:
        for transaction in transactions:
            transaction_obj = _as_dict(transaction)
            financial_info = _as_dict(transaction_obj.get("financialInformation"))
            reassociation = _as_dict(transaction_obj.get("paymentAndRemitReassociationDetails"))

            check_issue_or_eft_effective_date_16 = _first_non_empty(
                financial_info.get("checkIssueOrEFTEffectiveDate"),
                default_effective_date,
            )
            check_or_eft_trace_number_02 = _first_non_empty(
                reassociation.get("checkOrEFTTraceNumber"),
                default_trace_number,
            )

            for detail in _as_list(transaction_obj.get("detailInfo")):
                detail_obj = _as_dict(detail)
                for payment in _as_list(detail_obj.get("paymentInfo")):
                    payment_obj = _as_dict(payment)
                    claim_payment = _as_dict(payment_obj.get("claimPaymentInfo"))
                    patient = _as_dict(payment_obj.get("patientName"))

                    patient_control_number_01 = _first_non_empty(
                        claim_payment.get("patientControlNumber"),
                        claim_payment.get("patient_control_number_01"),
                    )
                    payer_claim_control_number_07 = _first_non_empty(
                        claim_payment.get("payerClaimControlNumber"),
                        claim_payment.get("payer_claim_control_number_07"),
                    )
                    patient_first_name_04 = _first_non_empty(
                        patient.get("firstName"),
                        patient.get("patient_first_name_04"),
                    )
                    patient_last_name_03 = _first_non_empty(
                        patient.get("lastName"),
                        patient.get("patient_last_name_03"),
                    )

                    row_index = len(rows)
                    rows.append(
                        {
                            "claim_row_id": _claim_row_id(
                                transaction_id,
                                patient_control_number_01,
                                payer_claim_control_number_07,
                                row_index,
                            ),
                            "transaction_id": transaction_id,
                            "check_issue_or_eft_effective_date_16": check_issue_or_eft_effective_date_16,
                            "check_or_eft_trace_number_02": check_or_eft_trace_number_02,
                            "patient_control_number_01": patient_control_number_01,
                            "payer_claim_control_number_07": payer_claim_control_number_07,
                            "patient_first_name_04": patient_first_name_04,
                            "patient_last_name_03": patient_last_name_03,
                            "total_claim_charge_amount_03": _to_float(
                                _first_non_empty(
                                    claim_payment.get("totalClaimChargeAmount"),
                                    claim_payment.get("total_claim_charge_amount_03"),
                                )
                            ),
                            "claim_payment_amount_04": _to_float(
                                _first_non_empty(
                                    claim_payment.get("claimPaymentAmount"),
                                    claim_payment.get("claim_payment_amount_04"),
                                )
                            ),
                            "parse_error": None,
                            "parsed_at": datetime.now(timezone.utc).replace(tzinfo=None),
                        }
                    )
        return rows

    detail = _as_dict(payload.get("detail"))
    for lx in _as_list(detail.get("header_number_LX_loop")):
        lx_obj = _as_dict(lx)
        for claim in _as_list(lx_obj.get("claim_payment_information_CLP_loop")):
            claim_obj = _as_dict(claim)
            clp = _as_dict(claim_obj.get("claim_payment_information_CLP"))
            patient = _as_dict(claim_obj.get("patient_name_NM1"))

            patient_control_number_01 = _first_non_empty(clp.get("patient_control_number_01"))
            payer_claim_control_number_07 = _first_non_empty(clp.get("payer_claim_control_number_07"))
            patient_first_name_04 = _first_non_empty(patient.get("patient_first_name_04"))
            patient_last_name_03 = _first_non_empty(patient.get("patient_last_name_03"))

            row_index = len(rows)
            rows.append(
                {
                    "claim_row_id": _claim_row_id(
                        transaction_id,
                        patient_control_number_01,
                        payer_claim_control_number_07,
                        row_index,
                    ),
                    "transaction_id": transaction_id,
                    "check_issue_or_eft_effective_date_16": default_effective_date,
                    "check_or_eft_trace_number_02": default_trace_number,
                    "patient_control_number_01": patient_control_number_01,
                    "payer_claim_control_number_07": payer_claim_control_number_07,
                    "patient_first_name_04": patient_first_name_04,
                    "patient_last_name_03": patient_last_name_03,
                    "total_claim_charge_amount_03": _to_float(_first_non_empty(clp.get("total_claim_charge_amount_03"))),
                    "claim_payment_amount_04": _to_float(_first_non_empty(clp.get("claim_payment_amount_04"))),
                    "parse_error": None,
                    "parsed_at": datetime.now(timezone.utc).replace(tzinfo=None),
                }
            )

    return rows


@model(
    "raw.stedi_835_submission_parsed",
    kind={
        "name": ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        "unique_key": ["claim_row_id"],
    },
    columns={
        "claim_row_id": "text",
        "transaction_id": "text",
        "check_issue_or_eft_effective_date_16": "text",
        "check_or_eft_trace_number_02": "text",
        "patient_control_number_01": "text",
        "payer_claim_control_number_07": "text",
        "patient_first_name_04": "text",
        "patient_last_name_03": "text",
        "total_claim_charge_amount_03": "double precision",
        "claim_payment_amount_04": "double precision",
        "parse_error": "text",
        "parsed_at": "timestamp",
    },
    depends_on=["raw.stedi_835_submissions_payloads"],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> t.Iterator[pd.DataFrame]:
    upstream_table = context.resolve_table("raw.stedi_835_submissions_payloads")
    payload_df = context.fetchdf(
        f"""
        SELECT transaction_id, document_payload, error_message
        FROM {upstream_table}
        WHERE transaction_id IS NOT NULL
        """
    )

    if payload_df.empty:
        yield from ()
        return

    rows: list[dict[str, t.Any]] = []

    for row in payload_df.to_dict("records"):
        transaction_id = str(row["transaction_id"])
        upstream_error = _first_non_empty(row.get("error_message"))

        if upstream_error:
            rows.append(_error_row(transaction_id, upstream_error))
            continue

        payload_text = row.get("document_payload")
        if payload_text in (None, ""):
            rows.append(_error_row(transaction_id, "missing_document_payload"))
            continue

        try:
            payload = json.loads(str(payload_text))
        except Exception as ex:
            rows.append(_error_row(transaction_id, f"json_parse_failed: {ex}"))
            continue

        claim_rows = _extract_claim_rows(payload, transaction_id)
        if not claim_rows:
            rows.append(_error_row(transaction_id, "no_claim_rows_found"))
            continue

        rows.extend(claim_rows)

    if not rows:
        yield from ()
        return

    yield pd.DataFrame(rows)
