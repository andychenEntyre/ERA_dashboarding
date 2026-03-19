import json
import re
import typing as t
from datetime import datetime, timezone

import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName


def _fetch_existing_transaction_ids(context: ExecutionContext) -> set[str]:
    query = """
        SELECT transaction_id
        FROM raw.stedi_837_submission_parsed
        WHERE transaction_id IS NOT NULL
    """

    try:
        existing_df = context.fetchdf(query)
    except Exception:
        return set()

    if existing_df.empty:
        return set()

    return {str(value) for value in existing_df["transaction_id"].dropna().tolist()}


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


def _deep_get(data: t.Any, *path_options: tuple[str, ...]) -> t.Any:
    for path in path_options:
        current = data
        found = True
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                found = False
                break
        if found and current not in (None, "", [], {}):
            return current
    return None


def _pick_entity(data: dict[str, t.Any], *keys: str) -> dict[str, t.Any]:
    for key in keys:
        value = data.get(key)
        if isinstance(value, dict) and value:
            return value
    return {}


def _normalize_diagnosis_codes(value: t.Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, str):
        codes = [code.strip() for code in re.split(r"[,|]", value) if code.strip()]
        return ", ".join(codes) if codes else None

    if isinstance(value, dict):
        collected: list[str] = []
        for item in value.values():
            normalized = _normalize_diagnosis_codes(item)
            if normalized:
                collected.extend([code.strip() for code in normalized.split(",") if code.strip()])
        deduped = list(dict.fromkeys(collected))
        return ", ".join(deduped) if deduped else None

    if isinstance(value, list):
        collected = []
        for item in value:
            if isinstance(item, dict):
                code = _first_non_empty(
                    item.get("code"),
                    item.get("diagnosisCode"),
                    item.get("id"),
                    item.get("value"),
                )
                if code:
                    collected.append(code)
            else:
                normalized = _normalize_diagnosis_codes(item)
                if normalized:
                    collected.extend([code.strip() for code in normalized.split(",") if code.strip()])
        deduped = list(dict.fromkeys(collected))
        return ", ".join(deduped) if deduped else None

    return None


def _to_float(value: t.Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _has_meaningful_values(values: dict[str, t.Any]) -> bool:
    return any(value not in (None, "", [], {}) for value in values.values())


def _merge_parsed_values(primary: dict[str, t.Any], fallback: dict[str, t.Any]) -> dict[str, t.Any]:
    merged: dict[str, t.Any] = dict(fallback)
    for key, value in primary.items():
        if value not in (None, "", [], {}):
            merged[key] = value
        elif key not in merged:
            merged[key] = value
    return merged


def _pick_entity_from_roots(roots: list[dict[str, t.Any]], *keys: str) -> dict[str, t.Any]:
    for root in roots:
        entity = _pick_entity(root, *keys)
        if entity:
            return entity
    return {}


def _json_roots(payload: dict[str, t.Any]) -> list[dict[str, t.Any]]:
    roots: list[dict[str, t.Any]] = [payload]
    for path in (
        ("event",),
        ("event", "detail"),
        ("event", "detail", "input"),
        ("detail",),
        ("input",),
        ("transaction",),
        ("data",),
        ("claim",),
        ("claimInformation",),
    ):
        candidate = _deep_get(payload, path)
        if isinstance(candidate, dict) and candidate not in roots:
            roots.append(candidate)
    return roots


def _looks_like_x12(value: str) -> bool:
    snippet = value.strip()[:64].upper()
    return bool("*" in value and "~" in value and (snippet.startswith("ISA") or snippet.startswith("GS") or snippet.startswith("ST")))


def _extract_embedded_x12(payload: dict[str, t.Any]) -> str | None:
    for path in (
        ("x12",),
        ("edi",),
        ("rawX12",),
        ("documentPayload",),
        ("document_payload",),
        ("event", "detail", "x12"),
        ("event", "detail", "input"),
        ("input",),
    ):
        candidate = _deep_get(payload, path)
        if isinstance(candidate, str) and _looks_like_x12(candidate):
            return candidate
    return None


def _parse_stedi_loop_payload(payload: dict[str, t.Any]) -> dict[str, t.Any]:
    detail = _deep_get(payload, ("detail",))
    if not isinstance(detail, dict):
        return {}

    billing_loops = detail.get("billing_provider_hierarchical_level_HL_loop")
    if not isinstance(billing_loops, list):
        return {}

    parsed: dict[str, t.Any] = {
        "transaction_set_creation_date_04": _first_non_empty(
            _deep_get(
                payload,
                ("heading", "beginning_of_hierarchical_transaction_BHT", "transaction_set_creation_date_04"),
            )
        ),
        "patient_control_number_01": None,
        "patient_first_name": None,
        "patient_last_name": None,
        "patient_gender": None,
        "patient_address_1": None,
        "patient_address_2": None,
        "patient_city": None,
        "patient_state": None,
        "patient_postal_code": None,
        "member_id": None,
        "plan_name": None,
        "diagnosis_codes": None,
        "total_charge_amount": None,
    }

    diagnosis_values: list[str] = []

    for billing_loop in billing_loops:
        if not isinstance(billing_loop, dict):
            continue

        subscriber_loops = billing_loop.get("subscriber_hierarchical_level_HL_loop")
        if not isinstance(subscriber_loops, list):
            continue

        for subscriber_loop in subscriber_loops:
            if not isinstance(subscriber_loop, dict):
                continue

            subscriber_nm1_loop = _pick_entity(subscriber_loop, "subscriber_name_NM1_loop")
            subscriber_nm1 = _pick_entity(subscriber_nm1_loop, "subscriber_name_NM1")
            subscriber_n3 = _pick_entity(subscriber_nm1_loop, "subscriber_address_N3")
            subscriber_n4 = _pick_entity(subscriber_nm1_loop, "subscriber_city_state_zip_code_N4")
            subscriber_dmg = _pick_entity(subscriber_nm1_loop, "subscriber_demographic_information_DMG")
            subscriber_sbr = _pick_entity(subscriber_loop, "subscriber_information_SBR")
            payer_nm1_loop = _pick_entity(subscriber_loop, "payer_name_NM1_loop")
            payer_nm1 = _pick_entity(payer_nm1_loop, "payer_name_NM1")

            if parsed["patient_first_name"] is None:
                parsed["patient_first_name"] = _first_non_empty(
                    subscriber_nm1.get("subscriber_first_name_04"),
                    subscriber_nm1.get("subscriber_first_name"),
                )
            if parsed["patient_last_name"] is None:
                parsed["patient_last_name"] = _first_non_empty(
                    subscriber_nm1.get("subscriber_last_name_03"),
                    subscriber_nm1.get("subscriber_last_name"),
                )
            if parsed["patient_gender"] is None:
                parsed["patient_gender"] = _first_non_empty(
                    subscriber_dmg.get("subscriber_gender_code_03"),
                    subscriber_dmg.get("gender"),
                )
            if parsed["patient_address_1"] is None:
                parsed["patient_address_1"] = _first_non_empty(
                    subscriber_n3.get("subscriber_address_line_01"),
                    subscriber_n3.get("subscriber_address_line_1_01"),
                )
            if parsed["patient_address_2"] is None:
                parsed["patient_address_2"] = _first_non_empty(
                    subscriber_n3.get("subscriber_address_line_02"),
                    subscriber_n3.get("subscriber_address_line_2_02"),
                )
            if parsed["patient_city"] is None:
                parsed["patient_city"] = _first_non_empty(subscriber_n4.get("subscriber_city_name_01"))
            if parsed["patient_state"] is None:
                parsed["patient_state"] = _first_non_empty(subscriber_n4.get("subscriber_state_code_02"))
            if parsed["patient_postal_code"] is None:
                parsed["patient_postal_code"] = _first_non_empty(
                    subscriber_n4.get("subscriber_postal_zone_or_zip_code_03")
                )
            if parsed["member_id"] is None:
                parsed["member_id"] = _first_non_empty(
                    subscriber_nm1.get("subscriber_identification_code_09"),
                    subscriber_sbr.get("subscriber_group_or_policy_number_03"),
                    subscriber_sbr.get("insured_group_or_policy_number_03"),
                )
            if parsed["plan_name"] is None:
                parsed["plan_name"] = _first_non_empty(
                    payer_nm1.get("payer_name_03"),
                    payer_nm1.get("payer_last_or_organization_name_03"),
                )

            claim_loops = subscriber_loop.get("claim_information_CLM_loop")
            if not isinstance(claim_loops, list):
                continue

            for claim_loop in claim_loops:
                if not isinstance(claim_loop, dict):
                    continue

                clm = _pick_entity(claim_loop, "claim_information_CLM")
                if parsed["patient_control_number_01"] is None:
                    parsed["patient_control_number_01"] = _first_non_empty(
                        clm.get("patient_control_number_01"),
                        clm.get("patient_control_number"),
                    )
                if parsed["total_charge_amount"] is None:
                    parsed["total_charge_amount"] = _to_float(
                        _first_non_empty(
                            clm.get("total_claim_charge_amount_02"),
                            clm.get("total_claim_charge_amount"),
                        )
                    )

                hi = _pick_entity(claim_loop, "health_care_diagnosis_code_HI")
                diagnosis_value = _normalize_diagnosis_codes(hi)
                if diagnosis_value:
                    diagnosis_values.extend([code.strip() for code in diagnosis_value.split(",") if code.strip()])

    deduped_diagnosis = list(dict.fromkeys(diagnosis_values))
    parsed["diagnosis_codes"] = ", ".join(deduped_diagnosis) if deduped_diagnosis else None
    return parsed


def _parse_json_payload(payload: dict[str, t.Any]) -> dict[str, t.Any]:
    roots = _json_roots(payload)
    claim = _pick_entity_from_roots(
        roots,
        "claimInformation",
        "claim",
        "claim_information",
        "billingClaim",
    )
    patient = _pick_entity_from_roots(
        roots,
        "patient",
        "patientInformation",
        "patientInfo",
        "member",
    )
    subscriber = _pick_entity_from_roots(
        roots,
        "subscriber",
        "subscriberInformation",
        "subscriberInfo",
        "insured",
    )
    payer = _pick_entity_from_roots(roots, "payer", "payerInformation", "insurance", "insurer")
    coverage = _pick_entity_from_roots(roots, "coverage", "plan", "benefits", "insurancePlan")

    if not patient:
        patient = _pick_entity(claim, "patient", "patientInformation", "patientInfo")
    if not subscriber:
        subscriber = _pick_entity(claim, "subscriber", "subscriberInformation", "subscriberInfo")
    if not payer:
        payer = _pick_entity(claim, "payer", "payerInformation", "insurance", "insurer")
    if not coverage:
        coverage = _pick_entity(claim, "coverage", "plan", "benefits", "insurancePlan")

    person = patient or subscriber
    address = _pick_entity(
        person,
        "address",
        "homeAddress",
        "patientAddress",
        "subscriberAddress",
    )

    diagnosis_codes = _normalize_diagnosis_codes(
        _deep_get(
            claim,
            ("healthCareCodeInformation", "diagnosisCodes"),
            ("healthCareCodeInformation", "principalDiagnosis"),
            ("diagnosisCodes",),
            ("diagnoses",),
        )
    )

    return {
        "transaction_set_creation_date_04": _first_non_empty(
            _deep_get(
                payload,
                ("heading", "beginning_of_hierarchical_transaction_BHT", "transaction_set_creation_date_04"),
            )
        ),
        "patient_control_number_01": _first_non_empty(
            claim.get("patientControlNumber"),
            payload.get("patientControlNumber"),
        ),
        "patient_first_name": _first_non_empty(
            person.get("firstName"),
            person.get("givenName"),
        ),
        "patient_last_name": _first_non_empty(
            person.get("lastName"),
            person.get("familyName"),
        ),
        "patient_gender": _first_non_empty(
            person.get("gender"),
            person.get("sex"),
        ),
        "patient_address_1": _first_non_empty(address.get("address1"), address.get("line1")),
        "patient_address_2": _first_non_empty(address.get("address2"), address.get("line2")),
        "patient_city": _first_non_empty(address.get("city")),
        "patient_state": _first_non_empty(address.get("state"), address.get("province")),
        "patient_postal_code": _first_non_empty(address.get("postalCode"), address.get("zipCode")),
        "member_id": _first_non_empty(
            subscriber.get("memberId"),
            subscriber.get("memberNumber"),
            subscriber.get("subscriberId"),
            subscriber.get("id"),
            coverage.get("memberId"),
        ),
        "plan_name": _first_non_empty(
            coverage.get("planName"),
            payer.get("name"),
            payer.get("organizationName"),
            coverage.get("name"),
        ),
        "diagnosis_codes": diagnosis_codes,
        "total_charge_amount": _to_float(
            _first_non_empty(
                claim.get("claimChargeAmount"),
                claim.get("totalChargeAmount"),
                payload.get("claimChargeAmount"),
            )
        ),
    }


def _segment_value(segments_by_name: dict[str, list[list[str]]], name: str, index: int) -> str | None:
    segments = segments_by_name.get(name, [])
    if not segments:
        return None
    segment = segments[0]
    if len(segment) > index:
        return _first_non_empty(segment[index])
    return None


def _find_nm1_entity(segments: list[list[str]], entity_code: str) -> list[str] | None:
    for segment in segments:
        if segment and segment[0] == "NM1" and len(segment) > 1 and segment[1] == entity_code:
            return segment
    return None


def _find_following_segment(segments: list[list[str]], start_segment: list[str] | None, target: str) -> list[str] | None:
    if not start_segment:
        return None

    start_index = segments.index(start_segment)
    for segment in segments[start_index + 1 :]:
        if not segment:
            continue
        if segment[0] == "HL":
            break
        if segment[0] == "NM1":
            break
        if segment[0] == target:
            return segment
    return None


def _extract_hi_codes(segments: list[list[str]]) -> str | None:
    codes: list[str] = []
    for segment in segments:
        if not segment or segment[0] != "HI":
            continue
        for element in segment[1:]:
            if not element:
                continue
            parts = element.split(":")
            if len(parts) >= 2 and parts[1].strip():
                codes.append(parts[1].strip())
    deduped = list(dict.fromkeys(codes))
    return ", ".join(deduped) if deduped else None


def _extract_member_id(segments: list[list[str]]) -> str | None:
    preferred_qualifiers = {"MI", "SY", "1W", "17", "Y4"}
    for segment in segments:
        if not segment or segment[0] != "REF" or len(segment) < 3:
            continue
        if segment[1] in preferred_qualifiers:
            return _first_non_empty(segment[2])
    return None


def _parse_x12_payload(payload_text: str) -> dict[str, t.Any]:
    segments = [
        [element.strip() for element in segment.split("*")]
        for segment in payload_text.replace("\n", "").split("~")
        if segment.strip()
    ]
    segments_by_name: dict[str, list[list[str]]] = {}
    for segment in segments:
        segments_by_name.setdefault(segment[0], []).append(segment)

    claim_segment = segments_by_name.get("CLM", [[None]])[0]
    patient_nm1 = _find_nm1_entity(segments, "QC") or _find_nm1_entity(segments, "IL")
    payer_nm1 = _find_nm1_entity(segments, "PR")
    address_n3 = _find_following_segment(segments, patient_nm1, "N3")
    address_n4 = _find_following_segment(segments, patient_nm1, "N4")
    dmg = _find_following_segment(segments, patient_nm1, "DMG")

    return {
        "patient_control_number_01": _first_non_empty(claim_segment[1] if len(claim_segment) > 1 else None),
        "patient_first_name": _first_non_empty(patient_nm1[4] if patient_nm1 and len(patient_nm1) > 4 else None),
        "patient_last_name": _first_non_empty(patient_nm1[3] if patient_nm1 and len(patient_nm1) > 3 else None),
        "patient_gender": _first_non_empty(dmg[3] if dmg and len(dmg) > 3 else None),
        "patient_address_1": _first_non_empty(address_n3[1] if address_n3 and len(address_n3) > 1 else None),
        "patient_address_2": _first_non_empty(address_n3[2] if address_n3 and len(address_n3) > 2 else None),
        "patient_city": _first_non_empty(address_n4[1] if address_n4 and len(address_n4) > 1 else None),
        "patient_state": _first_non_empty(address_n4[2] if address_n4 and len(address_n4) > 2 else None),
        "patient_postal_code": _first_non_empty(address_n4[3] if address_n4 and len(address_n4) > 3 else None),
        "member_id": _extract_member_id(segments),
        "plan_name": _first_non_empty(payer_nm1[3] if payer_nm1 and len(payer_nm1) > 3 else None),
        "diagnosis_codes": _extract_hi_codes(segments),
        "total_charge_amount": _to_float(
            _first_non_empty(claim_segment[2] if len(claim_segment) > 2 else None)
        ),
    }


def _parse_payload(document_format: str | None, payload_text: str) -> tuple[dict[str, t.Any], str | None]:
    if document_format != "x12":
        try:
            payload = json.loads(payload_text)
            parsed_json: dict[str, t.Any] = {}
            parsed_loop_json: dict[str, t.Any] = {}
            if isinstance(payload, dict):
                parsed_json = _parse_json_payload(payload)
                parsed_loop_json = _parse_stedi_loop_payload(payload)

            combined_json = _merge_parsed_values(parsed_loop_json, parsed_json)
            if _has_meaningful_values(combined_json):
                return combined_json, None

            embedded_x12 = _extract_embedded_x12(payload) if isinstance(payload, dict) else None
            if embedded_x12:
                try:
                    return _parse_x12_payload(embedded_x12), "json_unmapped_fell_back_to_x12"
                except Exception as x12_ex:
                    return combined_json, f"json_unmapped; x12_fallback_failed: {x12_ex}"

            if _looks_like_x12(payload_text):
                try:
                    return _parse_x12_payload(payload_text), "json_unmapped_fell_back_to_x12"
                except Exception as x12_ex:
                    return combined_json, f"json_unmapped; x12_fallback_failed: {x12_ex}"

            return combined_json, "json_unmapped_payload_shape"
        except Exception as ex:
            json_error = str(ex)
            try:
                return _parse_x12_payload(payload_text), f"json_parse_failed: {json_error}"
            except Exception as x12_ex:
                return {}, f"json_parse_failed: {json_error}; x12_parse_failed: {x12_ex}"

    try:
        return _parse_x12_payload(payload_text), None
    except Exception as ex:
        return {}, str(ex)


@model(
    "raw.stedi_837_submission_parsed",
    kind={
        "name": ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        "unique_key": ["transaction_id"],
    },
    columns={
        "transaction_id": "text",
        "transaction_set_creation_date_04": "text",
        "patient_control_number_01": "text",
        "patient_first_name": "text",
        "patient_last_name": "text",
        "patient_gender": "text",
        "patient_address_1": "text",
        "patient_address_2": "text",
        "patient_city": "text",
        "patient_state": "text",
        "patient_postal_code": "text",
        "member_id": "text",
        "plan_name": "text",
        "diagnosis_codes": "text",
        "total_charge_amount": "double precision",
        "source_format": "text",
        "parse_error": "text",
        "parsed_at": "timestamp",
    },
    depends_on=["raw.stedi_claim_submissions_payloads"],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> t.Iterator[pd.DataFrame]:
    upstream_table = context.resolve_table("raw.stedi_claim_submissions_payloads")
    payload_df = context.fetchdf(
        f"""
        SELECT transaction_id, document_format, document_payload, error_message
        FROM {upstream_table}
        WHERE transaction_id IS NOT NULL
        """
    )

    if payload_df.empty:
        yield from ()
        return

    existing_transaction_ids = _fetch_existing_transaction_ids(context)
    rows: list[dict[str, t.Any]] = []

    for row in payload_df.to_dict("records"):
        transaction_id = str(row["transaction_id"])
        if transaction_id in existing_transaction_ids:
            continue

        parse_error = _first_non_empty(row.get("error_message"))
        parsed_values: dict[str, t.Any] = {}
        payload_text = row.get("document_payload")

        if payload_text:
            parsed_values, parser_error = _parse_payload(row.get("document_format"), str(payload_text))
            parse_error = _first_non_empty(parse_error, parser_error)
            if not parse_error and not _has_meaningful_values(parsed_values):
                parse_error = "unmapped_payload_shape"

        rows.append(
            {
                "transaction_id": transaction_id,
                "transaction_set_creation_date_04": parsed_values.get("transaction_set_creation_date_04"),
                "patient_control_number_01": parsed_values.get("patient_control_number_01"),
                "patient_first_name": parsed_values.get("patient_first_name"),
                "patient_last_name": parsed_values.get("patient_last_name"),
                "patient_gender": parsed_values.get("patient_gender"),
                "patient_address_1": parsed_values.get("patient_address_1"),
                "patient_address_2": parsed_values.get("patient_address_2"),
                "patient_city": parsed_values.get("patient_city"),
                "patient_state": parsed_values.get("patient_state"),
                "patient_postal_code": parsed_values.get("patient_postal_code"),
                "member_id": parsed_values.get("member_id"),
                "plan_name": parsed_values.get("plan_name"),
                "diagnosis_codes": parsed_values.get("diagnosis_codes"),
                "total_charge_amount": parsed_values.get("total_charge_amount"),
                "source_format": row.get("document_format") or "unknown",
                "parse_error": parse_error,
                "parsed_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        )

    if not rows:
        yield from ()
        return

    yield pd.DataFrame(rows)
