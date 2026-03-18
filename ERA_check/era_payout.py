import json
import importlib
import os
import re
from pathlib import Path

import pandas as pd
import requests


def extract_results(data, transactionId):
    results = []
    check_issue_or_eft_effective_date_16 = ""
    check_or_eft_trace_number_02 = ""

    if "heading" in data:
        check_issue_or_eft_effective_date_16 = data.get("heading", {}).get("financial_information_BPR", {}).get("check_issue_or_eft_effective_date_16", "")
        check_or_eft_trace_number_02 = data.get("heading", {}).get("reassociation_trace_number_TRN", {}).get("check_or_eft_trace_number_02", "")

    if "transactions" in data:
        for transaction in data.get("transactions", []):
            check_issue_or_eft_effective_date_16 = transaction.get("financialInformation", {}).get("checkIssueOrEFTEffectiveDate", "")
            check_or_eft_trace_number_02 = transaction.get("paymentAndRemitReassociationDetails", {}).get("checkOrEFTTraceNumber", "")
            for detail in transaction.get("detailInfo", []):
                for payment in detail.get("paymentInfo", []):
                    claim_payment = payment.get("claimPaymentInfo", {})
                    patient = payment.get("patientName", {})
                    results.append({
                        "transactionId": transactionId,
                        "patient_first_name_04": patient.get("firstName", ""),
                        "patient_last_name_03": patient.get("lastName", ""),
                        "check_issue_or_eft_effective_date_16": check_issue_or_eft_effective_date_16,
                        "check_or_eft_trace_number_02": check_or_eft_trace_number_02,
                        "patient_control_number_01": claim_payment.get("patientControlNumber", ""),
                        "payer_claim_control_number_07": claim_payment.get("payerClaimControlNumber", ""),
                        "total_claim_charge_amount_03": claim_payment.get("totalClaimChargeAmount", ""),
                        "claim_payment_amount_04": claim_payment.get("claimPaymentAmount", ""),
                    })
        return results

    for lx in data.get("detail", {}).get("header_number_LX_loop", []):
        for claim in lx.get("claim_payment_information_CLP_loop", []):
            clp = claim.get("claim_payment_information_CLP", {})
            patient = claim.get("patient_name_NM1", {})
            results.append({
                "transactionId": transactionId,
                "patient_first_name_04": patient.get("patient_first_name_04", ""),
                "patient_last_name_03": patient.get("patient_last_name_03", ""),
                "check_issue_or_eft_effective_date_16": check_issue_or_eft_effective_date_16,
                "check_or_eft_trace_number_02": check_or_eft_trace_number_02,
                "patient_control_number_01": clp.get("patient_control_number_01", ""),
                "payer_claim_control_number_07": clp.get("payer_claim_control_number_07", ""),
                "total_claim_charge_amount_03": clp.get("total_claim_charge_amount_03", ""),
                "claim_payment_amount_04": clp.get("claim_payment_amount_04", ""),
            })

    return results


def _get_env(name, fallback_names=(), default=None, required=False):
    for key in (name, *fallback_names):
        value = os.getenv(key)
        if value not in (None, ""):
            return value

    if required:
        searched = ", ".join((name, *fallback_names))
        raise ValueError(f"Missing required environment variable. Expected one of: {searched}")

    return default


def _load_dotenv():
    env_candidates = [
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent.parent / ".env",
    ]

    for env_path in env_candidates:
        if not env_path.exists():
            continue

        for raw_line in env_path.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
        return env_path

    return None


def _column_types():
    exp = importlib.import_module("sqlglot").exp

    return {
        "transactionId": exp.DataType.build("text"),
        "check_issue_or_eft_effective_date_16": exp.DataType.build("text"),
        "check_or_eft_trace_number_02": exp.DataType.build("text"),
        "patient_control_number_01": exp.DataType.build("text"),
        "payer_claim_control_number_07": exp.DataType.build("text"),
        "patient_first_name_04": exp.DataType.build("text"),
        "patient_last_name_03": exp.DataType.build("text"),
        "total_claim_charge_amount_03": exp.DataType.build("double precision"),
        "claim_payment_amount_04": exp.DataType.build("double precision"),
    }


dotenv_path = _load_dotenv()


def write_results_to_postgres(results_df):
    PostgresConnectionConfig = importlib.import_module(
        "sqlmesh.core.config.connection"
    ).PostgresConnectionConfig

    schema_name = _get_env("SQLMESH_DEV_SCHEMA", default="raw")
    table_name = _get_env("SQLMESH_DEV_TABLE", default="era_payout_results")
    write_mode = _get_env("SQLMESH_DEV_WRITE_MODE", default="replace").lower()

    config = PostgresConnectionConfig(
        host=_get_env("SQLMESH_DEV_HOST", required=True),
        user=_get_env("SQLMESH_DEV_USER", required=True),
        password=_get_env("SQLMESH_DEV_PASSWORD", required=True),
        port=int(_get_env("SQLMESH_DEV_PORT", default="5432")),
        database=_get_env("SQLMESH_DEV_DATABASE", required=True),
        sslmode=_get_env("SQLMESH_DEV_SSLMODE", default=None),
    )
    database_name = _get_env("SQLMESH_DEV_DATABASE", required=True)

    adapter = config.create_engine_adapter()
    full_table_name = f"{schema_name}.{table_name}"
    column_types = _column_types()
    print(f"Writing to {database_name}.{full_table_name} (mode={write_mode})")

    numeric_columns = [
        "total_claim_charge_amount_03",
        "claim_payment_amount_04",
    ]
    for column in numeric_columns:
        results_df[column] = pd.to_numeric(results_df[column], errors="coerce")

    adapter.create_schema(schema_name)
    if write_mode == "append":
        adapter.create_table(full_table_name, column_types)
        adapter.insert_append(full_table_name, results_df, target_columns_to_types=column_types)
    elif write_mode == "replace":
        adapter.replace_query(full_table_name, results_df, target_columns_to_types=column_types)
    else:
        raise ValueError("SQLMESH_DEV_WRITE_MODE must be 'replace' or 'append'")

    return full_table_name

# df = pd.read_csv("/Users/Andy.Chen/Billing_Automation/ERA_check/04March2026_stedi.csv")
df = pd.read_csv("/Users/Andy.Chen/ERA_dashboarding/ERA_check/webhook_ERA_finder.csv")

list_transactionIds = []
for i, row in df.iterrows():
    s = row["data"]

    if isinstance(s, float) and pd.isna(s):
        continue

    # If the CSV stored it with surrounding quotes, trim whitespace
    s = str(s).strip()

    # Normalize python-ish tokens to JSON-ish
    s = re.sub(r"\bNone\b", "null", s)
    s = re.sub(r"\bTrue\b", "true", s)
    s = re.sub(r"\bFalse\b", "false", s)
    s = re.sub(r"\bNaN\b|\bnan\b", "null", s)

    # Convert single quotes to double quotes (works if there are no embedded apostrophes in values)
    s = s.replace("'", '"')

    try:
        data = json.loads(s)
        transactionId = data["event"]["detail"]["transactionId"]
        list_transactionIds.append(transactionId)
        print(transactionId)
    except Exception as e:
        print(f"Row {i} failed parse: {e}")
        # print(s)  # uncomment to inspect

print("✅", len(list_transactionIds))

all_results = []
authorization_token = _get_env("STEDI_API_KEY", required=True)

for transactionId in list_transactionIds:
    url = f"https://healthcare.us.stedi.com/2024-04-01/change/medicalnetwork/reports/v2/{transactionId}/835"
    response = requests.request("GET", url, headers = {
    "Authorization": authorization_token
    })
    response.raise_for_status()
    data = json.loads(response.text)

    print("🔴starting transactionId:", transactionId)
    results = extract_results(data, transactionId)

    all_results.extend(results)

    for result in results:
        print(result)
    print("✅ Finished transactionId:", transactionId, "\n")

column_order = [
    "transactionId",
    "check_issue_or_eft_effective_date_16",
    "check_or_eft_trace_number_02",
    "patient_control_number_01",
    "payer_claim_control_number_07",
    "patient_first_name_04",
    "patient_last_name_03",
    "total_claim_charge_amount_03",
    "claim_payment_amount_04",
]
results_df = pd.DataFrame(all_results).reindex(columns=column_order)
table_name = write_results_to_postgres(results_df)
print("✅ Wrote:", table_name, "rows:", len(all_results))
