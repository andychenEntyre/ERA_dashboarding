import pandas as pd
import json
import re
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

# df = pd.read_csv("/Users/Andy.Chen/Billing_Automation/ERA_check/04March2026_stedi.csv")
df = pd.read_csv("/Users/Andy.Chen/Billing_Automation/ERA_check/webhook_ERA_finder.csv")

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

for transactionId in list_transactionIds:
    url = f"https://healthcare.us.stedi.com/2024-04-01/change/medicalnetwork/reports/v2/{transactionId}/835"
    response = requests.request("GET", url, headers = {
    "Authorization": "RYnvhqL.0X6jgBc6ewt5N7v2ILnQtiGy"
    })
    response.raise_for_status()
    data = json.loads(response.text)

    print("🔴starting transactionId:", transactionId)
    results = extract_results(data, transactionId)

    all_results.extend(results)

    for result in results:
        print(result)
    print("✅ Finished transactionId:", transactionId, "\n")

out_path = "/Users/Andy.Chen/Billing_Automation/ERA_check/era_payout_results.csv"
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
pd.DataFrame(all_results).reindex(columns=column_order).to_csv(out_path, index=False)
print("✅ Wrote:", out_path, "rows:", len(all_results))
