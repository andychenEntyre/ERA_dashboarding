# Models Overview

This folder contains SQLMesh models for two Stedi pipelines:

- 837 claim submission intake and parsing
- 835 ERA intake and parsing

## Pipeline Map

### 837 Pipeline

1. `webhook_837_submission_seed.sql`
   - Model: `raw.stedi_837_submissions_seed`
   - Type: `VIEW` (hourly)
   - Purpose: filters `stedi_raw.events` to production 837 webhook events.

2. `webhook_837_submission_transaction_ids.sql`
   - Model: `raw.stedi_837_submissions`
   - Type: `VIEW` (hourly)
   - Purpose: extracts `transaction_id` from filtered 837 events.

3. `stedi_claim_submissions_payloads.py`
   - Model: `raw.stedi_claim_submissions_payloads`
   - Type: incremental by unique key (`transaction_id`) (hourly)
   - Purpose: calls Stedi Core API (`/transactions/{transaction_id}/input`) and stores raw payloads with fetch errors.

4. `stedi_837_submission_parsed.py`
   - Model: `raw.stedi_837_submission_parsed`
   - Type: incremental by unique key (`transaction_id`) (hourly)
   - Purpose: parses payloads (JSON/X12 fallback) into structured claim fields and records `parse_error` when needed.

### 835 Pipeline

1. `webhook_835_submission_seed.sql`
   - Model: `raw.stedi_835_submissions_seed`
   - Type: `VIEW`
   - Purpose: filters `stedi_raw.events` to production 835 webhook events.

2. `webhook_835_submission_transaction_ids.sql`
   - Model: `raw.stedi_835_submissions`
   - Type: `VIEW`
   - Purpose: extracts `transaction_id` from filtered 835 events.

3. `stedi_835_submissions_payloads.py`
   - Model: `raw.stedi_835_submissions_payloads`
   - Type: incremental by unique key (`transaction_id`)
   - Purpose: calls Stedi Healthcare ERA endpoint (`/change/medicalnetwork/reports/v2/{transaction_id}/835`) and stores payloads with fetch errors.

4. `stedi_835_submission_parsed.py`
   - Model: `raw.stedi_835_submission_parsed`
   - Type: incremental by unique key (`claim_row_id`)
   - Purpose: parses ERA payloads into claim-level rows and extracts claim/patient/payment fields plus service-level adjustment rollups.
   - Service adjustment rollups:
     - `service_adjustment_reason_codes_1`: comma-separated unique values from `adjustmentReasonCode1`
     - `service_adjustment_reasons_1`: comma-separated unique values from `adjustmentReason1`
   - Error handling: emits one error row per failed transaction with `parse_error`.

## Operational Notes

- 837 models run on hourly model cron.
- API models require `STEDI_API_KEY`.
- Unique keys:
  - 837 payloads and parsed: `transaction_id`
  - 835 payloads: `transaction_id`
  - 835 parsed: `claim_row_id`

## Common Commands

- Check missing intervals:
  - `sqlmesh --dotenv /Users/Andy.Chen/ERA_dashboarding/.env check_intervals prod`
- Run 835 pipeline:
  - `sqlmesh --dotenv /Users/Andy.Chen/ERA_dashboarding/.env run prod --select-model raw.stedi_835_submission_parsed`
- Run 837 pipeline:
  - `sqlmesh --dotenv /Users/Andy.Chen/ERA_dashboarding/.env run prod --select-model raw.stedi_837_submission_parsed`
