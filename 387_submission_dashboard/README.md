# 387 Submission Dashboard

## SQLMesh Prod Runbook (This Repo)

Use direct `sqlmesh` commands in this repository.

Do not use `smp` / `smesh` here, because your shell function points to a different repo (`/Users/Andy.Chen/data-warehouse-transformations`).

### Required dotenv

```bash
--dotenv /Users/Andy.Chen/ERA_dashboarding/.env
```

### 1) Check local changes vs prod

```bash
sqlmesh --dotenv /Users/Andy.Chen/ERA_dashboarding/.env diff prod
```

### 2) Promote model code to prod snapshot

```bash
sqlmesh --dotenv /Users/Andy.Chen/ERA_dashboarding/.env plan prod \
  --select-model raw.stedi_837_submission_parsed \
  --auto-apply
```

### 3) Restate/recompute historical data in prod

```bash
sqlmesh --dotenv /Users/Andy.Chen/ERA_dashboarding/.env plan prod \
  --include-unmodified \
  --select-model raw.stedi_837_submission_parsed \
  --restate-model raw.stedi_837_submission_parsed \
  --start 2026-02-06 \
  --end 2026-03-19 \
  --auto-apply
```

### 4) Validate parsed output

```bash
sqlmesh --log-file-dir /tmp --dotenv /Users/Andy.Chen/ERA_dashboarding/.env fetchdf \
"SELECT COUNT(*) AS total,
        COUNT(*) FILTER (WHERE patient_control_number_01 IS NOT NULL) AS pcn_non_null,
        COUNT(*) FILTER (WHERE patient_first_name IS NOT NULL) AS first_name_non_null,
        COUNT(*) FILTER (WHERE patient_last_name IS NOT NULL) AS last_name_non_null,
        COUNT(*) FILTER (WHERE member_id IS NOT NULL) AS member_id_non_null,
        COUNT(*) FILTER (WHERE plan_name IS NOT NULL) AS plan_name_non_null,
        COUNT(*) FILTER (WHERE total_charge_amount IS NOT NULL) AS charge_non_null,
        COUNT(*) FILTER (WHERE diagnosis_codes IS NOT NULL) AS diagnosis_non_null,
        COUNT(*) FILTER (WHERE parse_error IS NOT NULL) AS parse_error_non_null
 FROM raw.stedi_837_submission_parsed"
```

### 5) Parse error distribution

```bash
sqlmesh --log-file-dir /tmp --dotenv /Users/Andy.Chen/ERA_dashboarding/.env fetchdf \
"SELECT parse_error, COUNT(*) AS cnt
 FROM raw.stedi_837_submission_parsed
 GROUP BY 1
 ORDER BY 2 DESC NULLS LAST"
```

## Troubleshooting

- `No changes to plan`: usually means no snapshot changes were selected for that target environment.
- `Selector did not return any models`: add `--include-unmodified` plus explicit `--select-model ...` when restating unchanged models.
- `could not translate host name "None"`: env vars not loaded; include the explicit `--dotenv` path above.
