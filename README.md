# ERA_dashboarding

Utilities for working with ERA data and loading payout results into Postgres.

## ERA payout loader

`ERA_check/era_payout.py` reads transaction payloads from the webhook CSV, fetches 835 payout details from Stedi, flattens the claim-level results, and writes them to Postgres through SQLMesh.

### Prerequisites

- Python environment with SQLMesh installed
- Access to the target Postgres database
- A `.env` file in the repo root or in `ERA_check/`

### Required `.env` variables

```env
SQLMESH_DEV_HOST=your_host
SQLMESH_DEV_PORT=5432
SQLMESH_DEV_USER=your_user
SQLMESH_DEV_PASSWORD=your_password
SQLMESH_DEV_DATABASE=warehouse_dev
SQLMESH_DEV_SSLMODE=require
STEDI_API_KEY=your_stedi_api_key
```

### Optional `.env` variables

```env
SQLMESH_DEV_SCHEMA=public
SQLMESH_DEV_TABLE=era_payout_results
SQLMESH_DEV_WRITE_MODE=replace
```

- `SQLMESH_DEV_SCHEMA` defaults to `public`
- `SQLMESH_DEV_TABLE` defaults to `era_payout_results`
- `SQLMESH_DEV_WRITE_MODE` supports `replace` and `append`

### Run the script

If your `sqlmesh-3.13` environment is already active:

```bash
python ERA_check/era_payout.py
```

Or without activating it first:

```bash
PYENV_VERSION=sqlmesh-3.13 python ERA_check/era_payout.py
```

### Output

By default, the script writes to:

```text
warehouse_dev.public.era_payout_results
```

The script prints the final destination before loading, for example:

```text
Writing to warehouse_dev.public.era_payout_results (mode=replace)
```
