MODEL (
  name raw.stedi_837_submissions_seed,
  kind SEED (
    path '../19march26_837.csv'
  ),
  columns (
    _created TIMESTAMP,
    _id BIGINT,
    _index INTEGER,
    _ip TEXT,
    _fivetran_synced TIMESTAMP,
    headers TEXT,
    data TEXT
  ),
  grain (_id)
);
