MODEL (
  name raw.stedi_837_submissions,
  kind VIEW,
  grain (_id)
);

SELECT
  _created,
  _id,
  _index,
  _ip,
  _fivetran_synced,
  CAST(data AS JSONB) #>> '{event,detail,transactionId}' AS transaction_id,
  headers,
  data
FROM raw.stedi_837_submissions_seed
