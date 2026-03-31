MODEL (
  name raw.stedi_835_submissions_seed,
  kind VIEW,
  grain (_id)
);

SELECT
  _created,
  _id,
  _index,
  _ip,
  _fivetran_synced,
  headers,
  data
FROM stedi_raw.events
WHERE CAST(data AS JSONB) #>> '{event,detail,x12,metadata,transaction,transactionSetIdentifier}' = '835'
  AND CAST(data AS JSONB) #>> '{event,detail,mode}' = 'production'
