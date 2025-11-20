# Technical Task Solutions 

This document contains my solutions for the SQL, JSON ingestion and incremental load tasks. 

SQL
1)
SELECT 
       it.senderaccountid,
       it.receiveraccountid,
       it.amt,
       it.currency,
       it.transferstatus,
       it.transfertime,
       sc.clientname AS SenderClientName,
       sg.groupname  AS SenderGroupName,
       sg.grouppod   AS SenderGroupPod,
       rc.clientname AS ReceiverClientName,
       rg.groupname  AS ReceiverGroupName,
       rg.grouppod   AS ReceiverGroupPod
FROM   internaltransfers it
       JOIN account sa
         ON it.senderaccountid = sa.accountid
       JOIN client sc
         ON sa.clientid = sc.clientid
       JOIN "group" sg
         ON sc.groupid = sg.groupid
       JOIN account ra
         ON it.receiveraccountid = ra.accountid
       JOIN client rc
         ON ra.clientid = rc.clientid
       JOIN "group" rg
         ON rc.groupid = rg.groupid
WHERE  it.transferstatus = 'completed'
       AND it.transfertime >= '2025-01-01'
       AND it.transfertime < '2025-04-01'
       AND sg.grouppod <> rg.grouppod;

2)
WITH live_clients  AS (
    SELECT
        c.ClientId
    FROM Client c
    JOIN Account a
      ON c.ClientId = a.ClientId
    GROUP BY c.ClientId
    HAVING SUM(
        CASE WHEN a.AccountStatus <> 'closed' THEN 1 ELSE 0 END
    ) > 0
)
SELECT
    sc.ClientId,
    sc.ClientName,
    SUM(it.Amt * der.Rate) AS TotalAmtGBP
FROM InternalTransfers it
JOIN Account sa
  ON it.SenderAccountId = sa.AccountId
JOIN Client sc
  ON sa.ClientId = sc.ClientId
JOIN live_clients lc
  ON sc.ClientId = lc.ClientId
JOIN DailyExchangeRate der
  ON der.FromCurrency = it.Currency
 AND der.ToCurrency   = 'GBP'
 AND der.Date         = CAST(it.TransferTime AS DATE)
WHERE it.TransferStatus = 'completed'
  AND it.TransferTime >= '2024-01-01'
  AND it.TransferTime <  '2025-01-01'
  AND sc.Vertical = 'Gambling'
GROUP BY
    sc.ClientId,
    sc.ClientName;

3)
WITH daily_vertical_currency_totals   AS (
    SELECT
        CAST(it.TransferTime AS DATE) AS transfer_date,
        sc.Vertical                    AS sender_vertical,
        it.Currency                    AS currency,
        SUM(it.Amt)                    AS total_amt
    FROM InternalTransfers it
    JOIN Account sa
      ON it.SenderAccountId = sa.AccountId
    JOIN Client sc
      ON sa.ClientId = sc.ClientId
    WHERE it.TransferStatus = 'completed'
      AND it.TransferTime >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY
        CAST(it.TransferTime AS DATE),
        sc.Vertical,
        it.Currency
)
SELECT
    transfer_date,
    sender_vertical,
    currency,
    total_amt,
    AVG(total_amt) OVER (
        PARTITION BY sender_vertical, currency
        ORDER BY transfer_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d
FROM daily_vertical_currency_totals
ORDER BY
    sender_vertical,
    currency,
    transfer_date;

JSON Ingestion
1)

The approach utilised here would be to land the raw JSON into the Bronze layer, clean and normalise in the Silver layer and finally compose tables in the Gold layer which are ready for analytics so it can be used by teams such as Business Intelligence.

Bronze:

Acting as a raw landing zone keeping the JSON exactly as received with no transformation this would be a combination of fields from the JSON and metadata fields which can be added from the ingestion pipeline. The approach here would be to have one row per ingested JSON document with the columns being client_id, raw_json, ingestion_timestamp, source_system and schema_version. The data can be partitioned by the ingestion_date from the ingestion pipeline metadata this would be important for manageability and then keep the JSON untouched so schema evolution can be handled with care and for reprocessing purposes.

Silver:

Silver zone acts as a clean and normalised layer so the raw JSON will be turned into structured and tables that can be queried. Due to this there will be transformations from the bronze to the silver zone. Parse the JSON into columns and set the data types for example ‘Limit’ and ‘Score would be numeric and ‘Appetite’ would be text. Handle missing fields, if ‘Credit’ is missing then set the relevant fields to be NULL.

I would create three tables the first one can be silver.client where there would be one row per client per ingestion with the columns being client_id, credit_limit, credit_score, ingestion_timestamp.

The second table would be related to the partner bank information, since the partner bank information within the JSON is structurally the same this can be contained within the rows of this table. I would create a table called silver.partner_bank with the columns client_id, partner_bank, appetite, ingestion_timestamp.

The third table would be related to the reasons with there being one row per season. The table would be called silver.partner_bank_reason and the columns client_id, partner_bank, reason_index, reason_text, ingestion_timestamp.

Gold:

In the gold zone the goal is to have tables ready for tools to be built upon such as dashboards or any other reporting.

The first table would be gold.client_risk_summary a wide table with the following columns client_id, max_appetite, min_appetite, num_partner_banks_high_appetite, num_partner_banks_low_appetite, credit_limit, credit_score, reasons_text.

The second table would be aggregated by the partner bank. The table would be gold.partner_bank_risk_view with the columns being partner_bank, num_clients, avg_credit_score, pct_high_appetite. There will have to be some aggregations and business logic applied in this zone where the appetite text would be converted into ordered levels (Low<Medium<High). Counts of the client per appetite bucket per partner bank.

2)

The ingestion would have to be designed with schema evolution in mind. The bronze layer will always store the raw JSON as it is with some additional metadata this allows for reprocessing to occur if the nesting or the fieldnames change.

The silver layer can have some form of flexible parsing, the main objective is to make sure the silver layer schema remains stable such that the column names are not renamed even if the JSON does. This will be achieved by parsing the raw JSON into a small set of tables that are well typed therefore even if the nests or field names change in the JSON the parsing logic can be updated to map the old and new paths into the same columns therefore ensuring the downstream tables and ultimately the reports built on those downstream tables do not break.

Gold will be insulated from the upstream changes they will use stable column names that are business friendly and additional columns/fields will be introduced with great care. Bronze layer the JSON will be monitored carefully for schema drift and potentially use contracts or schema versions with the data provider.

Incremental Ingestion

A watermark based incremental ingestion strategy would be put in place. The maximum ‘last_updated_at’ ingested would be tracked, and only records with the ‘last_updated_at’ greater than the previous watermark will be ingested periodically every 45 minutes. A small overlap window will be employed to handle late updates. The changes will go into a staging table and from there a merge will be used on ‘transaction_id’ into the main transactional table, this ensures inserts and updates are applied idempotently. For each batch the source and target row counts would be compared and only advance the watermark if they match. Checks would be put in place for watermark continuity, duplicate primary keys and required fields. Over a larger window periodic source-target reconciliations would be ran. Partitioning by date and indexing on ‘transaction_id’/ ‘last_updated_at’ keeps the merges efficient even when the dataset contains millions of rows.
