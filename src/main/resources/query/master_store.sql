SELECT
  brand_id, official_store
FROM
  (
    SELECT
      DISTINCT channel_code as brand_id,
      CASE WHEN LOWER (official_store) LIKE 'sleek%' THEN 'Kino Baby & Kids Official Store' WHEN LOWER (official_store) LIKE 'ellip%' THEN 'Kino Hair & Beauty Care Official Store' ELSE TRIM(
        REGEXP_REPLACE(
          official_store, '[^-0-9A-Za-z&() ]',
          ''
        )
      ) END AS official_store,
      ROW_NUMBER() OVER(
        PARTITION BY channel_code
        ORDER BY
          official_store DESC
      ) row_num
    FROM
      (
        SELECT
          DISTINCT TRIM(
            SUBSTRING_INDEX(channel_code, '_', 1)
          ) channel_code,
          LOWER(
            TRIM(
              SUBSTRING_INDEX(channel_code, '_', -1)
            )
          ) mp_code,
          CASE WHEN channel_name REGEXP "B2B Beauty|B2B JNS|B2BJNS"
          AND channel_name NOT LIKE '%-%' THEN TRIM(
            SUBSTRING_INDEX(channel_code, '_', -1)
          ) ELSE TRIM(
            SUBSTRING_INDEX(channel_name, '-', -1)
          ) END AS official_store,
          TRIM(
            SUBSTRING_INDEX(channel_name, '-', 1)
          ) marketplace
        FROM
          oms_channel
        WHERE
          NOT LOWER(channel_code) REGEXP 'request|interco|csr|nonevent|reclass|non event|offline'
      ) data_oms
    WHERE
      official_store IS NOT NULL
      OR official_store != ""
      AND channel_code IS NOT NULL
  ) as o_os
WHERE
  row_num = 1
