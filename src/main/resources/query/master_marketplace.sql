SELECT
  mp_code, marketplace
FROM
  (
    SELECT
      DISTINCT mp_code,
      marketplace,
      ROW_NUMBER() OVER(
        PARTITION BY mp_code
        ORDER BY
          marketplace DESC
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
      mp_code IS NOT NULL
  ) as dat_mp
WHERE
  row_num = 1;
