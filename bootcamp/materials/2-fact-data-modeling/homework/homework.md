# Week 2 Fact Data Modeling
The homework this week will be using the `devices` and `events` dataset

Construct the following eight queries:

- A query to deduplicate `game_details` from Day 1 so there's no duplicates
```
WITH game_details_with_row_num AS (
  SELECT a.*,
  ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS row_num
  FROM game_details AS a
)
-- If row_num shouldn't be part of the final table, we must specify all projected columns individually in Postgres
SELECT *
FROM game_details_with_row_num
WHERE row_num = 1;
```

- A DDL for an `user_devices_cumulated` table that has:
  - a `device_activity_datelist` which tracks a users active days by `browser_type`
  - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)
```
CREATE TABLE user_devices_cumulative (
  user_id NUMERIC,
  date DATE,
  browser_type TEXT,
  device_activity_datelist DATE[],
  PRIMARY KEY (user_id, date, browser_type)
);
```

- A cumulative query to generate `device_activity_datelist` from `events`
```
-- user_devices_cumulative is populated daily accummulating the prior day's results.
WITH previous AS (
  SELECT
    user_id,
    browser_type,
    device_activity_datelist
  FROM user_devices_cumulative
  -- CHANGEME: Previous date
  WHERE date = '2022-12-31'::DATE
),
current AS (
  WITH events_deduped AS (
    SELECT DISTINCT
      user_id,
      device_id,
      DATE_TRUNC('day', event_time::TIMESTAMP)::DATE AS date
    FROM events
    WHERE
      user_id IS NOT NULL
      -- CHANGEME: Current date
      AND DATE_TRUNC('day', event_time::TIMESTAMP)::DATE = '2023-01-01'::DATE
  ),
  devices_deduped AS (
    SELECT DISTINCT device_id, browser_type
    FROM devices
  )
  SELECT DISTINCT
    user_id,
    date,
    browser_type
  FROM events_deduped
  JOIN devices_deduped
    ON events_deduped.device_id = devices_deduped.device_id
)
INSERT INTO user_devices_cumulative
SELECT
  COALESCE(l.user_id, t.user_id) AS user_id,
  --CHANGEME: Current date
  '2023-01-01'::DATE AS date,
  COALESCE(l.browser_type, t.browser_type) AS browser_type,
  CASE WHEN t.date IS NOT NULL THEN COALESCE(l.device_activity_datelist, ARRAY[]::DATE[]) || t.date
  ELSE COALESCE(l.device_activity_datelist, ARRAY[]::DATE[]) END AS device_activity_datelist
FROM previous l
FULL OUTER JOIN current t
ON l.user_id = t.user_id
AND l.browser_type = t.browser_type;
```

- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column
```
CREATE TABLE user_devices_cumulative (
  user_id NUMERIC,
  date DATE,
  browser_type TEXT,
  -- device_activity_datelist will store 32 days of activity. The most significant bit (MSB) represents today.
  -- The least significant bit (LSB) represents activity 32 days ago; initially this information won't be complete
  -- until the table has accumulated 32 days worth of changes.
  device_activity_datelist BIT(32),
  PRIMARY KEY (user_id, date, browser_type)
);

WITH previous AS (
  SELECT
    user_id,
    browser_type,
    device_activity_datelist
  FROM user_devices_cumulative
  -- CHANGEME: Previous date
  WHERE date = '2022-12-31'::DATE
),
current AS (
  WITH events_deduped AS (
    SELECT DISTINCT
      user_id,
      device_id,
      DATE_TRUNC('day', event_time::TIMESTAMP)::DATE AS date
    FROM events
    WHERE
      user_id IS NOT NULL
      -- CHANGEME: Current date
      AND DATE_TRUNC('day', event_time::TIMESTAMP)::DATE = '2023-01-01'::DATE
  ),
  devices_deduped AS (
    SELECT DISTINCT device_id, browser_type
    FROM devices
  )
  SELECT DISTINCT
    user_id,
    date,
    browser_type
  FROM events_deduped
  JOIN devices_deduped
    ON events_deduped.device_id = devices_deduped.device_id
)
INSERT INTO user_devices_cumulative2
SELECT
  COALESCE(l.user_id, t.user_id) AS user_id,
  --CHANGEME: Current date
  '2023-01-01'::DATE AS date,
  COALESCE(l.browser_type, t.browser_type) AS user_id,
  CASE
    WHEN l.device_activity_datelist IS NOT NULL AND t.date IS NULL THEN (l.device_activity_datelist >> 1)
    WHEN l.device_activity_datelist IS NOT NULL AND t.date IS NOT NULL THEN (l.device_activity_datelist >> 1) | (POW(2,32-1)::BIGINT::BIT(32))
    ELSE (POW(2,32-1)::BIGINT::BIT(32))
  END AS device_activity_datelist
FROM previous l
FULL OUTER JOIN current t
ON l.user_id = t.user_id
AND l.browser_type = t.browser_type;
```

- A DDL for `hosts_cumulated` table
  - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
```
CREATE TABLE hosts_cumulative (
  host TEXT,
  date DATE,
  -- host_activity_datelist will store 32 days of activity. The most significant bit (MSB) represents today.
  -- The least significant bit (LSB) represents activity 32 days ago; initially this information won't be complete
  -- until the table has accumulated 32 days worth of changes.
  host_activity_datelist BIT(32),
  PRIMARY KEY (host, date)
);
```

- The incremental query to generate `host_activity_datelist`
```
WITH previous AS (
  SELECT
    host,
    host_activity_datelist
  FROM hosts_cumulative
  -- CHANGEME: Previous date
  WHERE date = '2022-12-31'::DATE
),
current AS (
  SELECT DISTINCT
    host,
    DATE_TRUNC('day', event_time::TIMESTAMP)::DATE AS date
  FROM events
  WHERE
    user_id IS NOT NULL
    -- CHANGEME: Current date
    AND DATE_TRUNC('day', event_time::TIMESTAMP)::DATE = '2023-01-01'::DATE
)
INSERT INTO hosts_cumulative
SELECT
  COALESCE(l.host, t.host) AS host,
  --CHANGEME: Current date
  '2023-01-01'::DATE AS date,
  CASE
    WHEN l.host_activity_datelist IS NOT NULL AND t.date IS NULL THEN (l.host_activity_datelist >> 1)
    WHEN l.host_activity_datelist IS NOT NULL AND t.date IS NOT NULL THEN (l.host_activity_datelist >> 1) | (POW(2,32-1)::BIGINT::BIT(32))
    ELSE (POW(2,32-1)::BIGINT::BIT(32))
  END AS host_activity_datelist
FROM previous l
FULL OUTER JOIN current t
ON l.host = t.host;
```

- A monthly, reduced fact table DDL `host_activity_reduced`
   - month
   - host
   - hit_array - think COUNT(1)
   - unique_visitors array -  think COUNT(DISTINCT user_id)
```
CREATE TABLE monthly_host_activity_reduced
(
    host TEXT,
    hits_array BIGINT[],
    unique_visitors BIGINT[],
    month DATE,
    first_found_date DATE,
    date_partition DATE,
    PRIMARY KEY (host, date_partition, month)
);
```

- An incremental query that loads `host_activity_reduced`
  - day-by-day
```
WITH yesterday AS (
  SELECT *
  FROM monthly_host_activity_reduced
  --CHANGEME: The table is at the monthly grain. Initially it will be empty for the month. This should
  --never be set to a different month other than the as of date's month.
  WHERE date_partition = '2023-01-02'::DATE
),
today AS (
  SELECT
    host,
    DATE_TRUNC('day', event_time::TIMESTAMP)::DATE AS today_date,
    COUNT(1) AS num_hits,
    COUNT(DISTINCT user_id) AS num_visitors
  FROM events
  WHERE
    user_id IS NOT NULL
    -- CHANGEME: Current date
    AND DATE_TRUNC('day', event_time::TIMESTAMP)::DATE = '2023-01-03'::DATE
  GROUP BY 1, 2
)
INSERT INTO monthly_host_activity_reduced
SELECT
  COALESCE(y.host, t.host) AS host,
  CASE
    --CHANGEME: Adjust the starting date constant
    WHEN y.hits_array IS NULL AND t.num_hits IS NOT NULL THEN array_fill(NULL::BIGINT, ARRAY[DATE('2023-01-03') - DATE('2023-01-01')]) || ARRAY[t.num_hits]
    WHEN y.hits_array IS NOT NULL AND t.num_hits IS NULL THEN y.hits_array || ARRAY[NULL::BIGINT]
    ELSE y.hits_array || ARRAY[t.num_hits]
  END AS hit_array,
  CASE
    --CHANGEME: Adjust the starting date constant
    WHEN y.unique_visitors IS NULL AND t.num_visitors IS NOT NULL THEN array_fill(NULL::BIGINT, ARRAY[DATE('2023-01-03') - DATE('2023-01-01')]) || ARRAY[t.num_visitors]
    WHEN y.unique_visitors IS NOT NULL AND t.num_visitors IS NULL THEN y.unique_visitors || ARRAY[NULL::BIGINT]
    ELSE y.unique_visitors || ARRAY[t.num_visitors]
  END AS unique_visitors,
  COALESCE(y.month, DATE_TRUNC('month', t.today_date)) AS month,
  CASE
    WHEN y.first_found_date < t.today_date THEN y.first_found_date
    ELSE t.today_date
  END AS first_found_date,
  --CHANGEME: Current date
  DATE('2023-01-03') AS date_partition
FROM yesterday y
FULL OUTER JOIN today t
    ON y.host = t.host;
```

Please add these queries into a folder, zip them up and submit [here](https://bootcamp.techcreator.io)
