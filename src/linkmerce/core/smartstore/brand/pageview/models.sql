-- PageViewByDevice: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_seq BIGINT
  , device_type BIGINT -- {0: 'Pc', 1: 'Mobile', 2: 'All'}
  , page_click BIGINT
  , user_click BIGINT
  , time_on_site BIGINT
  , ymd DATE
  , PRIMARY KEY (ymd, mall_seq, device_type)
);

-- PageViewByDevice: select
SELECT
    TRY_CAST($mall_seq AS BIGINT) AS mall_seq
  , (CASE
      WHEN measuredThrough.device = 'Pc' THEN 0
      WHEN measuredThrough.device = 'Mobile' THEN 1
      WHEN measuredThrough.device = 'All' THEN 2
      ELSE NULL END) AS device_type
  , visit.pageClick AS page_click
  , visit.userClick AS user_click
  , visit.timeOnSite AS time_on_site
  , TRY_CAST(ymd AS DATE) AS ymd
FROM {{ array }}
WHERE (measuredThrough.device IN ('Pc','Mobile','All'))
  AND (TRY_CAST(ymd AS DATE) IS NOT NULL);

-- PageViewByDevice: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- PageViewByProduct: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_seq BIGINT
  , product_id BIGINT -- {10: 'Main', ...product}
  , page_click BIGINT
  , user_click BIGINT
  , time_on_site BIGINT
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, mall_seq, product_id)
);

-- PageViewByProduct: select
SELECT items.*
FROM (
  SELECT
      TRY_CAST($mall_seq AS BIGINT) AS mall_seq
    , (CASE
        WHEN REGEXP_MATCHES(measuredThrough.url, '^/[^/]+/products/\d+$')
          THEN CAST(REGEXP_EXTRACT(measuredThrough.url, '(\d+)$') AS BIGINT)
        WHEN REGEXP_MATCHES(measuredThrough.url, '^/[^/]+$')
          THEN 10
        ELSE NULL END) AS product_id
    , visit.pageClick AS page_click
    , visit.userClick AS user_click
    , visit.timeOnSite AS time_on_site
    , TRY_CAST(ymd AS DATE) AS ymd
  FROM {{ array }}
  WHERE (measuredThrough.url IS NOT NULL)
    AND (TRY_CAST(ymd AS DATE) IS NOT NULL)
) AS items
WHERE items.product_id IS NOT NULL;

-- PageViewByProduct: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- PageViewByUrl: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_seq BIGINT
  , page_url VARCHAR
  , page_click BIGINT
  , user_click BIGINT
  , time_on_site BIGINT
  , ymd DATE
  , PRIMARY KEY (ymd, mall_seq, page_url)
);

-- PageViewByUrl: select
SELECT
    TRY_CAST($mall_seq AS BIGINT) AS mall_seq
  , measuredThrough.url AS page_url
  , visit.pageClick AS page_click
  , visit.userClick AS user_click
  , visit.timeOnSite AS time_on_site
  , TRY_CAST(ymd AS DATE) AS ymd
FROM {{ array }}
WHERE (measuredThrough.url IS NOT NULL)
  AND (TRY_CAST(ymd AS DATE) IS NOT NULL);

-- PageViewByUrl: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;