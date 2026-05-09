-- PageViewByDevice: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_seq BIGINT NOT NULL
  , device_type TINYINT NOT NULL -- {0: 'Pc', 1: 'Mobile', 2: 'All'}
  , page_click INTEGER
  , user_click INTEGER
  , time_on_site BIGINT
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, mall_seq, device_type)
);

-- PageViewByDevice: bulk_insert
INSERT INTO {{ table }}
SELECT
    $mall_seq AS mall_seq
  , (CASE
      WHEN measuredThrough.device = 'Pc' THEN 0
      WHEN measuredThrough.device = 'Mobile' THEN 1
      WHEN measuredThrough.device = 'All' THEN 2
      ELSE -1 END) AS device_type
  , visit.pageClick AS page_click
  , visit.userClick AS user_click
  , visit.timeOnSite AS time_on_site
  , CAST(ymd AS DATE) AS ymd
FROM {{ rows }}
WHERE measuredThrough.device IN ('Pc', 'Mobile', 'All')
ON CONFLICT DO NOTHING;


-- PageViewByUrl: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_seq BIGINT NOT NULL
  , page_url VARCHAR NOT NULL
  , page_click BIGINT
  , user_click BIGINT
  , time_on_site BIGINT
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, mall_seq, page_url)
);

-- PageViewByUrl: bulk_insert
INSERT INTO {{ table }}
SELECT
    $mall_seq AS mall_seq
  , measuredThrough.url AS page_url
  , visit.pageClick AS page_click
  , visit.userClick AS user_click
  , visit.timeOnSite AS time_on_site
  , CAST(ymd AS DATE) AS ymd
FROM {{ rows }}
WHERE measuredThrough.url IS NOT NULL
ON CONFLICT DO NOTHING;


-- PageViewByProduct: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_seq BIGINT NOT NULL
  , product_id BIGINT NOT NULL -- {10: 'Main URL'} | Product URL
  , page_click BIGINT
  , user_click BIGINT
  , time_on_site BIGINT
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, mall_seq, product_id)
);

-- PageViewByProduct: bulk_insert
INSERT INTO {{ table }}
SELECT items.*
FROM (
  SELECT
      $mall_seq AS mall_seq
    , (CASE
        WHEN REGEXP_MATCHES(measuredThrough.url, '^/[^/]+/products/\d+$') -- Product URL
          THEN CAST(REGEXP_EXTRACT(measuredThrough.url, '(\d+)$') AS BIGINT)
        WHEN REGEXP_MATCHES(measuredThrough.url, '^/[^/]+$') -- Main URL
          THEN 10
        ELSE NULL END) AS product_id
    , visit.pageClick AS page_click
    , visit.userClick AS user_click
    , visit.timeOnSite AS time_on_site
    , CAST(ymd AS DATE) AS ymd
  FROM {{ rows }}
  WHERE measuredThrough.url IS NOT NULL
) AS items
WHERE items.product_id IS NOT NULL
ON CONFLICT DO NOTHING;