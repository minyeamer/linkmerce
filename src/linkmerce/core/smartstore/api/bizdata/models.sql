-- MarketingChannel: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    channel_seq BIGINT
  , device_category VARCHAR
  , nt_source VARCHAR
  , nt_medium VARCHAR
  , nt_detail VARCHAR
  , nt_keyword VARCHAR
  , num_users INTEGER
  , num_interactions INTEGER
  , page_view INTEGER
  , num_purchases INTEGER
  , payment_amount INTEGER
  , ymd DATE
  , PRIMARY KEY (ymd, channel_seq, device_category, nt_source, nt_medium, nt_detail, nt_keyword)
);

-- MarketingChannel: select
SELECT
    CAST($channel_seq AS BIGINT) AS channel_seq
  , IFNULL(deviceCategory, '-') AS device_category
  , IFNULL(ntSource, '-') AS nt_source
  , IFNULL(ntMedium, '-') AS nt_medium
  , IFNULL(ntDetail, '-') AS nt_detail
  , IFNULL(ntKeyword, '-') AS nt_keyword
  , TRY_CAST(numUsers AS INTEGER) AS num_users
  , TRY_CAST(numInteractions AS INTEGER) AS num_interactions
  , TRY_CAST(pv AS INTEGER) AS page_view
  , TRY_CAST(numPurchases AS INTEGER) AS num_purchases
  , TRY_CAST(payAmount AS INTEGER) AS payment_amount
  , CAST($ymd AS DATE) AS ymd
FROM {{ array }};

-- MarketingChannel: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;