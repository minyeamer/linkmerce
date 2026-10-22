-- Product: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_id VARCHAR NOT NULL
  , product_no BIGINT NOT NULL
  , product_code VARCHAR
  , custom_product_code VARCHAR
  , model_name VARCHAR
  , product_name VARCHAR
  , image_url VARCHAR
  , display BOOLEAN
  , selling BOOLEAN
  , sold_out BOOLEAN
  , price INTEGER
  , retail_price INTEGER
  , supply_price INTEGER
  , margin_rate VARCHAR
  , tags VARCHAR
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
  , PRIMARY KEY (mall_id, product_no)
);

-- Product: bulk_insert
INSERT INTO {{ table }}
SELECT
    $mall_id AS mall_id
  , product_no
  , product_code
  , NULLIF(custom_product_code, '') AS custom_product_code
  , NULLIF(model_name, '') AS model_name
  , product_name
  , NULLIF(list_image, '') AS image_url
  , (CASE display WHEN 'T' THEN TRUE WHEN 'F' THEN FALSE ELSE NULL END) AS display
  , (CASE selling WHEN 'T' THEN TRUE WHEN 'F' THEN FALSE ELSE NULL END) AS selling
  , (CASE sold_out WHEN 'T' THEN TRUE WHEN 'F' THEN FALSE ELSE NULL END) AS sold_out
  , TRY_CAST(split_part(price, '.', 1) AS INTEGER) AS price
  , TRY_CAST(split_part(retail_price, '.', 1) AS INTEGER) AS retail_price
  , TRY_CAST(split_part(supply_price, '.', 1) AS INTEGER) AS supply_price
  , margin_rate
  , NULLIF(array_to_string(product_tag, ','), '') AS tags
  , TRY_STRPTIME(SUBSTR(created_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
  , TRY_STRPTIME(SUBSTR(updated_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS modify_dt
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- Variant: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_id VARCHAR NOT NULL
  , product_no BIGINT NOT NULL
  , variant_code VARCHAR NOT NULL
  , option_group1 VARCHAR
  , option_name1 VARCHAR
  , option_group2 VARCHAR
  , option_name2 VARCHAR
  , option_group3 VARCHAR
  , option_name3 VARCHAR
  -- , custom_variant_code VARCHAR
  , image_url VARCHAR
  , display BOOLEAN
  , selling BOOLEAN
  , option_price INTEGER
  , supply_price INTEGER
  , display_order INTEGER
  , PRIMARY KEY (mall_id, variant_code)
);

-- Variant: bulk_insert
INSERT INTO {{ table }}
SELECT
    $mall_id AS mall_id
  , CAST($product_no AS BIGINT) AS product_no
  , variant_code
  , options."0".name AS option_group1
  , options."0".value AS option_name1
  , options."1".name AS option_group2
  , options."1".value AS option_name2
  , options."2".name AS option_group3
  , options."2".value AS option_name3
  -- , NULLIF(custom_variant_code, '') AS custom_variant_code
  , NULLIF(image, '') AS image_url
  , (CASE display WHEN 'T' THEN TRUE WHEN 'F' THEN FALSE ELSE NULL END) AS display
  , (CASE selling WHEN 'T' THEN TRUE WHEN 'F' THEN FALSE ELSE NULL END) AS selling
  , TRY_CAST(split_part(additional_amount, '.', 1) AS INTEGER) AS option_price
  , TRY_CAST(split_part(supply_price, '.', 1) AS INTEGER) AS supply_price
  , display_order
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- Category: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_id VARCHAR NOT NULL
  , category_id INTEGER NOT NULL
  , category_name VARCHAR
  , category_id1 INTEGER
  , category_id2 INTEGER
  , category_id3 INTEGER
  , category_id4 INTEGER
  , category_name1 VARCHAR
  , category_name2 VARCHAR
  , category_name3 VARCHAR
  , category_name4 VARCHAR
  , depth SMALLINT
  , display BOOLEAN
  , display_order INTEGER
  , PRIMARY KEY (mall_id, category_id)
);

-- Category: bulk_insert
INSERT INTO {{ table }}
SELECT
    $mall_id AS mall_id
  , category_no AS category_id
  , category_name
  , full_category_no."1" AS category_id1
  , full_category_no."2" AS category_id2
  , full_category_no."3" AS category_id3
  , full_category_no."4" AS category_id4
  , full_category_name."1" AS category_name1
  , full_category_name."2" AS category_name2
  , full_category_name."3" AS category_name3
  , full_category_name."4" AS category_name4
  , category_depth AS depth
  , (CASE use_display WHEN 'T' THEN TRUE WHEN 'F' THEN FALSE ELSE NULL END) AS display
  , display_order
FROM {{ rows }}
ON CONFLICT DO NOTHING;
