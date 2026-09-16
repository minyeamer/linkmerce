-- Item: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    item_id BIGINT NOT NULL
  , site_type TINYINT NOT NULL -- {1: 'auction', 2: 'gmarket'}
  , site_item_id VARCHAR NOT NULL
  , item_name VARCHAR
  , option_name VARCHAR
  , seller_id VARCHAR
  , brand_name VARCHAR
  , maker_name VARCHAR
  , category_code VARCHAR
  , category_name VARCHAR
  , sell_status VARCHAR
  , image_url VARCHAR
  , price INTEGER
  , created_at TIMESTAMP
  , updated_at TIMESTAMP
  , PRIMARY KEY (item_id, site_type)
);

-- Item: bulk_insert
INSERT INTO {{ table }}
SELECT
    TRY_CAST(goodsNo AS BIGINT) AS item_id
  , 1 AS site_type
  , siteGoodsNo.iac AS site_item_id
  , goodsName AS item_name
  , prmtGoodsName.iac AS option_name
  , siteSellerId.iac AS seller_id
  , brand.name AS brand_name
  , maker.name AS maker_name
  , category.esm.catCode AS category_code
  , category.esm.catName AS category_name
  , sellStatus.iac AS sell_status
  , imgUrl AS image_url
  , price.iac AS price
  , TRY_STRPTIME(createdDate, '%Y-%m-%d %H:%M:%S') AS created_at
  , TRY_STRPTIME(updatedDate, '%Y-%m-%d %H:%M:%S') AS updated_at
FROM {{ rows }}
WHERE (TRY_CAST(goodsNo AS BIGINT) IS NOT NULL)
  AND (siteGoodsNo.iac IS NOT NULL)
ON CONFLICT DO NOTHING;

INSERT INTO {{ table }}
SELECT
    TRY_CAST(goodsNo AS BIGINT) AS item_id
  , 2 AS site_type
  , siteGoodsNo.gmkt AS site_item_id
  , goodsName AS item_name
  , prmtGoodsName.gmkt AS option_name
  , siteSellerId.gmkt AS seller_id
  , brand.name AS brand_name
  , maker.name AS maker_name
  , category.esm.catCode AS category_code
  , category.esm.catName AS category_name
  , sellStatus.gmkt AS sell_status
  , imgUrl AS image_url
  , price.gmkt AS price
  , TRY_STRPTIME(createdDate, '%Y-%m-%d %H:%M:%S') AS created_at
  , TRY_STRPTIME(updatedDate, '%Y-%m-%d %H:%M:%S') AS updated_at
FROM {{ rows }}
WHERE (TRY_CAST(goodsNo AS BIGINT) IS NOT NULL)
  AND (siteGoodsNo.gmkt IS NOT NULL)
ON CONFLICT DO NOTHING;