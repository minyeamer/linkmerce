-- Search: create
CREATE TABLE IF NOT EXISTS {{ sections }} (
    query VARCHAR NOT NULL
  , sections JSON -- list[list[dict]]
  , PRIMARY KEY (query)
);

CREATE TABLE IF NOT EXISTS {{ summary }} (
    query VARCHAR NOT NULL
  , seq INTEGER NOT NULL
  , section VARCHAR
  , subject VARCHAR NOT NULL
  , item_count INTEGER
  , PRIMARY KEY (query, seq, subject)
);

-- Search: bulk_insert
INSERT INTO {{ summary }}
SELECT
    query
  , seq
  , section
  , subject
  , item_count
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- CafeTab: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    query VARCHAR NOT NULL
  , rank INTEGER NOT NULL
  , cafe_url VARCHAR
  , article_id BIGINT
  , ad_id VARCHAR
  , cafe_name VARCHAR
  , title VARCHAR
  , description VARCHAR
  , url VARCHAR
  , image_url VARCHAR
  , article_url VARCHAR
  , replies VARCHAR
  , write_date VARCHAR
  , PRIMARY KEY (query, rank)
);

-- CafeTab: bulk_insert
INSERT INTO {{ table }}
SELECT
    $query AS query
  , rank
  , cafe_url
  , TRY_CAST(article_id AS BIGINT) AS article_id
  , ad_id
  , cafe_name
  , title
  , description
  , url
  , image_url
  , article_url
  , replies
  , COALESCE(STRFTIME(TRY_STRPTIME(write_date, '%Y.%m.%d.'), '%Y-%m-%d'), write_date) AS write_date
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- CafeArticle: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    cafe_id BIGINT NOT NULL
  , article_id BIGINT NOT NULL
  , cafe_url VARCHAR
  , cafe_name VARCHAR
  , menu_name VARCHAR
  -- , head VARCHAR
  , title VARCHAR
  , tags VARCHAR
  , nick_name VARCHAR
  -- , member_level VARCHAR
  , title_length INTEGER
  , content_length INTEGER
  , image_count INTEGER
  , read_count INTEGER
  , comment_count INTEGER
  , commenter_count INTEGER
  -- , member_count INTEGER
  , write_dt TIMESTAMP
  , PRIMARY KEY (cafe_id, article_id)
);

-- CafeArticle: bulk_insert
INSERT INTO {{ table }}
SELECT
    cafeId AS cafe_id
  , articleId AS article_id
  , cafe.url AS cafe_url
  , cafe.name AS cafe_name
  , article.menu.name AS menu_name
  -- , article.head AS head
  , article.subject AS title
  , tags
  , article.writer.nick AS nick_name
  -- , article.writer.memberLevelName AS member_level
  , LENGTH(article.subject) AS title_length
  , article.content.wordCount AS content_length
  , article.content.imageCount AS image_count
  , article.readCount AS read_count
  , article.commentCount AS comment_count
  , article.commenterCount AS commenter_count
  -- , cafe.memberCount AS member_count
  , make_timestamp(article.writeDate // 1000 * 1000000) AS write_dt
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- ShoppingPage: create (deprecated)
CREATE TABLE IF NOT EXISTS {{ table }} (
    keyword VARCHAR NOT NULL
  , page_unit_ad INTEGER
  , page_unit_shop INTEGER
  , updated_at TIMESTAMP NOT NULL
  , PRIMARY KEY (keyword)
);

-- ShoppingPage: bulk_insert (deprecated)
INSERT INTO {{ table }}
SELECT
    $keyword AS keyword
  , COUNT(CASE WHEN cardType = 'AD_CARD' THEN 1 END) AS page_unit_ad
  , COUNT(CASE WHEN cardType <> 'AD_CARD' THEN 1 END) AS page_unit_shop
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updated_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;