-- PostgreSQL 원천 테이블 초기화 스크립트
-- 테이블명 규칙: <platform>.<table>
-- CREATE INDEX: <pfm>_<table>__<cluster>_idx
-- PARTITION BY RANGE: 시계열 필드 (일별/시간별 범위 지정)

-- ============================================================
-- 스키마 생성
-- ============================================================

CREATE SCHEMA IF NOT EXISTS cj_eflexs;
CREATE SCHEMA IF NOT EXISTS cj_loisparcel;
CREATE SCHEMA IF NOT EXISTS coupang;
CREATE SCHEMA IF NOT EXISTS coupang_ads;
CREATE SCHEMA IF NOT EXISTS ecount;
CREATE SCHEMA IF NOT EXISTS google;
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS naver;
CREATE SCHEMA IF NOT EXISTS relation;
CREATE SCHEMA IF NOT EXISTS sabangnet;
CREATE SCHEMA IF NOT EXISTS searchad;
CREATE SCHEMA IF NOT EXISTS searchad_gfa;
CREATE SCHEMA IF NOT EXISTS smartstore;
CREATE SCHEMA IF NOT EXISTS ss_bizdata;
CREATE SCHEMA IF NOT EXISTS ss_hcenter;

-- ============================================================
-- cj_eflexs
-- ============================================================

CREATE TABLE IF NOT EXISTS cj_eflexs.invoice (
    invoice_no TEXT NOT NULL
  , package_no TEXT
  , order_id TEXT
  , product_name TEXT
  , delivery_type TEXT
  , package_type TEXT
  , box_type TEXT
  , sbs_box_type TEXT
  , order_quantity INTEGER
  , sku_quantity INTEGER
  , delivery_fee INTEGER
  , shipping_cost INTEGER
  , jeju_cost INTEGER
  , extra_cost INTEGER
  , service_cost INTEGER
  , box_cost INTEGER
  , pickup_date DATE NOT NULL
  , PRIMARY KEY (pickup_date, invoice_no)
) PARTITION BY RANGE (pickup_date);

CREATE TABLE IF NOT EXISTS cj_eflexs.invoice_order (
    invoice_no TEXT NOT NULL
  , package_no TEXT
  , order_id TEXT NOT NULL
  , delivery_fee INTEGER
  , box_cost INTEGER
  , order_date DATE NOT NULL
  , pickup_date DATE NOT NULL
  , PRIMARY KEY (order_date, invoice_no, order_id)
) PARTITION BY RANGE (order_date);

CREATE TABLE IF NOT EXISTS cj_eflexs.stock (
    item_code TEXT NOT NULL
  , barcode TEXT
  , customer_id BIGINT NOT NULL
  , item_name TEXT
  , warehouse_code TEXT
  , warehouse_name TEXT
  , zone_code TEXT
  , location_name TEXT
  , lot_no INTEGER
  , total_quantity INTEGER
  , usable_quantity INTEGER
  , hold_quantity INTEGER
  , process_quantity INTEGER
  , remain_days INTEGER
  , validate_date DATE
  , inbound_date DATE
  , updated_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS cje_stock__itme_idx ON cj_eflexs.stock (customer_id, item_code);

-- ============================================================
-- cj_loisparcel
-- ============================================================

CREATE TABLE IF NOT EXISTS cj_loisparcel.invoice (
    invoice_no TEXT NOT NULL
  , order_id TEXT
  , product_id TEXT
  , product_name TEXT
  , order_quantity INTEGER
  , delivery_fee INTEGER
  , shipper_name TEXT
  , receiver_name TEXT
  , zipcode TEXT
  , address TEXT
  , phone1 TEXT
  , phone2 TEXT
  , delivery_message TEXT
  , box_type TEXT
  , delivery_type TEXT
  , order_status TEXT
  , delivery_status TEXT
  , register_date DATE NOT NULL
  , pickup_date DATE
  , delivery_date DATE
  , PRIMARY KEY (register_date, invoice_no)
) PARTITION BY RANGE (register_date);

-- ============================================================
-- coupang
-- ============================================================

CREATE TABLE IF NOT EXISTS coupang.inventory (
    vendor_inventory_id BIGINT
  , vendor_inventory_item_id BIGINT
  , product_id BIGINT
  , option_id BIGINT NOT NULL
  , sku_id BIGINT
  , vendor_id TEXT NOT NULL
  , stock_quantity INTEGER
  , inprogress_quantity INTEGER
  , sales_amount_7d INTEGER
  , sales_amount_30d INTEGER
  , unit_sold_7d INTEGER
  , unit_sold_30d INTEGER
  , days_of_cover INTEGER
  , fee_amount INTEGER
  , updated_at TIMESTAMP
  , PRIMARY KEY (vendor_id, option_id)
);

CREATE TABLE IF NOT EXISTS coupang.option (
    vendor_inventory_id BIGINT
  , vendor_inventory_item_id BIGINT
  , product_id BIGINT
  , option_id BIGINT NOT NULL
  , item_id BIGINT
  , barcode TEXT
  , vendor_id TEXT NOT NULL
  , product_name TEXT
  , option_name TEXT
  , display_category_id INTEGER
  , category_id INTEGER
  , category_name TEXT
  , brand_name TEXT
  , maker_name TEXT
  , product_status BIGINT
  , is_deleted BOOLEAN
  , price INTEGER
  , sales_price INTEGER
  , delivery_fee INTEGER
  , order_quantity INTEGER
  , stock_quantity INTEGER
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
  , PRIMARY KEY (vendor_id, option_id)
);

CREATE TABLE IF NOT EXISTS coupang.rocket_sales (
    order_id BIGINT NOT NULL
  , vendor_id TEXT NOT NULL
  , product_id BIGINT
  , option_id BIGINT NOT NULL
  , sku_id BIGINT
  , category_id INTEGER
  , settlement_type SMALLINT NOT NULL
  , period_type SMALLINT NOT NULL
  , unit_price INTEGER
  , order_quantity INTEGER
  , coupang_discount INTEGER
  , seller_discount INTEGER
  , settlement_amount INTEGER
  , sales_date DATE NOT NULL
  , settlement_date DATE
  , PRIMARY KEY (sales_date, vendor_id, order_id, option_id, settlement_type)
) PARTITION BY RANGE (sales_date);

CREATE TABLE IF NOT EXISTS coupang.rocket_shipping (
    order_id BIGINT NOT NULL
  , invoice_no BIGINT
  , vendor_id TEXT NOT NULL
  , product_id BIGINT
  , option_id BIGINT NOT NULL
  , sku_id BIGINT
  , settlement_type SMALLINT NOT NULL
  , period_type SMALLINT NOT NULL
  , warehousing_fee INTEGER
  , discount_amount INTEGER
  , extra_fee INTEGER
  , sales_date DATE NOT NULL
  , shipping_date DATE
  , settlement_date DATE
  , PRIMARY KEY (sales_date, vendor_id, order_id, option_id, settlement_type)
) PARTITION BY RANGE (sales_date);

CREATE TABLE IF NOT EXISTS coupang.vendor (
    vendor_id TEXT NOT NULL
  , vendor_name TEXT
  , vendor_alias TEXT
  , rocket_sales_date DATE
  , rocket_settle_date DATE
  , PRIMARY KEY (vendor_id)
);

-- ============================================================
-- coupang_ads
-- ============================================================

CREATE TABLE IF NOT EXISTS coupang_ads.campaign (
    campaign_id BIGINT NOT NULL
  , campaign_name TEXT
  , campaign_type TEXT
  , vendor_id TEXT NOT NULL
  , vendor_type SMALLINT
  , goal_type SMALLINT
  , group_id BIGINT
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , roas_target INTEGER
  , created_at TIMESTAMP
  , updated_at TIMESTAMP
  , PRIMARY KEY (vendor_id, campaign_id)
);

CREATE TABLE IF NOT EXISTS coupang_ads.adgroup (
    adgroup_id BIGINT NOT NULL
  , campaign_id BIGINT
  , adgroup_name TEXT
  , vendor_id TEXT NOT NULL
  , goal_type SMALLINT
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , roas_target INTEGER
  , created_at TIMESTAMP
  , updated_at TIMESTAMP
  , PRIMARY KEY (vendor_id, adgroup_id)
);

CREATE TABLE IF NOT EXISTS coupang_ads.creative (
    creative_id BIGINT NOT NULL
  , option_id BIGINT
  , vendor_id TEXT NOT NULL
  , creative_type TEXT
  , headline TEXT
  , ordering INTEGER
  , PRIMARY KEY (vendor_id, creative_id)
);

CREATE TABLE IF NOT EXISTS coupang_ads.report_pa (
    campaign_id BIGINT NOT NULL
  , vendor_id TEXT NOT NULL
  , option_id BIGINT NOT NULL
  , option_conv_id BIGINT NOT NULL
  , placement_group BIGINT NOT NULL
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , conv_count INTEGER
  , direct_conv_count INTEGER
  , conv_amount INTEGER
  , direct_conv_amount INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, vendor_id, campaign_id, option_id, option_conv_id, placement_group)
) PARTITION BY RANGE (ymd);

CREATE TABLE IF NOT EXISTS coupang_ads.report_nca (
    campaign_id BIGINT NOT NULL
  , vendor_id TEXT NOT NULL
  , creative_id BIGINT NOT NULL
  , creative_type SMALLINT
  , option_id BIGINT
  , placement_group BIGINT NOT NULL
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , view_count INTEGER
  , stay_time NUMERIC
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, vendor_id, campaign_id, creative_id, placement_group)
) PARTITION BY RANGE (ymd);

-- ============================================================
-- ecount
-- ============================================================

CREATE TABLE IF NOT EXISTS ecount.inventory (
    product_code TEXT NOT NULL
  , quantity INTEGER
  , updated_at TIMESTAMP
  , PRIMARY KEY (product_code)
);

CREATE TABLE IF NOT EXISTS ecount.product (
    product_code TEXT NOT NULL
  , option_id TEXT
  , product_name TEXT
  , remarks_name TEXT
  , brand_name TEXT
  , unit_quantity INTEGER
  , unit_name TEXT
  , org_price INTEGER
  , expiration_date TEXT
  , updated_at TIMESTAMP
  , PRIMARY KEY (product_code)
);
CREATE INDEX IF NOT EXISTS eco_product__brand_idx ON ecount.product (brand_name);

CREATE TABLE IF NOT EXISTS ecount.cost (
    id BIGINT NOT NULL
  , name TEXT
  , sales_team TEXT
  , brand_name TEXT
  , cost_type TEXT NOT NULL
  , cost BIGINT
  , start_date DATE
  , end_date DATE NOT NULL
  , PRIMARY KEY (end_date, id)
) PARTITION BY RANGE (end_date);
CREATE INDEX IF NOT EXISTS eco_cost_idx ON ecount.cost (cost_type, sales_team, brand_name);

-- ============================================================
-- google
-- ============================================================

CREATE TABLE IF NOT EXISTS google.campaign (
    campaign_id TEXT NOT NULL
  , campaign_name TEXT
  , customer_id TEXT NOT NULL
  , campaign_type TEXT
  , campaign_status TEXT
  , bidding_strategy TEXT
  , campaign_budget INTEGER
  , impression_count_30d INTEGER
  , click_count_30d INTEGER
  , ad_cost_30d INTEGER
  , created_at TIMESTAMP
  , PRIMARY KEY (customer_id, campaign_id)
);

CREATE TABLE IF NOT EXISTS google.adgroup (
    adgroup_id TEXT NOT NULL
  , adgroup_name TEXT
  , customer_id TEXT NOT NULL
  , campaign_id TEXT NOT NULL
  , adgroup_type TEXT
  , adgroup_status TEXT
  , target_cpa INTEGER
  , impression_count_30d INTEGER
  , click_count_30d INTEGER
  , ad_cost_30d INTEGER
  , PRIMARY KEY (customer_id, adgroup_id)
);
CREATE INDEX IF NOT EXISTS ggl_adgroup_idx ON google.adgroup (customer_id, campaign_id);

CREATE TABLE IF NOT EXISTS google.ad (
    ad_id TEXT NOT NULL
  , ad_name TEXT
  , customer_id TEXT NOT NULL
  , campaign_id TEXT NOT NULL
  , adgroup_id TEXT NOT NULL
  , ad_type TEXT
  , ad_status TEXT
  , impression_count_30d INTEGER
  , click_count_30d INTEGER
  , ad_cost_30d INTEGER
  , PRIMARY KEY (customer_id, ad_id)
);
CREATE INDEX IF NOT EXISTS ggl_ad_idx ON google.ad (customer_id, campaign_id, adgroup_id);

CREATE TABLE IF NOT EXISTS google.asset (
    asset_id TEXT NOT NULL
  , asset_name TEXT
  , customer_id TEXT NOT NULL
  , asset_type TEXT
  , asset_url TEXT
  , PRIMARY KEY (customer_id, asset_id)
);

CREATE TABLE IF NOT EXISTS google.insight (
    customer_id TEXT NOT NULL
  , campaign_id TEXT NOT NULL
  , adgroup_id TEXT NOT NULL
  , ad_id TEXT NOT NULL
  , device_type SMALLINT NOT NULL
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, customer_id, ad_id, device_type)
) PARTITION BY RANGE (ymd);

-- ============================================================
-- meta
-- ============================================================

CREATE TABLE IF NOT EXISTS meta.accounts (
    account_id TEXT NOT NULL
  , account_name TEXT
  , account_group TEXT
  , account_seq BIGINT
  , PRIMARY KEY (account_id)
);
CREATE INDEX IF NOT EXISTS met_accounts_idx ON meta.accounts (account_group);

CREATE TABLE IF NOT EXISTS meta.campaigns (
    campaign_id TEXT NOT NULL
  , campaign_name TEXT
  , account_id TEXT NOT NULL
  , objective TEXT
  , effective_status TEXT
  , created_at TIMESTAMP
  , PRIMARY KEY (account_id, campaign_id)
);

CREATE TABLE IF NOT EXISTS meta.adsets (
    adset_id TEXT NOT NULL
  , adset_name TEXT
  , account_id TEXT NOT NULL
  , campaign_id TEXT
  , effective_status TEXT
  , daily_budget INTEGER
  , created_at TIMESTAMP
  , PRIMARY KEY (account_id, adset_id)
);
CREATE INDEX IF NOT EXISTS met_adsets_idx ON meta.adsets (account_id, campaign_id);

CREATE TABLE IF NOT EXISTS meta.ads (
    ad_id TEXT NOT NULL
  , ad_name TEXT
  , account_id TEXT NOT NULL
  , campaign_id TEXT
  , adset_id TEXT
  , effective_status TEXT
  , created_at TIMESTAMP
  , PRIMARY KEY (account_id, ad_id)
);
CREATE INDEX IF NOT EXISTS met_ads_idx ON meta.ads (account_id, campaign_id, adset_id);

CREATE TABLE IF NOT EXISTS meta.insights (
    account_id TEXT NOT NULL
  , campaign_id TEXT NOT NULL
  , adset_id TEXT NOT NULL
  , ad_id TEXT NOT NULL
  , impression_count INTEGER
  , reach_count INTEGER
  , click_count INTEGER
  , link_click_count INTEGER
  , ad_cost INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, account_id, ad_id)
) PARTITION BY RANGE (ymd);

-- ============================================================
-- naver
-- ============================================================

CREATE TABLE IF NOT EXISTS naver.category (
    category_id BIGINT NOT NULL
  , category_name TEXT NOT NULL
  , category_id1 BIGINT
  , category_id2 BIGINT
  , category_id3 BIGINT
  , category_id4 BIGINT
  , category_name1 TEXT
  , category_name2 TEXT
  , category_name3 TEXT
  , category_name4 TEXT
  , depth BIGINT NOT NULL
  , PRIMARY KEY (category_id)
);
CREATE INDEX IF NOT EXISTS nvr_category_idx ON naver.category (depth);
CREATE INDEX IF NOT EXISTS nvr_category2_idx ON naver.category (category_id1, category_id2, category_id3);

CREATE TABLE IF NOT EXISTS naver.keyword (
    keyword TEXT NOT NULL
  , keyword_group TEXT NOT NULL
  , keyword_seq BIGINT
  , keyword_type TEXT
  , target_page INTEGER
  , page_unit_ad INTEGER
  , page_unit_shop INTEGER
  , is_deleted BOOLEAN
  , updated_at TIMESTAMP
  , deleted_at TIMESTAMP
  , PRIMARY KEY (keyword, keyword_group)
);
CREATE INDEX IF NOT EXISTS nvr_keyword_idx ON naver.keyword (keyword_group);

CREATE TABLE IF NOT EXISTS naver.kwd_pid (
    keyword TEXT NOT NULL
  , product_id BIGINT NOT NULL
  , PRIMARY KEY (keyword, product_id)
);

CREATE TABLE IF NOT EXISTS naver.product (
    id BIGINT NOT NULL
  , product_id BIGINT
  , product_type SMALLINT
  , product_name TEXT
  , category_id BIGINT
  , full_category_name TEXT
  , mall_name TEXT
  , brand_name TEXT
  , sales_price INTEGER
  , updated_at TIMESTAMP
  , PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS nvr_product_idx ON naver.product (product_type);

CREATE TABLE IF NOT EXISTS naver.rank (
    keyword TEXT NOT NULL
  , id BIGINT NOT NULL
  , product_id BIGINT
  , product_type SMALLINT
  , display_rank SMALLINT NOT NULL
  , created_at TIMESTAMP NOT NULL
  , PRIMARY KEY (created_at, keyword, id)
) PARTITION BY RANGE (created_at);

-- ============================================================
-- relation
-- ============================================================

CREATE TABLE IF NOT EXISTS relation.cje_itm_to_eco_cod (
    item_code TEXT NOT NULL
  , validate_date DATE NOT NULL
  , product_code TEXT NOT NULL
  , PRIMARY KEY (item_code, validate_date, product_code)
);

CREATE TABLE IF NOT EXISTS relation.cpg_opt_to_eco_cod (
    option_id BIGINT NOT NULL
  , product_code TEXT NOT NULL
  , product_quantity INTEGER NOT NULL
  , PRIMARY KEY (option_id, product_code)
);

CREATE TABLE IF NOT EXISTS relation.cpg_opt_to_sbn_ids (
    option_id BIGINT NOT NULL
  , bundle_option_ids TEXT NOT NULL
  , PRIMARY KEY (option_id)
);

CREATE TABLE IF NOT EXISTS relation.ad_id_to_cat_id (
    ad_id TEXT NOT NULL
  , ad_type INTEGER NOT NULL
  , platform_name TEXT NOT NULL
  , category_id TEXT NOT NULL
  , PRIMARY KEY (ad_id, ad_type, platform_name)
);
CREATE INDEX IF NOT EXISTS rel_ad_id_to_cat_id_idx ON relation.ad_id_to_cat_id (platform_name, ad_type);

CREATE TABLE IF NOT EXISTS relation.smt_opt_to_sbn_ids (
    option_id BIGINT NOT NULL
  , bundle_option_ids TEXT NOT NULL
  , PRIMARY KEY (option_id)
);

CREATE TABLE IF NOT EXISTS relation.smt_prd_to_cat_id (
    product_id BIGINT NOT NULL
  , category_id TEXT NOT NULL
  , PRIMARY KEY (product_id, category_id)
);

-- ============================================================
-- sabangnet
-- ============================================================

CREATE TABLE IF NOT EXISTS sabangnet.account (
    account_no BIGINT NOT NULL
  , shop_id TEXT NOT NULL
  , shop_name TEXT
  , shop_group TEXT
  , userid TEXT
  , status BOOLEAN
  , commission_rate DOUBLE PRECISION
  , PRIMARY KEY (account_no)
);
CREATE INDEX IF NOT EXISTS sbn_account_idx ON sabangnet.account (shop_group, shop_name);

CREATE TABLE IF NOT EXISTS sabangnet.add_product (
    group_id TEXT NOT NULL
  , group_name TEXT
  , shop_id TEXT
  , option_seq INTEGER NOT NULL
  , option_id TEXT NOT NULL
  , option_name TEXT
  , sales_price INTEGER
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
  , PRIMARY KEY (group_id, option_seq)
);
CREATE INDEX IF NOT EXISTS sbn_add_product_idx ON sabangnet.add_product (shop_id, group_id);

CREATE TABLE IF NOT EXISTS sabangnet.delivery (
    delivery_group TEXT NOT NULL
  , min_unit INTEGER NOT NULL
  , coolant_cost INTEGER
  , label_cost INTEGER
  , wrap_cost INTEGER
  , box_cost INTEGER
  , delivery_fee INTEGER
  , n_arrival_fee INTEGER
  , n_arrival_add INTEGER
  , PRIMARY KEY (delivery_group, min_unit)
);

CREATE TABLE IF NOT EXISTS sabangnet.mapping_id (
    product_id_shop TEXT NOT NULL
  , product_id TEXT NOT NULL
  , account_no BIGINT NOT NULL
  , shop_id TEXT
  , product_name TEXT
  , sales_price INTEGER
  , mapping_count INTEGER
  , PRIMARY KEY (account_no, product_id_shop)
);
CREATE INDEX IF NOT EXISTS sbn_mapping_id_idx ON sabangnet.mapping_id (shop_id, account_no);

CREATE TABLE IF NOT EXISTS sabangnet.mapping_name (
    product_id_shop TEXT NOT NULL
  , option_id TEXT NOT NULL
  , shop_id TEXT NOT NULL
  , product_name TEXT
  , option_name TEXT
  , sku_seq INTEGER NOT NULL
  , sku_name TEXT
  , register_dt TIMESTAMP
  , PRIMARY KEY (shop_id, product_id_shop, sku_seq)
);
CREATE INDEX IF NOT EXISTS sbn_mapping_name_idx ON sabangnet.mapping_name (shop_id, product_id_shop);

CREATE TABLE IF NOT EXISTS sabangnet.model (
    category_id TEXT NOT NULL
  , category_seq BIGINT
  , model_code TEXT
  , model_id TEXT
  , product_id TEXT
  , brand_name TEXT
  , category_name1 TEXT
  , category_name2 TEXT
  , category_name3 TEXT
  , category_name4 TEXT
  , color TEXT
  , product_name TEXT
  , unit_name TEXT
  , unit_scale INTEGER
  , maker_name TEXT
  , sales_team TEXT
  , delivery_group TEXT
  , delivery_fee INTEGER
  , org_price INTEGER
  , wrap_price INTEGER
  , extra_cost INTEGER
);
CREATE INDEX IF NOT EXISTS sbn_model_idx ON sabangnet.model (sales_team, brand_name, category_name1);

CREATE TABLE IF NOT EXISTS sabangnet.option (
    option_id TEXT NOT NULL
  , barcode BIGINT
  , option_group TEXT
  , option_name TEXT
  , bundle_option_ids TEXT
  , option_status INTEGER
  , option_type INTEGER
  , option_quantity INTEGER
  , option_price INTEGER
  , register_dt TIMESTAMP
  , PRIMARY KEY (option_id)
);

CREATE TABLE IF NOT EXISTS sabangnet.order (
    order_seq BIGINT NOT NULL
  , order_seq_org BIGINT
  , order_id TEXT
  , order_id_dup TEXT
  , account_no BIGINT
  , option_id TEXT
  , product_id_shop TEXT
  , order_quantity INTEGER
  , sku_quantity INTEGER
  , payment_amount INTEGER
  , order_amount INTEGER
  , order_dt TIMESTAMP NOT NULL
  , register_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (order_dt, order_seq)
) PARTITION BY RANGE (order_dt);
CREATE INDEX IF NOT EXISTS sbn_order_idx ON sabangnet.order (account_no);

CREATE TABLE IF NOT EXISTS sabangnet.order_option (
    option_id TEXT NOT NULL
  , product_id_shop TEXT NOT NULL
  , account_no BIGINT NOT NULL
  , model_code TEXT
  , model_id TEXT
  , product_name TEXT
  , product_name_shop TEXT
  , product_name_abbr TEXT
  , option_name TEXT
  , option_name_shop TEXT
  , option_name_abbr TEXT
  , sales_price INTEGER
  , order_id TEXT
  , order_date DATE
  , PRIMARY KEY (account_no, product_id_shop, option_id)
);

CREATE TABLE IF NOT EXISTS sabangnet.order_invoice (
    order_seq BIGINT NOT NULL
  , account_no BIGINT
  , invoice_no TEXT NOT NULL
  , delivery_company TEXT
  , order_status_div INTEGER
  , order_status INTEGER
  , order_dt TIMESTAMP NOT NULL
  , invoice_date DATE
  , PRIMARY KEY (order_dt, order_seq)
) PARTITION BY RANGE (order_dt);
CREATE INDEX IF NOT EXISTS sbn_order_invoice_idx ON sabangnet.order_invoice (account_no);

CREATE TABLE IF NOT EXISTS sabangnet.order_dispatch (
    order_seq BIGINT NOT NULL
  , order_id TEXT
  , account_no BIGINT
  , option_id TEXT
  , sku_quantity INTEGER
  , orderer_name TEXT
  , receiver_name TEXT
  , zipcode TEXT
  , address TEXT
  , phone1 TEXT
  , phone2 TEXT
  , delivery_message TEXT
  , box_type TEXT
  , delivery_type TEXT
  , register_dt TIMESTAMP NOT NULL
  , order_dt TIMESTAMP
  , PRIMARY KEY (register_dt, order_seq)
) PARTITION BY RANGE (register_dt);

CREATE TABLE IF NOT EXISTS sabangnet.order_status (
    order_id TEXT NOT NULL
  , product_order_id TEXT
  , shop_name TEXT
  , order_status TEXT NOT NULL
  , order_date DATE NOT NULL
  , PRIMARY KEY (order_date, order_id, order_status)
) PARTITION BY RANGE (order_date);

CREATE TABLE IF NOT EXISTS sabangnet.product (
    product_id TEXT NOT NULL
  , model_code TEXT
  , model_id TEXT
  , product_name TEXT
  , product_keyword TEXT
  , brand_name TEXT
  , maker_name TEXT
  , logistics_service TEXT
  , product_status INTEGER
  , manufacture_year INTEGER
  , sales_price INTEGER
  , org_price INTEGER
  , image_file TEXT
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
  , PRIMARY KEY (product_id)
);
CREATE INDEX IF NOT EXISTS sbn_product_idx ON sabangnet.product (brand_name);

CREATE TABLE IF NOT EXISTS sabangnet.shop (
    shop_seq BIGINT NOT NULL
  , shop_id TEXT NOT NULL
  , shop_alias TEXT
  , shop_name TEXT
  , corp_name TEXT
  , shop_group TEXT
  , shop_url TEXT
  , scm_url TEXT
  , commission_rate DOUBLE PRECISION
  , PRIMARY KEY (shop_id)
);
CREATE INDEX IF NOT EXISTS sbn_shop_idx ON sabangnet.shop (shop_group);

-- ============================================================
-- searchad
-- ============================================================

CREATE TABLE IF NOT EXISTS searchad.account (
    customer_id BIGINT NOT NULL
  , account_name TEXT
  , account_type TEXT
  , account_seq BIGINT
  , PRIMARY KEY (customer_id)
);
CREATE INDEX IF NOT EXISTS sad_account_idx ON searchad.account (account_type);

CREATE TABLE IF NOT EXISTS searchad.campaign (
    campaign_id TEXT NOT NULL
  , campaign_name TEXT
  , campaign_type INTEGER
  , customer_id BIGINT NOT NULL
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
  , PRIMARY KEY (campaign_id)
);
CREATE INDEX IF NOT EXISTS sad_campaign_idx ON searchad.campaign (customer_id);

CREATE TABLE IF NOT EXISTS searchad.adgroup (
    adgroup_id TEXT NOT NULL
  , campaign_id TEXT NOT NULL
  , adgroup_name TEXT
  , adgroup_type INTEGER
  , customer_id BIGINT NOT NULL
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
  , bid_amount INTEGER
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
  , PRIMARY KEY (adgroup_id)
);
CREATE INDEX IF NOT EXISTS sad_adgroup_idx ON searchad.adgroup (customer_id, campaign_id);

CREATE TABLE IF NOT EXISTS searchad.ad (
    ad_id TEXT NOT NULL
  , adgroup_id TEXT NOT NULL
  , ad_type INTEGER
  , customer_id BIGINT NOT NULL
  , title TEXT
  , description TEXT
  , landing_url_pc TEXT
  , landing_url_mobile TEXT
  , nv_mid BIGINT
  , product_id BIGINT
  , category_id INTEGER
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
  , bid_amount INTEGER
  , sales_price INTEGER
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
  , PRIMARY KEY (ad_id)
);
CREATE INDEX IF NOT EXISTS sad_ad_idx ON searchad.ad (customer_id, adgroup_id);

CREATE TABLE IF NOT EXISTS searchad.media (
    media_code BIGINT NOT NULL
  , media_name TEXT
  , media_type INTEGER
  , group_id INTEGER
  , PRIMARY KEY (media_type, media_code)
);
CREATE INDEX IF NOT EXISTS sad_media_idx ON searchad.media (group_id);

CREATE TABLE IF NOT EXISTS searchad.report (
    ad_id TEXT NOT NULL
  , customer_id BIGINT NOT NULL
  , media_code BIGINT NOT NULL
  , pc_mobile_type SMALLINT NOT NULL
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , ad_rank_sum INTEGER
  , conv_count INTEGER
  , direct_conv_count INTEGER
  , conv_amount INTEGER
  , direct_conv_amount INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, customer_id, ad_id, media_code, pc_mobile_type)
) PARTITION BY RANGE (ymd);

CREATE TABLE IF NOT EXISTS searchad.contract (
    contract_id TEXT NOT NULL
  , adgroup_id TEXT NOT NULL
  , customer_id BIGINT NOT NULL
  , contract_name TEXT
  , contract_type INTEGER
  , contract_status SMALLINT
  , contract_amount INTEGER
  , refund_amount INTEGER
  , contract_qc INTEGER
  , keyword_qc INTEGER
  , register_dt TIMESTAMP
  , edit_dt TIMESTAMP
  , contract_start_date DATE
  , contract_end_date DATE NOT NULL
  , exposure_start_date DATE
  , exposure_end_date DATE
  , cancel_date DATE
  , PRIMARY KEY (contract_end_date, contract_id)
) PARTITION BY RANGE (contract_end_date);

CREATE TABLE IF NOT EXISTS searchad.rank (
    keyword TEXT NOT NULL
  , id BIGINT NOT NULL
  , display_rank SMALLINT NOT NULL
  , created_at TIMESTAMP NOT NULL
  , PRIMARY KEY (created_at, keyword, id)
) PARTITION BY RANGE (created_at);

-- ============================================================
-- searchad_gfa
-- ============================================================

CREATE TABLE IF NOT EXISTS searchad_gfa.report (
    campaign_no BIGINT NOT NULL
  , adset_no BIGINT NOT NULL
  , creative_no BIGINT NOT NULL
  , account_no BIGINT NOT NULL
  , reach_count INTEGER
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , conv_count INTEGER
  , conv_amount INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, account_no, creative_no)
) PARTITION BY RANGE (ymd);
CREATE INDEX IF NOT EXISTS sgf_report_idx ON searchad_gfa.report (account_no, campaign_no);

-- ============================================================
-- smartstore
-- ============================================================

CREATE TABLE IF NOT EXISTS smartstore.channel (
    channel_seq BIGINT NOT NULL
  , channel_name TEXT
  , brand_name TEXT
  , brand_seq BIGINT
  , sales_team TEXT
  , mall_url TEXT
  , order_date DATE
  , PRIMARY KEY (channel_seq)
);

CREATE TABLE IF NOT EXISTS smartstore.product (
    product_id BIGINT NOT NULL
  , product_no BIGINT NOT NULL
  , catalog_id BIGINT
  , channel_seq BIGINT NOT NULL
  , product_name TEXT
  , management_code TEXT
  , model_name TEXT
  , brand_name TEXT
  , category_id INTEGER
  , status_type TEXT
  , display_type TEXT
  , tags TEXT
  , price INTEGER
  , sales_price INTEGER
  , delivery_type INTEGER
  , delivery_fee INTEGER
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
  , PRIMARY KEY (product_id)
);
CREATE INDEX IF NOT EXISTS smt_product_idx ON smartstore.product (channel_seq);

CREATE TABLE IF NOT EXISTS smartstore.option (
    option_id BIGINT NOT NULL
  , product_id BIGINT NOT NULL
  , channel_seq BIGINT NOT NULL
  , product_type SMALLINT
  , option_group1 TEXT
  , option_name1 TEXT
  , option_group2 TEXT
  , option_name2 TEXT
  , option_group3 TEXT
  , option_name3 TEXT
  , management_code TEXT
  , usable BOOLEAN
  , option_price INTEGER
  , stock_quantity INTEGER
  , register_order INTEGER
  , PRIMARY KEY (option_id)
);
CREATE INDEX IF NOT EXISTS smt_option_idx ON smartstore.option (channel_seq, product_id);

CREATE TABLE IF NOT EXISTS smartstore.order (
    order_id BIGINT NOT NULL
  , channel_seq BIGINT NOT NULL
  , orderer_no BIGINT
  , orderer_id BIGINT
  , payment_location INTEGER
  , order_dt TIMESTAMP
  , payment_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (payment_dt, order_id)
) PARTITION BY RANGE (payment_dt);
CREATE INDEX IF NOT EXISTS smt_order_idx ON smartstore.order (channel_seq);

CREATE TABLE IF NOT EXISTS smartstore.order_delivery (
    product_order_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , channel_seq BIGINT NOT NULL
  , invoice_no TEXT NOT NULL
  , delivery_company TEXT
  , delivery_method INTEGER
  , zip_code TEXT
  , latitude TEXT
  , longitude TEXT
  , pickup_dt TIMESTAMP
  , send_dt TIMESTAMP
  , payment_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (payment_dt, product_order_id)
) PARTITION BY RANGE (payment_dt);

CREATE TABLE IF NOT EXISTS smartstore.order_detail (
    product_order_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , channel_seq BIGINT NOT NULL
  , product_id BIGINT NOT NULL
  , option_id BIGINT NOT NULL
  , product_type SMALLINT
  , delivery_type INTEGER
  , delivery_tag_type INTEGER
  , inflow_path TEXT
  , inflow_path_add TEXT
  , order_quantity INTEGER
  , unit_price INTEGER
  , option_price INTEGER
  , discount_amount INTEGER
  , seller_discount_amount INTEGER
  , supply_amount INTEGER
  , delivery_fee INTEGER
  , payment_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (payment_dt, product_order_id)
) PARTITION BY RANGE (payment_dt);

CREATE TABLE IF NOT EXISTS smartstore.order_option (
    product_id BIGINT NOT NULL
  , option_id BIGINT NOT NULL
  , channel_seq BIGINT NOT NULL
  , seller_product_code TEXT
  , seller_option_code TEXT
  , product_type SMALLINT
  , product_name TEXT
  , option_name TEXT
  , sales_price INTEGER
  , option_price INTEGER
  , update_date DATE
  , PRIMARY KEY (option_id, channel_seq)
);
CREATE INDEX IF NOT EXISTS smt_order_option_idx ON smartstore.order_option (channel_seq);

CREATE TABLE IF NOT EXISTS smartstore.order_status (
    product_order_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , channel_seq BIGINT NOT NULL
  , order_status SMALLINT NOT NULL
  , payment_dt TIMESTAMP NOT NULL
  , updated_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (payment_dt, product_order_id, order_status)
) PARTITION BY RANGE (payment_dt);

-- ============================================================
-- ss_hcenter  (smartstore HCenter API)
-- ============================================================

CREATE TABLE IF NOT EXISTS ss_hcenter.category (
    seq BIGINT NOT NULL
  , id BIGINT NOT NULL
  , group TEXT NOT NULL
  , name TEXT NOT NULL
  , target_id BIGINT NOT NULL
  , target_keyword TEXT
  , target_depth BIGINT NOT NULL
  , PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS ss_hcenter.mall (
    mall_seq BIGINT NOT NULL
  , mall_name TEXT NOT NULL
  , mall_group TEXT NOT NULL
  , mall_url TEXT NOT NULL
  , sales_date DATE
  , PRIMARY KEY (mall_seq)
);

CREATE TABLE IF NOT EXISTS ss_hcenter.pid_cid (
    product_id BIGINT NOT NULL
  , catalog_id BIGINT NOT NULL
  , created_at TIMESTAMP NOT NULL
  , PRIMARY KEY (created_at, product_id)
) PARTITION BY RANGE (created_at);

-- ss_hcenter.pageview: page_id는 집계 기준 (device_type/product_id 등 추상 식별자)
CREATE TABLE IF NOT EXISTS ss_hcenter.pageview (
    mall_seq BIGINT NOT NULL
  , page_id BIGINT NOT NULL
  , page_click INTEGER
  , user_click INTEGER
  , time_on_site INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, mall_seq, page_id)
) PARTITION BY RANGE (ymd);

CREATE TABLE IF NOT EXISTS ss_hcenter.price (
    product_id BIGINT NOT NULL
  , mall_seq BIGINT
  , category_id INTEGER
  , sales_price INTEGER
  , created_at TIMESTAMP NOT NULL
  , PRIMARY KEY (created_at, product_id)
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS ssh_price_idx ON ss_hcenter.price (mall_seq);

CREATE TABLE IF NOT EXISTS ss_hcenter.product (
    product_id BIGINT NOT NULL
  , mall_seq BIGINT
  , category_id INTEGER
  , category_id3 INTEGER
  , product_name TEXT
  , sales_price INTEGER
  , register_date DATE
  , update_date DATE
  , PRIMARY KEY (product_id)
);
CREATE INDEX IF NOT EXISTS ssh_product_idx ON ss_hcenter.product (mall_seq);

CREATE TABLE IF NOT EXISTS ss_hcenter.sales (
    product_id BIGINT NOT NULL
  , mall_seq BIGINT
  , category_id3 INTEGER
  , click_count INTEGER
  , payment_count INTEGER
  , payment_amount INTEGER
  , payment_date DATE NOT NULL
  , PRIMARY KEY (payment_date, product_id)
) PARTITION BY RANGE (payment_date);

CREATE TABLE IF NOT EXISTS ss_hcenter.stock (
    product_id BIGINT NOT NULL
  , price INTEGER
  , sales_price INTEGER
  , stock_quantity INTEGER
  , review_count INTEGER
  , review_score DOUBLE PRECISION
  , soldout BOOLEAN
  , created_at TIMESTAMP NOT NULL
  , PRIMARY KEY (created_at, product_id)
) PARTITION BY RANGE (created_at);

-- ============================================================
-- ss_bizdata
-- ============================================================

CREATE TABLE IF NOT EXISTS ss_bizdata.marketing_channel (
    channel_seq BIGINT NOT NULL
  , device_category TEXT NOT NULL
  , nt_source TEXT NOT NULL
  , nt_medium TEXT NOT NULL
  , nt_detail TEXT NOT NULL
  , nt_keyword TEXT NOT NULL
  , num_users INTEGER
  , num_interactions INTEGER
  , page_view INTEGER
  , num_purchases INTEGER
  , payment_amount INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, channel_seq, device_category, nt_source, nt_medium, nt_detail, nt_keyword)
) PARTITION BY RANGE (ymd);
CREATE INDEX IF NOT EXISTS ssb_marketing_channel_idx ON ss_bizdata.marketing_channel (channel_seq, device_category, nt_source, nt_medium);

