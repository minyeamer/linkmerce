# dbt source column split candidates

이 문서는 mart incremental 전환 전에, 원천 테이블 기준으로 dimension/bridge 분리 대상을 다시 정리한 문서다.

## 기준

분리 대상은 "incremental 방식으로 전환될 mart 또는 그 upstream intermediate의 중간 계산에 영향을 주는 원천 테이블"에 한정한다.

포함 기준:

- 원천 컬럼이 중간 CTE의 join, group by, partition, order by, filter, split 비율, 금액/수량 계산에 쓰인다.
- 해당 컬럼이 바뀌면 최종 mart의 금액, 수량, 카테고리 귀속, 배송비, 광고비 배분, 정렬 순서, 집계 단위가 바뀔 수 있다.
- fact에 반복 저장하기보다 dimension 또는 bridge로 분리해 두고, incremental mart 계산 시 필요한 시점에 join하는 편이 낫다.

제외 기준:

- 최종 SELECT 직전에 상품명, 옵션명, 채널명, URL 같은 표시용 master 속성만 붙인다.
- 해당 컬럼이 자기 자신 외 다른 계산 결과에 영향을 주지 않는다.
- 예: `smartstore.product`는 `smartstore.product_order`의 마지막에 상품명/가격을 붙는 master 테이블이므로 이 기준에서는 우선 대상이 아니다.

검토 기준 모델:

- `analytics.product_sales`
- `analytics.categorical_sales`
- `analytics.stock_report`
- `smartstore.product_order`
- `searchad.report_detail`
- `coupang_ads.report`
- `google_ads.report`
- `meta_ads.report`
- `ss_hcenter.mall_product`
- `ss_hcenter.mall_sales2`
- `ss_hcenter.pivot_sales`
- `ss_hcenter.two_sales`
- `ss_hcenter.price_change`

## 분리 대상 원천 테이블

### `sabangnet.model`

영향 받는 모델:
`int_analytics.total_order`, `analytics.product_sales`, `int_analytics.marketing_cost`, `analytics.stock_report`, 광고 object/detail 계열, `ss_hcenter` 외 대부분의 상품/카테고리 귀속 로직

계산에 영향을 주는 컬럼:

- `product_id`: 주문 option/product를 Sabangnet 기준 상품으로 귀속하는 join key다.
- `category_id`: `total_order`, 광고 report, `categorical_sales`의 집계 기준으로 이어진다.
- `category_seq`: 카테고리 정렬, 대표 카테고리 선택, 일부 `DIV(category_seq, 100)` 계산에 쓰인다.
- `brand_name`: `analytics.cost`의 브랜드 비용을 카테고리에 귀속할 때 join 기준으로 쓰인다.
- `category_name2`: `marketing_cost`에서 대표 brand row를 고를 때 `category_name2 IS NULL` 필터에 쓰인다.
- `delivery_group`: `total_order`에서 `analytics.product_sales` 배송비 계산의 key로 넘어간다.
- `delivery_fee`: `total_order`에서 source 배송비 fallback으로 쓰이고, 이후 `product_sales.margin_amount`, `profit`에 영향을 준다.
- `unit_scale`: `analytics.product_sales.unit_quantity` 계산에 직접 쓰인다.
- `org_price`, `wrap_price`, `extra_cost`: `main_product.org_price` 계산 및 복합 주문 금액/배송비 split 비율에 쓰인다.
- `eflexs_item_code`: `analytics.stock_report`에서 CJ Eflexs 재고를 Sabangnet option으로 매핑한다.

분리 방향:
상품 표시용 dimension 하나로 단순 분리하기보다, 계산 용도별로 나누는 편이 좋다.

- `dim_sabangnet_category`: `category_id`, `category_seq`, `brand_name`, `category_name2`
- `dim_sabangnet_delivery_group`: `product_id`, `delivery_group`, `delivery_fee`
- `dim_sabangnet_cost_basis`: `product_id`, `org_price`, `wrap_price`, `extra_cost`, `unit_scale`
- `bridge_eflexs_item_to_option`: `eflexs_item_code`, `product_id`, derived `option_id`

### `sabangnet.delivery`

영향 받는 모델:
`analytics.product_sales`

계산에 영향을 주는 컬럼:

- `delivery_group`
- `min_unit`
- `coolant_cost`
- `label_cost`
- `wrap_cost`
- `box_cost`
- `delivery_fee`
- `n_arrival_fee`
- `n_arrival_add`

분리 방향:
`dim_delivery_fee_rule` 또는 `dim_sabangnet_delivery_rule`로 두는 것이 좋다. 이 테이블은 표시용 master가 아니라 배송비 산식 자체이므로 incremental 계산에서 반드시 별도 관리해야 한다.

### `relation.smt_opt_to_sbn_ids`

영향 받는 모델:
`int_analytics.total_order`, `int_analytics.sold_quantity`, `sabangnet.product_undefined`

계산에 영향을 주는 컬럼:

- `option_id`
- `bundle_option_ids`

분리 방향:
dimension보다는 bridge가 맞다.

- `bridge_smartstore_option_to_sabangnet_option`

`bundle_option_ids`는 수량 배수와 target option을 함께 담고 있어 `sku_quantity`, 주문 금액 split, sold quantity 계산에 직접 영향을 준다.

### `relation.cpg_opt_to_sbn_ids`

영향 받는 모델:
`int_analytics.total_order`, `int_analytics.sold_quantity`, `analytics.stock_report`, `coupang_ads.report`, `sabangnet.product_undefined`

계산에 영향을 주는 컬럼:

- `option_id`
- `bundle_option_ids`

분리 방향:
`bridge_coupang_option_to_sabangnet_option`으로 분리한다. Coupang 판매/재고/광고 모두 이 매핑을 통해 Sabangnet option/product 기준으로 바뀐다.

### `relation.ad_id_to_cat_id`

영향 받는 모델:
`searchad.report_detail`, `google_ads.report`, `meta_ads.report`, `analytics.categorical_sales`, `analytics.product_sales`

계산에 영향을 주는 컬럼:

- `platform_name`
- `ad_type`
- `ad_id`
- `category_id`

분리 방향:
`bridge_ad_to_category`로 분리한다. 광고 성과가 어느 category로 귀속되는지가 바뀌면 `categorical_sales`, `product_sales`의 광고비 귀속 결과가 바뀐다.

### `relation.smt_prd_to_cat_id`

영향 받는 모델:
`searchad.report_detail`

계산에 영향을 주는 컬럼:

- `product_id`
- `category_id`

분리 방향:
`bridge_smartstore_product_to_category`로 분리한다. Searchad 광고가 product_id 기준으로 들어올 때 category 귀속에 영향을 준다.

### `ecount.product`

영향 받는 모델:
`int_sabangnet.main_product`, `analytics.stock_report`, `int_analytics.total_order`, `analytics.product_sales`

계산에 영향을 주는 컬럼:

- `option_id`: 원가 및 재고 기준 key다.
- `org_price`: 복합 주문 split 비율, `supply_cost`, `total_cost`, `org_price`에 영향을 준다.
- `expiration_date`: `stock_report`의 lot 선택, 유통기한 정렬, `performance`, `expected_date` 판단에 영향을 준다.
- `product_code`: Ecount 재고 join, 누적 재고 계산, 정렬에 영향을 준다.
- `updated_at`: 재고 기준 기간 필터에 쓰인다.

분리 방향:
`dim_option_cost_basis`와 `dim_stock_lot`을 분리 후보로 본다. 단순 상품명 master가 아니라 원가/lot 계산 입력이다.

### `smartstore.channel`

영향 받는 모델:
`analytics.product_sales`, `analytics.stock_report`

계산에 영향을 주는 컬럼:

- `channel_seq`: Smartstore 주문을 channel 기준으로 통과시키는 join key다.
- `brand_name`: `product_sales`에서 Searchad 비용을 N배송/일반 배송으로 배분할 때 `smartstore_daily_brand`의 group by 및 join key로 쓰인다.
- `brand_seq`: `stock_report` 정렬 순서 계산에 쓰인다.

대상 아님:
`sales_team`, `mall_url`은 현재 기준에서는 표시용에 가깝다. `smartstore.product_order` 마지막 master join 용도로만 보면 분리 대상이 아니다.

분리 방향:
`dim_smartstore_channel_calc`처럼 계산용 속성만 얇게 분리한다.

### `analytics.cost`

영향 받는 모델:
`int_analytics.marketing_cost`, `analytics.product_sales`, `analytics.categorical_sales`

계산에 영향을 주는 컬럼:

- `id`: marketing cost를 광고성 주문 row로 붙일 때 `product_id_shop` key로 쓰인다.
- `brand_name`: `sabangnet.model`의 브랜드/카테고리로 비용을 귀속하는 join key다.
- `cost_type`: `product_sales.shop_name`, `categorical_sales.record_type`으로 이어진다.
- `cost`: 일자별 비용 계산과 광고비 row 금액이다.
- `start_date`, `end_date`: 비용을 일자별로 펼치는 range 계산에 쓰인다.
- `sales_team`, `name`: `product_sales`의 광고성 row 표시값으로도 쓰이지만, 비용 row 식별 및 귀속 결과와 붙어 움직인다.

분리 방향:
`fact_marketing_cost_schedule`와 `dim_marketing_cost_item`으로 나누는 방향이 좋다. 다만 `cost`, `start_date`, `end_date`는 dimension이 아니라 schedule/fact에 남겨야 한다.

### `ss_hcenter.category_group`

영향 받는 모델:
`ss_hcenter.mall_product`, downstream `mall_sales2`, `pivot_sales`, `two_sales`

계산에 영향을 주는 컬럼:

- `mapping_depth`
- `mapping_id`
- `mapping_keyword`
- `seq`
- `group_id`
- `brand_name`
- `group_name`

분리 방향:
`dim_ss_hcenter_category_mapping_rule`로 분리한다. 이 테이블은 `mall_product`의 category 귀속, `category_seq`, `category_group`, `category_name`, `total_ref`를 결정하고 downstream 집계 partition에 영향을 준다.

### `naver_shp.category`

영향 받는 모델:
`ss_hcenter.mall_product`, downstream `mall_sales2`, `pivot_sales`, `two_sales`

계산에 영향을 주는 컬럼:

- `category_id`
- `category_id2`
- `category_id3`
- `depth`

보조 출력 컬럼:
`category_name2`, `category_name3`

분리 방향:
`dim_naver_category_tree`로 분리한다. 단순 표시용 이름뿐 아니라 `category_id3 -> category_id2` fallback 매핑이 `mall_product` category 귀속에 쓰인다.

### `ss_hcenter.product`

영향 받는 모델:
`ss_hcenter.mall_product`, downstream `mall_sales2`, `pivot_sales`, `two_sales`, `price_change`

계산에 영향을 주는 컬럼:

- `product_id`
- `mall_seq`
- `category_id`
- `category_id3`
- `product_name`: keyword filter와 `category_group.mapping_keyword` 매칭에 쓰인다.
- `sales_price`: `mall_product` 및 downstream price/sales 기준값으로 이어진다.
- `register_date`
- `update_date`

분리 방향:
이 테이블은 `ss_hcenter`의 product dimension 원천이다. `smartstore.product`와 달리 `mall_product` 자체의 category 매칭/필터 계산에 직접 들어가므로 대상에 포함한다.

### `ss_hcenter.mall`

영향 받는 모델:
`ss_hcenter.mall_product`, downstream `mall_sales2`, `pivot_sales`, `two_sales`

계산에 영향을 주는 컬럼:

- `mall_seq`
- `mall_url`: `mall_type`, `product_url` 계산에 쓰인다.
- `mall_name`: `mall_highlight` 처리에 쓰인다.
- `mall_group`
- `sales_date`

분리 방향:
`dim_ss_hcenter_mall`로 분리한다. `mall_url`, `mall_name`은 표시용처럼 보이지만 현재 `mall_type`, `product_url`, highlight 표시 계산에 직접 쓰인다.

## 일부 컬럼만 계산 대상인 원천 테이블

### 광고 object source

대상 source:
`searchad.ad`, `searchad.campaign`, `searchad.adgroup`, `google_ads.ad`, `google_ads.campaign`, `google_ads.adgroup`, `meta_ads.ad`, `meta_ads.campaign`, `meta_ads.adset`

계산에 영향을 주는 컬럼:

- 광고 id 계열: `ad_id`, `campaign_id`, `adgroup_id`, `adset_id`
- 광고 유형 계열: `ad_type`, `campaign_type`, `adgroup_type`, `objective`
- 상태 계열 중 집계 대상 결정에 쓰이는 값: `ad_status`, `campaign_status`, `adgroup_status`, `effective_status`
- 생성일 계열: `created_at`

메모:
광고명(`campaign_name`, `ad_name`, `adset_name`)은 대부분 표시용이라 이 기준에서는 우선 대상이 아니다. 반면 유형/objective는 `categorical_sales.record_type`으로 이어져 집계 bucket에 영향을 준다.

### 광고 report source

대상 source:
`searchad.report_sad`, `searchad.report_gfa`, `google_ads.insight`, `meta_ads.insight`, `coupang_ads.report_pa`, `coupang_ads.report_nca`

판단:
이들은 dimension 분리 대상이라기보다 incremental fact 원천이다. metric, id, date는 fact에 남겨야 한다.

예:
`impression_count`, `click_count`, `ad_cost`, `conv_count`, `conv_amount`, `ymd` 등

## 명시적으로 대상이 아닌 원천 테이블

### `smartstore.product`

제외 이유:
현재 `smartstore.product_order`에서는 마지막 단계에 `product_name`, `sales_price`, `category_id`를 붙는 master join 성격이다. 이 source 자체가 중간 집계나 split 비율을 바꾸는 구조는 아니다.

### `smartstore.order_option`

제외 이유:
현재 `smartstore.product_order`에서는 `option_name`, `option_price`를 마지막에 붙는다. `option_name1~3` 파생은 표시용 가공이며 mart 계산 fact를 바꾸지 않는다.

### `coupang.vendor`, `meta_ads.account`, `searchad.account`

제외 이유:
현재는 주로 이름/계정명 표시용 lookup이다. metric 계산이나 귀속 기준을 바꾸는 핵심 source는 아니다.

### `ss_hcenter.price`, `ss_hcenter.sales`, `ss_hcenter.stock`

제외 이유:
이들은 dimension 분리 대상이 아니라 incremental fact 원천이다. price/sales/stock의 metric과 날짜는 mart fact에 남아야 한다.

## 우선순위

1. 먼저 분리할 계산 dimension/bridge:
`sabangnet.model`, `sabangnet.delivery`, `relation.smt_opt_to_sbn_ids`, `relation.cpg_opt_to_sbn_ids`, `relation.ad_id_to_cat_id`

2. 다음 분리 후보:
`ecount.product`, `smartstore.channel`, `analytics.cost`, `ss_hcenter.category_group`, `naver_shp.category`

3. `ss_hcenter` 전용 dimension 후보:
`ss_hcenter.product`, `ss_hcenter.mall`

4. 제외 유지:
`smartstore.product`, `smartstore.order_option`, 계정/이름만 공급하는 광고 account/vendor 계열

