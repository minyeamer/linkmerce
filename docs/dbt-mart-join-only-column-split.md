# dbt mart join-only column split

이 문서는 mart incremental 전환 전에, 최종 분석 테이블에서 분리 가능한 열을 엄격하게 추린 메모다.

기준:

- 마지막 단계의 join 또는 lookup으로만 붙는다.
- 해당 열이 자기 자신 외 다른 최종 열의 계산, group by, partition, filter, split 보정에 전혀 쓰이지 않는다.
- `COALESCE(...)` 형태여도 자기 자신만 만드는 용도면 분리 후보로 본다.
- 상품명, 카테고리명, 캠페인명 같은 표시용 속성을 우선 대상으로 본다.
- 숫자 열이어도 단순 참조 속성이면 후보가 될 수 있지만, 우선순위는 표시용 문자열보다 낮다.

범위:

- 기준 검토는 `dbt_postgres` 모델을 기준으로 했고, 현재 `dbt_bigquery`도 동일한 구조로 따라가면 된다.

## 분리 후보가 명확한 테이블

### `analytics.product_sales`

분리 후보:
`shop_group`, `shop_name`, `category_seq`, `brand_name`, `category_name1`, `category_name2`, `category_name3`, `category_unit3`, `category_name4`, `sales_team`, `product_name`, `model_code`, `org_price`

메모:
`main_product`, `sabangnet.model`, `analytics.cost`에서 마지막에 붙는 속성들이다. 이 열들은 `margin_amount`, `profit`, `unit_quantity` 같은 계산 결과를 만드는 데 다시 쓰이지 않는다.

### `coupang_ads.report`

분리 후보:
`vendor_name`, `campaign_name`, `campaign_type`, `adgroup_name`, `goal_type`, `title`, `description`, `category_seq`, `brand_name`, `category_name1`, `category_name2`, `category_name3`, `sales_team`, `is_active`, `is_deleted`, `placement_group`

메모:
성과 값은 `impression_count`, `click_count`, `ad_cost`, `conv_count`, `direct_conv_count`, `conv_amount`, `direct_conv_amount`, `ymd` 쪽에 모여 있다. 나머지는 사실상 광고/상품 설명 속성이다.

### `google_ads.report`

분리 후보:
`campaign_name`, `campaign_type`, `adgroup_name`, `adgroup_type`, `ad_name`, `ad_type`, `device_type`, `category_seq`, `brand_name`, `category_name1`, `category_name2`, `category_name3`, `sales_team`, `ad_status`

메모:
`renewal`과 `ad_group_ad`에서 들어오는 속성들이며, 성과 열 계산에 다시 참여하지 않는다.

### `meta_ads.report`

분리 후보:
`account_name`, `campaign_name`, `adset_name`, `ad_name`, `objective`, `category_seq`, `brand_name`, `category_name1`, `category_name2`, `category_name3`, `sales_team`, `effective_status`

메모:
성과 값은 `impression_count`, `reach_count`, `click_count`, `link_click_count`, `ad_cost`, `ymd`다. 나머지는 lookup 성격이 강하다.

### `searchad.report_detail`

분리 후보:
`account_name`, `campaign_name`, `campaign_type`, `adgroup_name`, `adgroup_type`, `ad_type`, `title`, `description`, `category_seq`, `brand_name`, `category_name1`, `category_name2`, `category_name3`, `sales_team`, `is_enabled`, `is_deleted`, `pc_mobile_type`

메모:
카테고리 분할 보정은 `category_id`, `ad_cost`, 전환/노출/클릭 값에만 영향을 주고, 위 열들은 최종 표시용으로만 남는다.

### `smartstore.product_order`

분리 후보:
`sales_team`, `channel_name`, `order_status`, `product_type`, `delivery_type`, `product_name`, `option_name`, `category_name1`, `category_name2`, `category_name3`, `category_name4`, `payment_location`, `inflow_path`, `inflow_path_add`, `product_url`, `option_name1`, `option_name2`, `option_name3`

메모:
`payment_amount`, `supply_amount`, `order_count`, `first_payment_date`, `last_payment_date` 계산에는 다시 쓰이지 않는 표시용 속성들이다.

### `sabangnet.product_undefined`

분리 후보:
`shop_name`, `product_name`, `option_name`

메모:
이 모델은 QA용이라 컬럼 수가 많지 않다. 그래도 `undefined` 판정과 `order_seq` 계산에는 위 세 열이 필요 없다.

### `ss_hcenter.price_change`

분리 후보:
`mall_name`, `category_seq`, `category_group`, `category_name`, `category_name2`, `category_name3`, `product_name`, `is_refurb`, `product_url`

메모:
가격 변동 계산은 `sales_price`, `lag_price`, `changed_date`, `diff_price`, `diff_price_rate`에 집중되어 있다. 나머지는 `mall_product`에서 온 표시용 속성이다.

### `ss_hcenter.pivot_sales`

분리 후보:
`mall_type`, `mall_name`, `category_seq`, `category_group`, `category_name`, `category_name2`, `category_name3`, `product_name`, `mall_url`, `product_url`, `sales_price`, `register_date`, `sales_date`, `metric_name`, `dtype`

메모:
실제 pivot 값은 `product_id`, `metric_type`, `metric_day0`, `metric_day1`, `metric_week1`, `metric_month1`, `metric_year1`, `metric_year2`다. `mall_product`, `metric_type`에서 붙는 설명 속성은 분리 효과가 있다.

### `ss_hcenter.two_sales`

분리 후보:
`mall_name`, `category_seq`, `category_name`, `category_name2`, `category_name3`, `product_name`, `mall_url`, `product_url`, `register_date`, `sales_date`

메모:
비교 지표 계산은 left/right amount, count, total 계열에서 끝난다. 위 열들은 최종 표시용이다.

## 분리 후보가 애매하거나 없는 테이블

### `analytics.categorical_sales`

보류:
설명 열이 있긴 하지만, 이 모델은 여러 광고/주문 소스를 합친 뒤 `MIN(...)`, `GROUP BY`로 같이 굳혀진 구조다. 지금 단계에서는 join-only 속성만 떼어내기가 깔끔하지 않다.

### `analytics.stock_report`

보류:
표시용 열이 섞여 있지만 대부분이 최종 join으로만 붙는 구조가 아니라 중간 단계와 정렬 단계에 섞여 있다. `join-only 속성 분리`보다는 별도 stock dimension 설계가 맞다.

### `analytics.stock_uptime`

제외:
행 수가 작고 설명 열이 거의 없다.

### `ss_hcenter.mall_product`

제외:
이 테이블 자체가 상품 dimension 역할이다. 다른 fact에서 join해 쓰는 기준 테이블로 두는 편이 맞다.

### `ss_hcenter.mall_sales2`

보류:
설명 열이 많지만 `mall_seq`, `category_seq`, `category_group`이 중간 집계와 평균 계산에 함께 엮여 있다. 지금 기준으로는 join-only 분리 대상으로 보기 어렵다.

### `ss_hcenter.mall_pageview`

보류:
`mall_name`, 카테고리명, 상품명은 표시용이지만 `category_name`, `product_name`, `url`이 단순 lookup이 아니라 `CASE`로 가공되어 있다. 이 모델은 우선순위가 낮다.

### `ss_hcenter.pivot_range`

제외:
단일 범위 표시 테이블이라 분리 이점이 거의 없다.

## 우선순위

1. 바로 분리해도 되는 1순위:
`analytics.product_sales`, `coupang_ads.report`, `google_ads.report`, `meta_ads.report`, `searchad.report_detail`, `smartstore.product_order`

2. 다음 후보:
`sabangnet.product_undefined`, `ss_hcenter.price_change`, `ss_hcenter.pivot_sales`, `ss_hcenter.two_sales`

3. 이번 단계에서는 유지:
`analytics.categorical_sales`, `analytics.stock_report`, `analytics.stock_uptime`, `ss_hcenter.mall_product`, `ss_hcenter.mall_sales2`, `ss_hcenter.mall_pageview`, `ss_hcenter.pivot_range`

