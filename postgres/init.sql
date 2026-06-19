-- PostgreSQL 원천 테이블 초기화 스크립트
-- CREATE INDEX: <pfm>_<table>__<cluster>_idx
-- PARTITION BY RANGE: 시계열 필드 (일별/시간별 범위 지정)

-- ============================================================
-- 스키마 생성
-- ============================================================

CREATE SCHEMA IF NOT EXISTS core; -- cor
CREATE SCHEMA IF NOT EXISTS cj_eflexs; -- cje
CREATE SCHEMA IF NOT EXISTS cj_loisparcel; -- cjl
CREATE SCHEMA IF NOT EXISTS coupang; -- cpg
CREATE SCHEMA IF NOT EXISTS coupang_ads; -- cpa
CREATE SCHEMA IF NOT EXISTS coupang_rfm; -- cpr
CREATE SCHEMA IF NOT EXISTS ecount; -- eco
CREATE SCHEMA IF NOT EXISTS google_ads; -- ggl
CREATE SCHEMA IF NOT EXISTS meta_ads; -- met
CREATE SCHEMA IF NOT EXISTS naver_shp; -- nsh
CREATE SCHEMA IF NOT EXISTS relation; -- rel
CREATE SCHEMA IF NOT EXISTS sabangnet; -- sbn
CREATE SCHEMA IF NOT EXISTS searchad; -- sad
CREATE SCHEMA IF NOT EXISTS smartstore; -- smt
CREATE SCHEMA IF NOT EXISTS ss_hcenter; -- ssh

CREATE SCHEMA IF NOT EXISTS analytics; -- dbt/models/marts/**
CREATE SCHEMA IF NOT EXISTS transformed; -- dbt/models/intermediate/**
CREATE SCHEMA IF NOT EXISTS xfm_ads; -- dbt/models/intermediate/ads/**
CREATE SCHEMA IF NOT EXISTS xfm_sales; -- dbt/models/intermediate/sales/**

CREATE SCHEMA IF NOT EXISTS partman; -- pg_partman
CREATE SCHEMA IF NOT EXISTS test; -- pytest

-- ============================================================
-- parquet_io / pg_partman 확장 및 초기 파티션 설정
-- ============================================================

CREATE EXTENSION IF NOT EXISTS parquet_io;
CREATE EXTENSION IF NOT EXISTS pg_partman WITH SCHEMA partman;

-- ============================================================
-- core (공통 상품/비용)
-- ============================================================

-- [대표상품]
CREATE TABLE IF NOT EXISTS core.item (
    item_id TEXT NOT NULL -- 분류코드
  , item_seq BIGINT -- 순번
  , product_id TEXT -- 품목코드
  , team_name TEXT -- 영업팀
  , brand_name TEXT -- 브랜드
  , category_name1 TEXT -- 대분류
  , category_name2 TEXT -- 중분류
  , category_name3 TEXT -- 소분류
  , category_name4 TEXT -- 세분류
  , color TEXT -- 색상
  , product_name TEXT -- 상품명
  , unit_name TEXT -- 소분류단위
  , unit_scale INTEGER -- 소분류배율
  , maker_name TEXT -- 업체명
  , model_code TEXT -- 대표코드
  , model_id TEXT -- 식별코드
  , eflexs_item_code TEXT -- 풀필먼트코드
  , in_stock_yn BOOLEAN -- 자사판매여부
  , delivery_group TEXT -- 배송그룹
  , delivery_fee INTEGER -- 배송비
  , org_price INTEGER -- 원가
  , extra_cost INTEGER -- 부자재비
  , PRIMARY KEY (item_id)
);

-- [배송그룹]
CREATE TABLE IF NOT EXISTS core.delivery_group (
    delivery_group TEXT NOT NULL -- 배송그룹
  , min_unit INTEGER NOT NULL -- 최소구성
  , coolant_cost INTEGER -- 냉매
  , label_cost INTEGER -- 스티커
  , wrap_cost INTEGER -- 에어버블
  , box_cost INTEGER -- 택배박스
  , delivery_fee INTEGER -- 자체배송비
  , n_arrival_fee INTEGER -- N배송비
  , n_arrival_add INTEGER -- N배송비추가
  , PRIMARY KEY (delivery_group, min_unit)
);

-- [고정지출]
CREATE TABLE IF NOT EXISTS core.expense (
    expense_id BIGINT NOT NULL -- 순번
  , expense_type TEXT -- 구분
  , company_name TEXT -- 거래처명
  , description TEXT -- 내용
  , amount BIGINT -- 비용
  , ymd DATE NOT NULL -- 결제일
  , PRIMARY KEY (ymd, expense_id)
) PARTITION BY RANGE (ymd);

-- [기타매출]
CREATE TABLE IF NOT EXISTS core.extra_sales (
    product_id TEXT NOT NULL -- 품번코드
  , shop_id TEXT NOT NULL -- 쇼핑몰코드
  , sales_amount BIGINT -- 매출액
  , sales_date DATE NOT NULL -- 매출발생일
  , PRIMARY KEY (sales_date, product_id, shop_id)
) PARTITION BY RANGE (sales_date);

-- [운영비용]
CREATE TABLE IF NOT EXISTS core.opex (
    expense_id BIGINT NOT NULL -- 순번
  , expense_name TEXT -- 명칭
  , dept_id SMALLINT NOT NULL -- 부서ID
  , brand_id TEXT NOT NULL -- 연결브랜드ID
  , amount BIGINT -- 비용
  , start_date DATE -- 시작일
  , end_date DATE NOT NULL -- 종료일
  , PRIMARY KEY (end_date, expense_id)
) PARTITION BY RANGE (end_date);

-- [주문상태]
CREATE TABLE IF NOT EXISTS core.order_status (
    order_id TEXT NOT NULL -- 주문번호
  , product_order_id TEXT -- 상품주문번호
  , shop_name TEXT -- 쇼핑몰명
  , order_status SMALLINT NOT NULL -- 주문상태
  , order_date DATE NOT NULL -- 주문일시
  , PRIMARY KEY (order_date, order_id, order_status)
) PARTITION BY RANGE (order_date);

-- ============================================================
-- cj_eflexs (CJ대한통운 eFLEXs)
-- ============================================================

-- [CJ대한통운 eFLEXs 운송장상세]
CREATE TABLE IF NOT EXISTS cj_eflexs.invoice (
    invoice_no TEXT NOT NULL -- 운송장번호
  , package_no TEXT -- 묶음배송번호
  , order_id TEXT -- 주문번호
  , product_name TEXT -- 대표품목명
  , delivery_type TEXT -- 배송유형
  , package_type TEXT -- 정산분류
  , box_type TEXT -- 박스구분
  , sbs_box_type TEXT -- SBS박스구분
  , order_quantity INTEGER -- SKU건수
  , sku_quantity INTEGER -- 총품목수량
  , delivery_fee INTEGER -- 출고금액
  , shipping_cost INTEGER -- 도선운임
  , jeju_cost INTEGER -- 제주운임
  , extra_cost INTEGER -- 기타운임
  , service_cost INTEGER -- 서비스운임
  , box_cost INTEGER -- 부자재금액
  , pickup_date DATE NOT NULL -- 집하일자
  , PRIMARY KEY (pickup_date, invoice_no)
) PARTITION BY RANGE (pickup_date);

-- [CJ대한통운 eFLEXs 고객매출확인]
CREATE TABLE IF NOT EXISTS cj_eflexs.invoice_order (
    invoice_no TEXT NOT NULL -- 운송장번호
  , package_no TEXT -- 묶음배송번호
  , order_id TEXT NOT NULL -- 주문번호
  , delivery_fee INTEGER -- 정산금액
  , box_cost INTEGER -- 부자재금액
  , order_date DATE NOT NULL -- 주문일자
  , pickup_date DATE NOT NULL -- 집하일자
  , PRIMARY KEY (order_date, invoice_no, order_id)
) PARTITION BY RANGE (order_date);

-- [CJ대한통운 eFLEXs 상세재고조회]
CREATE TABLE IF NOT EXISTS cj_eflexs.stock (
    item_code TEXT NOT NULL -- 품목코드
  , barcode TEXT -- 품목바코드
  , customer_id BIGINT NOT NULL -- 고객ID
  , item_name TEXT -- 품목명
  , warehouse_code TEXT -- F/C
  , warehouse_name TEXT -- F/C명
  , zone_code TEXT -- 구역
  , location_name TEXT -- 로케이션
  , lot_no BIGINT -- 로트번호
  , total_quantity INTEGER -- 합계수량
  , usable_quantity INTEGER -- 가용수량
  , hold_quantity INTEGER -- 보류수량
  , process_quantity INTEGER -- 작업수량
  , remain_days INTEGER -- 잔여재고일
  , validate_date DATE -- 유효일자
  , inbound_date DATE -- 입고확정일자
  , updated_at TIMESTAMP NOT NULL -- 갱신일시
) PARTITION BY RANGE (updated_at);

-- ============================================================
-- cj_loisparcel (CJ대한통운 로이스파셀)
-- ============================================================

-- [CJ대한통운 로이스파셀 기업고객일별배송상세]
CREATE TABLE IF NOT EXISTS cj_loisparcel.invoice (
    invoice_no TEXT NOT NULL -- 운송장번호
  , order_id TEXT -- 주문번호
  , product_id TEXT -- 단품코드
  , product_name TEXT -- 품명
  , order_quantity INTEGER -- 수량
  , delivery_fee INTEGER -- 운임
  , shipper_name TEXT -- 송화인
  , receiver_name TEXT -- 받는분
  , zipcode TEXT -- 우편번호
  , address TEXT -- 주소
  , phone1 TEXT -- 전화번호
  , phone2 TEXT -- 휴대번호
  , delivery_message TEXT -- 특이사항(배송메시지)
  , box_type TEXT -- 박스타입
  , delivery_type TEXT -- 운임구분
  , order_status TEXT -- 예약구분
  , delivery_status TEXT -- 접수구분
  , register_date DATE NOT NULL -- 접수일자
  , pickup_date DATE -- 집화일자
  , delivery_date DATE -- 배송일자
  , PRIMARY KEY (register_date, invoice_no)
) PARTITION BY RANGE (register_date);

-- ============================================================
-- coupang (쿠팡 Wing)
-- ============================================================

-- [쿠팡 Wing 상품]
CREATE TABLE IF NOT EXISTS coupang.option (
    vendor_inventory_id BIGINT NOT NULL -- 등록상품ID
  , vendor_inventory_item_id BIGINT NOT NULL -- 등록옵션ID
  , product_id BIGINT NOT NULL -- 노출상품ID
  , option_id BIGINT NOT NULL -- 노출옵션ID
  , item_id BIGINT NOT NULL -- 옵션ID
  , barcode TEXT -- 바코드
  , vendor_id TEXT NOT NULL -- 업체코드
  , product_name TEXT -- 등록상품명
  , option_name TEXT -- 등록옵션명
  , display_category_id INTEGER -- 노출카테고리코드
  , category_id INTEGER -- 등록카테고리코드
  , category_name TEXT -- 카테고리
  , brand_name TEXT -- 브랜드
  , maker_name TEXT -- 제조사
  , product_status SMALLINT -- 판매상태
  , is_deleted BOOLEAN -- 삭제여부
  , price INTEGER -- 판매가
  , sales_price INTEGER -- 할인가
  , delivery_fee INTEGER -- 배송비
  , order_quantity INTEGER -- 판매량
  , stock_quantity INTEGER -- 재고수량
  , register_dt TIMESTAMP -- 생성일시
  , modify_dt TIMESTAMP -- 수정일시
  , PRIMARY KEY (vendor_inventory_item_id)
);

-- [쿠팡 업체]
CREATE TABLE IF NOT EXISTS coupang.vendor (
    vendor_id TEXT NOT NULL -- 업체코드
  , vendor_name TEXT -- 업체명
  , vendor_alias TEXT -- 업체별칭
  , vendor_seq BIGINT -- 업체순번
  , bundle_brand_ids TEXT -- 연결브랜드ID
  , rocket_sales_date DATE -- 매출인식일
  , rocket_settle_date DATE -- 정산시작일
  , PRIMARY KEY (vendor_id)
);

-- ============================================================
-- coupang_ads (쿠팡 광고)
-- ============================================================

-- [쿠팡 광고 캠페인]
CREATE TABLE IF NOT EXISTS coupang_ads.campaign (
    campaign_id BIGINT NOT NULL -- 캠페인ID
  , campaign_name TEXT -- 캠페인명
  , campaign_type TEXT -- 캠페인유형
  , vendor_id TEXT NOT NULL -- 업체코드
  , vendor_type SMALLINT -- 공급방식
  , goal_type SMALLINT -- 광고목표
  , is_active BOOLEAN -- 상태
  , is_deleted BOOLEAN -- 삭제여부
  , roas_target INTEGER -- 목표수익률
  , created_at TIMESTAMP -- 등록일시
  , updated_at TIMESTAMP -- 수정일시
  , PRIMARY KEY (campaign_id)
);

-- [쿠팡 광고그룹]
CREATE TABLE IF NOT EXISTS coupang_ads.adgroup (
    adgroup_id BIGINT NOT NULL -- 광고그룹ID
  , adgroup_name TEXT -- 광고그룹명
  , vendor_id TEXT NOT NULL -- 업체코드
  , campaign_id BIGINT NOT NULL -- 캠페인ID
  , goal_type SMALLINT -- 광고목표
  , is_active BOOLEAN -- 상태
  , is_deleted BOOLEAN -- 삭제여부
  , roas_target INTEGER -- 목표수익률
  , created_at TIMESTAMP -- 등록일시
  , updated_at TIMESTAMP -- 수정일시
  , PRIMARY KEY (adgroup_id)
);

-- [쿠팡 광고 소재]
CREATE TABLE IF NOT EXISTS coupang_ads.creative (
    creative_id BIGINT NOT NULL -- 소재ID
  , option_id BIGINT -- 광고집행 옵션ID
  , vendor_id TEXT NOT NULL -- 업체코드
  , creative_type TEXT -- 영역구분
  , headline TEXT -- 소재이름
  , ordering INTEGER -- 순서
  , PRIMARY KEY (creative_id)
);

-- [쿠팡 매출 성장 광고 보고서]
CREATE TABLE IF NOT EXISTS coupang_ads.report_pa (
    campaign_id BIGINT NOT NULL -- 캠페인ID
  , vendor_id TEXT NOT NULL -- 업체코드
  , option_id BIGINT NOT NULL -- 광고집행 옵션ID
  , option_conv_id BIGINT NOT NULL -- 광고전환매출발생 옵션ID
  , placement_group SMALLINT NOT NULL -- 광고노출지면
  , impression_count INTEGER -- 노출수
  , click_count INTEGER -- 클릭수
  , ad_cost INTEGER -- 광고비
  , conv_count INTEGER -- 전환수
  , direct_conv_count INTEGER -- 직접전환수
  , conv_amount INTEGER -- 전환매출액
  , direct_conv_amount INTEGER -- 직접전환매출액
  , ymd DATE NOT NULL -- 날짜
  , PRIMARY KEY (ymd, campaign_id, option_id, option_conv_id, placement_group)
) PARTITION BY RANGE (ymd);
CREATE INDEX IF NOT EXISTS cpa_report__option_idx ON coupang_ads.report_pa (option_id);

-- [쿠팡 신규 구매 고객 확보 광고 보고서]
CREATE TABLE IF NOT EXISTS coupang_ads.report_nca (
    campaign_id BIGINT NOT NULL -- 캠페인ID
  , vendor_id TEXT NOT NULL -- 업체코드
  , creative_id BIGINT NOT NULL -- 소재ID
  , creative_type SMALLINT -- 소재유형
  , option_id BIGINT -- 광고집행 옵션ID
  , placement_group SMALLINT NOT NULL -- 광고노출지면
  , impression_count INTEGER -- 노출수
  , click_count INTEGER -- 클릭수
  , ad_cost INTEGER -- 광고비
  , view_count INTEGER -- 동영상조회수
  , stay_time NUMERIC(18, 2) -- 평균재생시간
  , ymd DATE NOT NULL -- 날짜
  , PRIMARY KEY (ymd, campaign_id, creative_id, placement_group)
) PARTITION BY RANGE (ymd);
CREATE INDEX IF NOT EXISTS cpa_report__creative_idx ON coupang_ads.report_nca (creative_id);

-- ============================================================
-- coupang_rfm (쿠팡 로켓그로스)
-- ============================================================

-- [쿠팡 로켓그로스 재고현황]
CREATE TABLE IF NOT EXISTS coupang_rfm.inventory (
    vendor_inventory_id BIGINT NOT NULL -- 등록상품ID
  , vendor_inventory_item_id BIGINT -- 등록옵션ID
  , product_id BIGINT NOT NULL -- 노출상품ID
  , option_id BIGINT NOT NULL -- 노출옵션ID
  , sku_id BIGINT -- SKU ID
  , vendor_id TEXT NOT NULL -- 업체코드
  , stock_quantity INTEGER -- 판매가능재고
  , inprogress_quantity INTEGER -- 입고예정재고
  , sales_amount_7d INTEGER -- 최근매출 (지난7일)
  , sales_amount_30d INTEGER -- 최근매출 (지난30일)
  , unit_sold_7d INTEGER -- 최근판매수량 (지난7일)
  , unit_sold_30d INTEGER -- 최근판매수량 (지난30일)
  , days_of_cover INTEGER -- 재고예상소진일
  , fee_amount INTEGER -- 이번달 누적보관료
  , updated_at TIMESTAMP NOT NULL -- 갱신일시
  , PRIMARY KEY (updated_at, option_id)
) PARTITION BY RANGE (updated_at);

-- [쿠팡 로켓그로스 재고-소비기한]
CREATE TABLE IF NOT EXISTS coupang_rfm.inventory_exp (
    option_id BIGINT NOT NULL -- 노출옵션ID
  , expiration_date DATE NOT NULL -- 유통기한
  , start_time TIMESTAMP NOT NULL -- 시작일시
  , end_time TIMESTAMP NOT NULL -- 종료일시
  , PRIMARY KEY (option_id, expiration_date)
);

-- [쿠팡 로켓그로스 정산현황 - 판매 수수료 리포트]
CREATE TABLE IF NOT EXISTS coupang_rfm.sales (
    order_id BIGINT NOT NULL -- 주문ID
  , vendor_id TEXT NOT NULL -- 업체코드
  , product_id BIGINT NOT NULL -- 등록상품 ID
  , option_id BIGINT NOT NULL -- 옵션ID
  , sku_id BIGINT -- SKU ID
  , category_id INTEGER -- 카테고리ID
  , settlement_type SMALLINT NOT NULL -- 거래유형
  , period_type SMALLINT NOT NULL -- 정산유형
  , unit_price INTEGER -- 판매가
  , order_quantity INTEGER -- 판매수량
  , coupang_discount INTEGER -- 쿠팡지원할인
  , seller_discount INTEGER -- 판매자할인쿠폰
  , settlement_amount INTEGER -- 정산대상액
  , sales_date DATE NOT NULL -- 매출인식일
  , settlement_date DATE -- 정산종료일
  , PRIMARY KEY (sales_date, order_id, option_id, settlement_type)
) PARTITION BY RANGE (sales_date);
CREATE INDEX IF NOT EXISTS cpr_sales__vendor_idx ON coupang_rfm.sales (vendor_id);

-- [쿠팡 로켓그로스 정산현황 - 입출고비/배송비 리포트]
CREATE TABLE IF NOT EXISTS coupang_rfm.shipping (
    order_id BIGINT NOT NULL -- 주문ID
  , invoice_no BIGINT NOT NULL -- 배송ID
  , vendor_id TEXT NOT NULL -- 업체코드
  , product_id BIGINT NOT NULL -- 등록상품 ID
  , option_id BIGINT NOT NULL -- 옵션ID
  , sku_id BIGINT -- SKU ID
  , settlement_type SMALLINT NOT NULL -- 거래유형
  , period_type SMALLINT NOT NULL -- 정산유형
  , warehousing_fee INTEGER -- 발생비용
  , discount_amount INTEGER -- 할인가
  , extra_fee INTEGER -- 추가비용
  , sales_date DATE NOT NULL -- 주문일
  , shipping_date DATE -- 매출인식일
  , settlement_date DATE -- 정산종료일
  , PRIMARY KEY (sales_date, order_id, option_id, settlement_type)
) PARTITION BY RANGE (sales_date);
CREATE INDEX IF NOT EXISTS cpr_shipping__vendor_idx ON coupang_rfm.shipping (vendor_id);

-- ============================================================
-- ecount (이카운트)
-- ============================================================

-- [이카운트 재고현황]
CREATE TABLE IF NOT EXISTS ecount.inventory (
    product_code TEXT NOT NULL -- 품목코드
  , quantity INTEGER -- 재고수량
  , updated_at TIMESTAMP NOT NULL -- 갱신일시
  , PRIMARY KEY (updated_at, product_code)
) PARTITION BY RANGE (updated_at);

-- [이카운트 품목등록 리스트]
CREATE TABLE IF NOT EXISTS ecount.product (
    product_code TEXT NOT NULL -- 품목코드
  , option_id TEXT -- 옵션코드
  , product_name TEXT -- 품목명
  , product_keyword TEXT -- 상품약어
  , brand_name TEXT -- 브랜드
  , remarks TEXT -- 적요
  , unit_quantity INTEGER -- 규격
  , unit_name TEXT -- 단위
  , org_price INTEGER -- 원가
  , expiration_date TEXT -- 유통기한
  , updated_at TIMESTAMP -- 갱신일시
  , PRIMARY KEY (product_code)
);

-- [이카운트 재고-발주일정]
CREATE TABLE IF NOT EXISTS ecount.schedule (
    product_code TEXT NOT NULL -- 품목코드
  , order_date DATE -- 발주진행일
  , delivery_date DATE -- 입고예정일
  , remarks TEXT -- 구분
  , PRIMARY KEY (product_code)
);

-- ============================================================
-- google_ads (구글 광고)
-- ============================================================

-- [구글 광고 계정]
CREATE TABLE IF NOT EXISTS google_ads.account (
    customer_id BIGINT NOT NULL -- 계정ID
  , userid TEXT -- 아이디
  , account_name TEXT -- 계정명
  , account_seq BIGINT -- 계정순번
  , bundle_brand_ids TEXT -- 연결브랜드ID
  , PRIMARY KEY (customer_id)
);

-- [구글 광고 캠페인 보고서]
CREATE TABLE IF NOT EXISTS google_ads.campaign (
    campaign_id TEXT NOT NULL -- 캠페인ID
  , campaign_name TEXT -- 캠페인명
  , customer_id BIGINT NOT NULL -- 고객ID
  , campaign_type TEXT -- 캠페인유형
  , campaign_status TEXT -- 캠페인상태
  , bidding_strategy TEXT -- 입찰전략유형
  , campaign_budget INTEGER -- 캠페인예산
  , impression_count_30d INTEGER -- 노출수
  , click_count_30d INTEGER -- 클릭수
  , ad_cost_30d INTEGER -- 비용
  , created_at TIMESTAMP -- 등록일시
  , PRIMARY KEY (campaign_id)
);

-- [구글 광고그룹 보고서]
CREATE TABLE IF NOT EXISTS google_ads.adgroup (
    adgroup_id TEXT NOT NULL -- 광고그룹ID
  , adgroup_name TEXT -- 광고그룹명
  , customer_id BIGINT NOT NULL -- 고객ID
  , campaign_id TEXT NOT NULL -- 캠페인ID
  , adgroup_type TEXT -- 광고그룹유형
  , adgroup_status TEXT -- 광고그룹상태
  , target_cpa INTEGER -- 타겟CPA
  , impression_count_30d INTEGER -- 노출수
  , click_count_30d INTEGER -- 클릭수
  , ad_cost_30d INTEGER -- 비용
  , PRIMARY KEY (adgroup_id)
);

-- [구글 광고 소재 보고서]
CREATE TABLE IF NOT EXISTS google_ads.ad (
    ad_id TEXT NOT NULL -- 광고ID
  , ad_name TEXT -- 확장소재명
  , customer_id BIGINT NOT NULL -- 고객ID
  , campaign_id TEXT NOT NULL -- 캠페인ID
  , adgroup_id TEXT NOT NULL -- 광고그룹ID
  , ad_type TEXT -- 광고유형
  , ad_status TEXT -- 광고상태
  , impression_count_30d INTEGER -- 노출수
  , click_count_30d INTEGER -- 클릭수
  , ad_cost_30d INTEGER -- 비용
  , PRIMARY KEY (ad_id)
);

-- [구글 광고 일별 소재 보고서]
CREATE TABLE IF NOT EXISTS google_ads.insight (
    customer_id BIGINT NOT NULL -- 고객ID
  , campaign_id TEXT NOT NULL -- 캠페인ID
  , adgroup_id TEXT NOT NULL -- 광고그룹ID
  , ad_id TEXT NOT NULL -- 광고ID
  , device_type SMALLINT NOT NULL -- 기기유형
  , impression_count INTEGER -- 노출수
  , click_count INTEGER -- 클릭수
  , ad_cost INTEGER -- 비용
  , ymd DATE NOT NULL -- 날짜
  , PRIMARY KEY (ymd, ad_id, device_type)
) PARTITION BY RANGE (ymd);

-- [구글 광고 애셋 보고서]
CREATE TABLE IF NOT EXISTS google_ads.asset (
    asset_id TEXT NOT NULL -- 애셋ID
  , asset_name TEXT -- 애셋명
  , customer_id BIGINT NOT NULL -- 고객ID
  , asset_type TEXT -- 애셋유형
  , asset_url TEXT -- URL
  , PRIMARY KEY (asset_id)
);

-- ============================================================
-- meta_ads (메타 광고)
-- ============================================================

-- [메타 광고 계정]
CREATE TABLE IF NOT EXISTS meta_ads.account (
    account_id TEXT NOT NULL -- 계정ID
  , userid TEXT -- 아이디
  , account_name TEXT -- 계정명
  , account_seq BIGINT -- 계정순번
  , bundle_brand_ids TEXT -- 연결브랜드ID
  , PRIMARY KEY (account_id)
);

-- [메타 광고 캠페인 보고서]
CREATE TABLE IF NOT EXISTS meta_ads.campaign (
    campaign_id TEXT NOT NULL -- 캠페인ID
  , campaign_name TEXT -- 캠페인명
  , account_id TEXT NOT NULL -- 계정ID
  , objective TEXT -- 캠페인목표
  , effective_status TEXT -- 캠페인상태
  , created_at TIMESTAMP -- 등록일시
  , PRIMARY KEY (campaign_id)
);

-- [메타 광고세트 보고서]
CREATE TABLE IF NOT EXISTS meta_ads.adset (
    adset_id TEXT NOT NULL -- 광고세트ID
  , adset_name TEXT -- 광고세트명
  , account_id TEXT NOT NULL -- 계정ID
  , campaign_id TEXT NOT NULL -- 캠페인ID
  , effective_status TEXT -- 광고세트상태
  , daily_budget INTEGER -- 일일예산
  , created_at TIMESTAMP -- 등록일시
  , PRIMARY KEY (adset_id)
);

-- [메타 광고 광고 보고서]
CREATE TABLE IF NOT EXISTS meta_ads.ad (
    ad_id TEXT NOT NULL -- 광고ID
  , ad_name TEXT -- 광고명
  , account_id TEXT NOT NULL -- 계정ID
  , campaign_id TEXT NOT NULL -- 캠페인ID
  , adset_id TEXT NOT NULL -- 광고세트ID
  , effective_status TEXT -- 광고상태
  , created_at TIMESTAMP -- 등록일시
  , PRIMARY KEY (ad_id)
);

-- [메타 광고 광고 성과 보고서]
CREATE TABLE IF NOT EXISTS meta_ads.insight (
    account_id TEXT NOT NULL -- 계정ID
  , campaign_id TEXT NOT NULL -- 캠페인ID
  , adset_id TEXT NOT NULL -- 광고세트ID
  , ad_id TEXT NOT NULL -- 광고ID
  , impression_count INTEGER -- 노출수
  , reach_count INTEGER -- 도달수
  , click_count INTEGER -- 클릭수
  , link_click_count INTEGER -- 링크클릭수
  , ad_cost INTEGER -- 광고비
  , ymd DATE NOT NULL -- 날짜
  , PRIMARY KEY (ymd, account_id, campaign_id, adset_id, ad_id)
) PARTITION BY RANGE (ymd);
CREATE INDEX IF NOT EXISTS met_ads__ad_idx ON meta_ads.insight (ymd, ad_id);

-- ============================================================
-- naver_shp (네이버 쇼핑)
-- ============================================================

-- [네이버 쇼핑 카테고리]
CREATE TABLE IF NOT EXISTS naver_shp.category (
    category_id INTEGER NOT NULL -- 카테고리코드
  , category_name TEXT NOT NULL -- 카테고리
  , category_id1 INTEGER -- 대분류코드
  , category_id2 INTEGER -- 중분류코드
  , category_id3 INTEGER -- 소분류코드
  , category_id4 INTEGER -- 세분류코드
  , category_name1 TEXT -- 대분류
  , category_name2 TEXT -- 중분류
  , category_name3 TEXT -- 소분류
  , category_name4 TEXT -- 세분류
  , depth SMALLINT NOT NULL -- 카테고리단계
  , PRIMARY KEY (category_id)
);
CREATE INDEX IF NOT EXISTS nsh_category__depth_idx ON naver_shp.category (depth);
CREATE INDEX IF NOT EXISTS nsh_category__id4_idx ON naver_shp.category (category_id1, category_id2, category_id3, category_id4);

-- [네이버 쇼핑 검색 키워드]
CREATE TABLE IF NOT EXISTS naver_shp.keyword (
    keyword TEXT NOT NULL -- 키워드
  , keyword_group TEXT NOT NULL -- 키워드그룹
  , keyword_seq INTEGER -- 키워드순번
  , keyword_type TEXT -- 키워드분류
  , target_page INTEGER -- 목표페이지
  , page_unit_ad INTEGER -- 광고페이지단위
  , page_unit_shop INTEGER -- 비광고페이지단위
  , is_deleted BOOLEAN -- 삭제여부
  , updated_at TIMESTAMP -- 갱신일시
  , deleted_at TIMESTAMP -- 삭제일시
  , PRIMARY KEY (keyword)
);

-- [네이버 쇼핑 상품 목록]
CREATE TABLE IF NOT EXISTS naver_shp.product (
    nv_mid BIGINT NOT NULL -- 쇼핑상품ID
  , product_id BIGINT -- 상품코드
  , product_type SMALLINT -- 상품종류
  , product_name TEXT -- 상품명
  , category_id INTEGER -- 카테고리코드
  , full_category_name TEXT -- 전체카테고리
  , mall_name TEXT -- 판매처
  , brand_name TEXT -- 브랜드
  , sales_price INTEGER -- 판매가
  , updated_at TIMESTAMP -- 갱신일시
  , PRIMARY KEY (nv_mid)
);

-- [네이버 쇼핑 상품 순위 (시간별)]
CREATE TABLE IF NOT EXISTS naver_shp.rank (
    keyword TEXT NOT NULL -- 키워드
  , nv_mid BIGINT NOT NULL -- 쇼핑상품ID
  , product_id BIGINT -- 상품코드
  , product_type SMALLINT -- 상품종류
  , display_rank SMALLINT NOT NULL -- 노출순위
  , created_at TIMESTAMP NOT NULL -- 수집일시
  , PRIMARY KEY (created_at, keyword, display_rank)
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS nsh_rank__keyword_idx ON naver_shp.rank (keyword);
CREATE INDEX IF NOT EXISTS nsh_rank__item_idx ON naver_shp.rank (nv_mid);

-- [네이버 쇼핑 상품 순위 (최신)]
CREATE TABLE IF NOT EXISTS naver_shp.rank_now (
    keyword TEXT NOT NULL -- 키워드
  , nv_mid BIGINT NOT NULL -- 쇼핑상품ID
  , product_id BIGINT -- 상품코드
  , product_type SMALLINT -- 상품종류
  , display_rank SMALLINT NOT NULL -- 노출순위
  , created_at TIMESTAMP NOT NULL -- 수집일시
  , PRIMARY KEY (keyword, display_rank)
);
CREATE INDEX IF NOT EXISTS nsh_rank_now__keyword_idx ON naver_shp.rank_now (keyword);
CREATE INDEX IF NOT EXISTS nsh_rank_now__item_idx ON naver_shp.rank_now (nv_mid);

-- ============================================================
-- relation
-- ============================================================

-- [종합 캠페인/광고그룹/소재 - 사방넷 묶음품목 관계]
CREATE TABLE IF NOT EXISTS relation.ad_id_to_sbn_ids (
    ad_id TEXT NOT NULL -- 광고ID
  , ad_level INTEGER NOT NULL -- 광고단위
  , bundle_product_ids TEXT NOT NULL -- 연결품번코드
  , platform_name TEXT NOT NULL -- 광고플랫폼
  , PRIMARY KEY (platform_name, ad_level, ad_id)
);

-- [쿠팡 옵션 - 사방넷 묶음상품 관계]
CREATE TABLE IF NOT EXISTS relation.cpg_opt_to_sbn_ids (
    option_id BIGINT NOT NULL -- 노출옵션ID
  , bundle_product_ids TEXT -- 연결품번코드
  , PRIMARY KEY (option_id)
);

-- [네이버 쇼핑 키워드 - 상품 관계]
CREATE TABLE IF NOT EXISTS relation.nsh_kwd_to_prd_id (
    keyword TEXT NOT NULL -- 키워드
  , product_id BIGINT NOT NULL -- 상품코드
  , PRIMARY KEY (keyword, product_id)
);

-- [네이버 쇼핑 상품 - 카탈로그 관계 (최신)]
CREATE TABLE IF NOT EXISTS relation.nsh_prd_to_ctl_id (
    product_id BIGINT NOT NULL -- 상품코드
  , catalog_id BIGINT NOT NULL -- 카탈로그코드
  , created_at TIMESTAMP NOT NULL -- 수집일시
  , PRIMARY KEY (product_id)
);

-- [스마트스토어 옵션 - 사방넷 묶음상품 관계]
CREATE TABLE IF NOT EXISTS relation.smt_opt_to_sbn_ids (
    option_id BIGINT NOT NULL -- 옵션코드
  , bundle_product_ids TEXT -- 연결품번코드
  , PRIMARY KEY (option_id)
);

-- [스마트스토어 상품 - 사방넷 묶음품목 관계]
CREATE TABLE IF NOT EXISTS relation.smt_prd_to_sbn_ids (
    product_id BIGINT NOT NULL -- 상품코드
  , bundle_product_ids TEXT -- 연결품번코드
  , PRIMARY KEY (product_id)
);

-- ============================================================
-- sabangnet (사방넷)
-- ============================================================

-- [사방넷 쇼핑몰]
CREATE TABLE IF NOT EXISTS sabangnet.shop (
    shop_seq BIGINT NOT NULL -- 쇼핑몰순번
  , shop_id TEXT NOT NULL -- 쇼핑몰코드
  , shop_alias TEXT -- 쇼핑몰별칭
  , shop_name TEXT -- 쇼핑몰명
  , corp_name TEXT -- 법인명
  , shop_group TEXT -- 마켓분류
  , shop_url TEXT -- 쇼핑몰URL
  , scm_url TEXT -- SCM URL
  , commission_rate DOUBLE PRECISION -- 수수료율
  , PRIMARY KEY (shop_id)
);

-- [사방넷 쇼핑몰계정]
CREATE TABLE IF NOT EXISTS sabangnet.account (
    account_no INTEGER NOT NULL -- 계정등록순번
  , shop_id TEXT NOT NULL -- 쇼핑몰코드
  , shop_name TEXT -- 쇼핑몰명
  , shop_group TEXT -- 마켓분류
  , userid TEXT -- 쇼핑몰ID
  , status BOOLEAN -- 상태
  , commission_rate DOUBLE PRECISION -- 수수료율
  , PRIMARY KEY (account_no)
);

-- [사방넷 주문 내역]
CREATE TABLE IF NOT EXISTS sabangnet.order (
    order_seq BIGINT NOT NULL -- 주문번호(사방넷)
  , order_seq_org BIGINT -- 원주문번호(사방넷)
  , order_id TEXT -- 주문번호(쇼핑몰)
  , order_id_dup TEXT -- 부주문번호(쇼핑몰)
  , account_no INTEGER NOT NULL -- 계정등록순번
  , option_id TEXT -- 옵션코드(사방넷)
  , product_id_shop TEXT -- 상품코드(쇼핑몰)
  , order_quantity INTEGER -- 주문수량
  , sku_quantity INTEGER -- EA
  , payment_amount INTEGER -- 결제금액
  , order_amount INTEGER -- 주문금액
  , register_dt TIMESTAMP NOT NULL -- 수집일시
  , order_dt TIMESTAMP NOT NULL -- 주문일시
  , PRIMARY KEY (order_dt, order_seq)
) PARTITION BY RANGE (order_dt);
CREATE INDEX IF NOT EXISTS sbn_order__account_idx ON sabangnet.order (account_no);
CREATE INDEX IF NOT EXISTS sbn_order__dt_idx ON sabangnet.order (order_dt);

-- [사방넷 주문 옵션]
CREATE TABLE IF NOT EXISTS sabangnet.order_option (
    option_id TEXT NOT NULL -- 옵션코드(사방넷)
  , product_id_shop TEXT NOT NULL -- 상품코드(쇼핑몰)
  , account_no INTEGER NOT NULL -- 계정등록순번
  , model_code TEXT -- 모델명
  , model_id TEXT -- 자체상품코드
  , product_name TEXT -- 상품명(확정)
  , product_name_shop TEXT -- 상품명(수집)
  , product_name_abbr TEXT -- 상품약어
  , option_name TEXT -- 옵션(확정)
  , option_name_shop TEXT -- 옵션(수집)
  , option_name_abbr TEXT -- 옵션별칭
  , sales_price INTEGER -- 판매가(상품)
  , order_id TEXT -- 주문번호(쇼핑몰)
  , last_order_date DATE -- 최근주문일자
  , PRIMARY KEY (account_no, product_id_shop, option_id)
);

-- [사방넷 발주 내역]
CREATE TABLE IF NOT EXISTS sabangnet.order_invoice (
    order_seq BIGINT NOT NULL -- 주문번호(사방넷)
  , account_no INTEGER NOT NULL -- 계정등록순번
  , invoice_no TEXT NOT NULL -- 송장번호
  , delivery_company TEXT -- 택배사
  , order_status_div INTEGER -- 주문구분
  , order_status INTEGER -- 주문상태
  , order_dt TIMESTAMP NOT NULL -- 주문일시
  , invoice_date DATE -- 송장등록일자
  , PRIMARY KEY (order_dt, order_seq)
) PARTITION BY RANGE (order_dt);
CREATE INDEX IF NOT EXISTS sbn_order_invoice__account_idx ON sabangnet.order_invoice (account_no);
CREATE INDEX IF NOT EXISTS sbn_order_invoice__dt_idx ON sabangnet.order_invoice (order_dt);

-- [사방넷 발송 내역]
CREATE TABLE IF NOT EXISTS sabangnet.order_dispatch (
    order_seq BIGINT NOT NULL -- 주문번호(사방넷)
  , order_id TEXT -- 주문번호(쇼핑몰)
  , account_no INTEGER NOT NULL -- 계정등록순번
  , option_id TEXT -- 상품코드(사방넷)
  , sku_quantity INTEGER -- EA(확정)
  , orderer_name TEXT -- 주문자명
  , receiver_name TEXT -- 수취인명
  , zipcode TEXT -- 수취인우편번호1
  , address TEXT -- 수취인주소1
  , phone1 TEXT -- 수취인전화번호1
  , phone2 TEXT -- 수취인전화번호2
  , delivery_message TEXT -- 배송메세지1
  , box_type TEXT -- 박스타입
  , delivery_type TEXT -- 운임구분
  , register_dt TIMESTAMP NOT NULL -- 수집일시
  , order_dt TIMESTAMP -- 주문일시
  , PRIMARY KEY (register_dt, order_seq)
) PARTITION BY RANGE (register_dt);

-- [사방넷 품번코드매핑]
CREATE TABLE IF NOT EXISTS sabangnet.mapping_id (
    product_id_shop TEXT NOT NULL -- 상품코드(쇼핑몰)
  , product_id TEXT NOT NULL -- 상품코드(사방넷)
  , account_no INTEGER NOT NULL -- 계정등록순번
  , shop_id TEXT -- 쇼핑몰코드
  , product_name TEXT -- 상품명
  , sales_price INTEGER -- 판매가
  , mapping_count INTEGER -- 매핑수량
  , PRIMARY KEY (account_no, product_id_shop)
);

-- [사방넷 단품코드매핑]
CREATE TABLE IF NOT EXISTS sabangnet.mapping_name (
    product_id_shop TEXT NOT NULL -- 상품코드(쇼핑몰)
  , option_id TEXT NOT NULL -- 옵션코드(사방넷)
  , shop_id TEXT NOT NULL -- 쇼핑몰코드
  , product_name TEXT -- 상품명
  , option_name TEXT -- 옵션명(사방넷)
  , sku_seq INTEGER NOT NULL -- 옵션순번
  , sku_name TEXT -- 옵션명(쇼핑몰)
  , register_dt TIMESTAMP -- 등록일시
  , PRIMARY KEY (shop_id, product_id_shop, sku_seq)
);

-- [사방넷 상품]
CREATE TABLE IF NOT EXISTS sabangnet.product (
    product_id TEXT NOT NULL -- 품번코드
  , model_code TEXT -- 모델명
  , model_id TEXT -- 자체상품코드
  , product_name TEXT -- 상품명
  , product_keyword TEXT -- 상품약어
  , brand_name TEXT -- 브랜드
  , maker_name TEXT -- 제조사
  , logistics_service TEXT -- 물류처
  , product_status SMALLINT -- 상품상태
  , manufacture_year INTEGER -- 생산년도
  , sales_price INTEGER -- 판매가
  , org_price INTEGER -- 원가
  , image_file TEXT -- 대표이미지
  , register_dt TIMESTAMP -- 등록일시
  , modify_dt TIMESTAMP -- 수정일시
  , PRIMARY KEY (product_id)
);

-- [사방넷 옵션]
CREATE TABLE IF NOT EXISTS sabangnet.option (
    option_id TEXT NOT NULL -- 옵션코드
  , barcode BIGINT -- 바코드
  , option_group TEXT -- 옵션제목
  , option_name TEXT -- 옵션상세명칭
  , bundle_option_ids TEXT -- 연결상품코드
  , option_status SMALLINT -- 공급상태
  , option_type SMALLINT -- 옵션구분
  , option_quantity INTEGER -- EA
  , option_price INTEGER -- 단품추가금액
  , register_dt TIMESTAMP -- 등록일시
  , PRIMARY KEY (option_id)
);

-- [사방넷 추가상품]
CREATE TABLE IF NOT EXISTS sabangnet.add_product (
    group_id TEXT NOT NULL -- 그룹코드
  , group_name TEXT -- 추가상품제목
  , shop_id TEXT -- 쇼핑콜코드
  , option_seq INTEGER NOT NULL -- 옵션순서
  , option_id TEXT NOT NULL -- 옵션코드
  , option_name TEXT -- 옵션명
  , sales_price INTEGER -- 판매가
  , register_dt TIMESTAMP -- 등록일시
  , modify_dt TIMESTAMP -- 수정일시
  , PRIMARY KEY (group_id, option_seq)
);

-- ============================================================
-- searchad (네이버 검색광고)
-- ============================================================

-- [네이버 검색광고 계정]
CREATE TABLE IF NOT EXISTS searchad.account (
    customer_id BIGINT NOT NULL -- 계정ID
  , userid TEXT -- 아이디
  , account_name TEXT -- 계정명
  , account_type TEXT -- 계정유형
  , account_seq BIGINT -- 계정순번
  , bundle_brand_ids TEXT -- 연결브랜드ID
  , PRIMARY KEY (customer_id)
);

-- [네이버 검색광고 캠페인]
CREATE TABLE IF NOT EXISTS searchad.campaign (
    campaign_id TEXT NOT NULL -- 캠페인ID
  , campaign_name TEXT -- 캠페인명
  , campaign_type SMALLINT -- 캠페인유형
  , customer_id BIGINT NOT NULL -- 계정ID
  , is_enabled BOOLEAN -- 노출여부
  , is_deleted BOOLEAN -- 삭제여부
  , created_at TIMESTAMP -- 등록일시
  , deleted_at TIMESTAMP -- 삭제일시
  , PRIMARY KEY (campaign_id)
);

-- [네이버 검색광고 광고그룹]
CREATE TABLE IF NOT EXISTS searchad.adgroup (
    adgroup_id TEXT NOT NULL -- 광고그룹ID
  , campaign_id TEXT NOT NULL -- 캠페인ID
  , adgroup_name TEXT -- 광고그룹명
  , adgroup_type SMALLINT -- 광고그룹유형
  , customer_id BIGINT NOT NULL -- 계정ID
  , is_enabled BOOLEAN -- 노출여부
  , is_deleted BOOLEAN -- 삭제여부
  , bid_amount INTEGER -- 입찰가
  , created_at TIMESTAMP -- 등록일시
  , deleted_at TIMESTAMP -- 삭제일시
  , PRIMARY KEY (adgroup_id)
);

-- [네이버 검색광고 소재]
CREATE TABLE IF NOT EXISTS searchad.ad (
    ad_id TEXT NOT NULL -- 소재ID
  , adgroup_id TEXT NOT NULL -- 광고그룹ID
  , ad_type SMALLINT -- 소재유형
  , customer_id BIGINT NOT NULL -- 계정ID
  , title TEXT -- 제목
  , description TEXT -- 설명
  , landing_url_pc TEXT -- 랜딩주소(PC)
  , landing_url_mobile TEXT -- 랜딩주소(모바일)
  , nv_mid BIGINT -- 쇼핑상품ID
  , product_id BIGINT -- 상품코드
  , category_id INTEGER -- 카테고리코드
  , is_enabled BOOLEAN -- 노출여부
  , is_deleted BOOLEAN -- 삭제여부
  , bid_amount INTEGER -- 입찰가
  , sales_price INTEGER -- 판매가
  , created_at TIMESTAMP -- 등록일시
  , deleted_at TIMESTAMP -- 삭제일시
  , PRIMARY KEY (ad_id)
);

-- [네이버 검색광고 매체]
CREATE TABLE IF NOT EXISTS searchad.media (
    media_code INTEGER NOT NULL -- 매체코드
  , media_name TEXT -- 매체이름
  , media_type SMALLINT NOT NULL -- 매체타입
  , group_id INTEGER -- 상위매체그룹
  , PRIMARY KEY (media_type, media_code)
);

-- [네이버 검색광고 다차원 보고서]
CREATE TABLE IF NOT EXISTS searchad.report_sad (
    ad_id TEXT NOT NULL -- 소재ID
  , customer_id BIGINT NOT NULL -- 계정ID
  , media_code BIGINT NOT NULL -- 매체코드
  , pc_mobile_type SMALLINT NOT NULL -- PC/모바일 매체
  , impression_count INTEGER -- 노출수
  , click_count INTEGER -- 클릭수
  , ad_cost INTEGER -- 총비용(원)
  , ad_rank_sum INTEGER -- 노출순위합계
  , conv_count INTEGER -- 전환수
  , direct_conv_count INTEGER -- 직접전환수
  , conv_amount INTEGER -- 전환매출액(원)
  , direct_conv_amount INTEGER -- 직접전환매출액(원)
  , ymd DATE NOT NULL -- 일별
  , PRIMARY KEY (ymd, ad_id, pc_mobile_type, media_code)
) PARTITION BY RANGE (ymd);

-- [네이버 성과형 디스플레이 광고 보고서]
CREATE TABLE IF NOT EXISTS searchad.report_gfa (
    campaign_no BIGINT NOT NULL -- 캠페인ID
  , adset_no BIGINT NOT NULL -- 광고그룹ID
  , creative_no BIGINT NOT NULL -- 소재ID
  , account_no BIGINT NOT NULL -- 계정ID
  , impression_count INTEGER -- 노출수
  , click_count INTEGER -- 클릭수
  , reach_count INTEGER -- 도달수
  , ad_cost INTEGER -- 총비용(VAT포함,원)
  , conv_count INTEGER -- 전환수
  , conv_amount INTEGER -- 전환매출액(원)
  , ymd DATE NOT NULL -- 일별
  , PRIMARY KEY (ymd, campaign_no, creative_no)
) PARTITION BY RANGE (ymd);

-- [네이버 검색광고 계약기간]
CREATE TABLE IF NOT EXISTS searchad.contract (
    contract_id TEXT NOT NULL -- 계약ID
  , adgroup_id TEXT NOT NULL -- 광고그룹ID
  , customer_id BIGINT NOT NULL -- 계정ID
  , contract_name TEXT -- 계약명
  , contract_type INTEGER -- 계약유형
  , contract_status SMALLINT -- 계약상태
  , contract_amount INTEGER -- 계약금액
  , refund_amount INTEGER -- 환불금액
  , contract_qc INTEGER -- 계약가능 검색수
  , keyword_qc INTEGER -- 키워드 검색수
  , register_dt TIMESTAMP -- 등록일시
  , edit_dt TIMESTAMP -- 수정일시
  , contract_start_date DATE NOT NULL -- 계약시작일
  , contract_end_date DATE NOT NULL -- 계약종료일
  , exposure_start_date DATE -- 노출시작일
  , exposure_end_date DATE -- 노출종료일
  , cancel_date DATE -- 취소일
  , PRIMARY KEY (contract_end_date, contract_id)
) PARTITION BY RANGE (contract_end_date);

-- [네이버 검색광고 노출 순위 (시간별)]
CREATE TABLE IF NOT EXISTS searchad.rank (
    keyword TEXT NOT NULL -- 키워드
  , nv_mid BIGINT NOT NULL -- 쇼핑상품ID
  , display_rank SMALLINT NOT NULL -- 노출순위
  , created_at TIMESTAMP NOT NULL -- 수집일시
  , PRIMARY KEY (created_at, keyword, display_rank)
) PARTITION BY RANGE (created_at);

-- [네이버 검색광고 노출 순위 (최신)]
CREATE TABLE IF NOT EXISTS searchad.rank_now (
    keyword TEXT NOT NULL -- 키워드
  , nv_mid BIGINT NOT NULL -- 쇼핑상품ID
  , display_rank SMALLINT NOT NULL -- 노출순위
  , created_at TIMESTAMP NOT NULL -- 수집일시
  , PRIMARY KEY (keyword, display_rank)
);

-- ============================================================
-- smartstore (네이버 스마트스토어)
-- ============================================================

-- [스마트스토어 채널]
CREATE TABLE IF NOT EXISTS smartstore.channel (
    channel_seq BIGINT NOT NULL -- 채널번호
  , channel_name TEXT -- 채널명
  , brand_id TEXT -- 연결브랜드ID
  , brand_name TEXT -- 연결브랜드
  , brand_seq BIGINT -- 연결브랜드순번
  , mall_url TEXT -- 판매처주소
  , order_start_date DATE -- 최초주문일
  , PRIMARY KEY (channel_seq)
);

-- [스마트스토어 상품]
CREATE TABLE IF NOT EXISTS smartstore.product (
    product_id BIGINT NOT NULL -- 상품코드
  , product_no BIGINT NOT NULL -- 상품번호
  , catalog_id BIGINT -- 카탈로그코드
  , channel_seq BIGINT NOT NULL -- 채널번호
  , product_name TEXT -- 상품명
  , management_code TEXT -- 관리코드
  , model_name TEXT -- 모델명
  , brand_name TEXT -- 브랜드
  , category_id INTEGER -- 카테고리코드
  , status_type TEXT -- 판매상태
  , display_type TEXT -- 전시상태
  , tags TEXT -- 태그
  , price INTEGER -- 판매가
  , sales_price INTEGER -- 할인가
  , delivery_type SMALLINT -- 배송속성
  , delivery_fee INTEGER -- 배송비
  , register_dt TIMESTAMP -- 등록일시
  , modify_dt TIMESTAMP -- 수정일시
  , PRIMARY KEY (product_id)
);

-- [스마트스토어 옵션]
CREATE TABLE IF NOT EXISTS smartstore.option (
    product_id BIGINT NOT NULL -- 상품코드
  , option_id BIGINT NOT NULL -- 옵션코드
  , channel_seq BIGINT NOT NULL -- 채널번호
  , product_type SMALLINT -- 상품종류
  , option_group1 TEXT -- 옵션그룹1
  , option_name1 TEXT -- 옵션명1
  , option_group2 TEXT -- 옵션그룹2
  , option_name2 TEXT -- 옵션명2
  , option_group3 TEXT -- 옵션그룹3
  , option_name3 TEXT -- 옵션명3
  , management_code TEXT -- 관리코드
  , usable BOOLEAN -- 사용여부
  , option_price INTEGER -- 옵션가
  , stock_quantity INTEGER -- 재고수량
  , register_order INTEGER -- 등록순서
  , PRIMARY KEY (product_id, option_id)
);

-- [스마트스토어 주문]
CREATE TABLE IF NOT EXISTS smartstore.order (
    order_id BIGINT NOT NULL -- 주문번호
  , channel_seq BIGINT NOT NULL -- 채널번호
  , orderer_no BIGINT -- 주문자번호
  , payment_location SMALLINT -- 결제위치
  , order_dt TIMESTAMP -- 주문일시
  , payment_dt TIMESTAMP NOT NULL -- 결제일시
  , PRIMARY KEY (payment_dt, order_id, channel_seq)
) PARTITION BY RANGE (payment_dt);
CREATE INDEX IF NOT EXISTS smt_order__channel_idx ON smartstore.order (channel_seq);

-- [스마트스토어 상품 주문]
CREATE TABLE IF NOT EXISTS smartstore.order_detail (
    product_order_id BIGINT NOT NULL -- 상품주문번호
  , order_id BIGINT NOT NULL -- 주문번호
  , channel_seq BIGINT NOT NULL -- 채널번호
  , product_id BIGINT NOT NULL -- 상품코드
  , option_id BIGINT NOT NULL -- 옵션코드
  , product_type SMALLINT -- 상품종류
  , delivery_type SMALLINT -- 배송속성
  , delivery_tag_type SMALLINT -- 배송태그타입
  , inflow_path TEXT -- 유입경로
  , inflow_path_add TEXT -- 유입경로 추가정보
  , order_quantity INTEGER -- 주문수량
  , unit_price INTEGER -- 상품가격
  , option_price INTEGER -- 옵션가격
  , discount_amount INTEGER -- 최종 상품별 할인액
  , seller_discount_amount INTEGER -- 판매자 부담 할인액
  , supply_amount INTEGER -- 정산금액
  , delivery_fee INTEGER -- 배송비
  , payment_dt TIMESTAMP NOT NULL -- 결제일시
  , PRIMARY KEY (payment_dt, product_order_id)
) PARTITION BY RANGE (payment_dt);
CREATE INDEX IF NOT EXISTS smt_order_detail__channel_idx ON smartstore.order_detail (channel_seq);
CREATE INDEX IF NOT EXISTS smt_order_detail__product_idx ON smartstore.order_detail (product_id);
CREATE INDEX IF NOT EXISTS smt_order_detail__option_idx ON smartstore.order_detail (option_id);

-- [스마트스토어 주문 배송]
CREATE TABLE IF NOT EXISTS smartstore.order_delivery (
    product_order_id BIGINT NOT NULL -- 상품주문번호
  , order_id BIGINT NOT NULL -- 주문번호
  , channel_seq BIGINT NOT NULL -- 채널번호
  , invoice_no TEXT NOT NULL -- 송장번호
  , delivery_company TEXT -- 택배사
  , delivery_method SMALLINT -- 배송방법
  , zip_code TEXT -- 우편번호
  , latitude TEXT -- 위도
  , longitude TEXT -- 경도
  , pickup_dt TIMESTAMP -- 집화일시
  , send_dt TIMESTAMP -- 발송일시
  , payment_dt TIMESTAMP NOT NULL -- 결제일시
  , PRIMARY KEY (payment_dt, product_order_id)
) PARTITION BY RANGE (payment_dt);
CREATE INDEX IF NOT EXISTS smt_order_delivery__channel_idx ON smartstore.order_delivery (channel_seq);

-- [스마트스토어 주문 옵션]
CREATE TABLE IF NOT EXISTS smartstore.order_option (
    product_id BIGINT NOT NULL -- 상품코드
  , option_id BIGINT NOT NULL -- 옵션코드
  , channel_seq BIGINT NOT NULL -- 채널번호
  , seller_product_code TEXT -- 판매자상품코드
  , seller_option_code TEXT -- 옵션관리코드
  , product_type SMALLINT -- 상품종류
  , product_name TEXT -- 상품명
  , option_name TEXT -- 옵션명
  , sales_price INTEGER -- 판매가
  , option_price INTEGER -- 옵션가
  , update_date DATE -- 갱신일자
  , PRIMARY KEY (product_id, option_id)
);
CREATE INDEX IF NOT EXISTS smt_order_option__channel_idx ON smartstore.order_option (channel_seq);

-- [스마트스토어 주문 상태]
CREATE TABLE IF NOT EXISTS smartstore.order_status (
    product_order_id BIGINT NOT NULL -- 상품주문번호
  , order_id BIGINT NOT NULL -- 주문번호
  , channel_seq BIGINT NOT NULL -- 채널번호
  , order_status SMALLINT NOT NULL -- 주문상태
  , claim_status SMALLINT -- 클레임상태
  , payment_dt TIMESTAMP NOT NULL -- 결제일시
  , updated_dt TIMESTAMP NOT NULL -- 변경일시
  , PRIMARY KEY (payment_dt, product_order_id, order_status)
) PARTITION BY RANGE (payment_dt);
CREATE INDEX IF NOT EXISTS smt_order_status__channel_idx ON smartstore.order_status (channel_seq);

-- [스마트스토어 사용자 정의 채널]
CREATE TABLE IF NOT EXISTS smartstore.marketing_channel (
    channel_seq BIGINT NOT NULL -- 채널번호
  , device_category TEXT NOT NULL -- 채널속성
  , nt_source TEXT NOT NULL -- 마케팅채널 명칭
  , nt_medium TEXT NOT NULL -- 마케팅채널 종류
  , nt_detail TEXT NOT NULL -- 마케팅채널 하위 명칭
  , nt_keyword TEXT NOT NULL -- 키워드
  , num_users INTEGER -- 고객수
  , num_interactions INTEGER -- 유입수
  , page_view INTEGER -- 페이지수
  , num_purchases INTEGER -- 결제수
  , payment_amount INTEGER -- 결제금액
  , ymd DATE NOT NULL -- 날짜
  , PRIMARY KEY (ymd, channel_seq, device_category, nt_source, nt_medium, nt_detail, nt_keyword)
) PARTITION BY RANGE (ymd);

-- ============================================================
-- ss_hcenter  (네이버 쇼핑파트너센터)
-- ============================================================

-- [네이버 쇼핑 카테고리 그룹]
CREATE TABLE IF NOT EXISTS ss_hcenter.category_group (
    seq INTEGER NOT NULL -- 순번
  , group_id INTEGER NOT NULL -- 카테고리그룹ID
  , brand_name TEXT NOT NULL -- 브랜드
  , group_name TEXT NOT NULL -- 카테고리그룹
  , mapping_id INTEGER NOT NULL -- 맵핑카테고리ID
  , mapping_keyword TEXT -- 맵핑키워드
  , mapping_depth SMALLINT NOT NULL -- 맵핑단계
  , PRIMARY KEY (seq)
);

-- [네이버 쇼핑 판매처 목록]
CREATE TABLE IF NOT EXISTS ss_hcenter.mall (
    mall_seq BIGINT NOT NULL -- 판매처순번
  , mall_name TEXT NOT NULL -- 판매처명
  , mall_group TEXT NOT NULL -- 판매처그룹
  , mall_url TEXT NOT NULL -- 판매처주소
  , sales_date DATE -- 매출수집일
  , PRIMARY KEY (mall_seq)
);

-- [네이버 쇼핑 판매처 상품]
CREATE TABLE IF NOT EXISTS ss_hcenter.product (
    product_id BIGINT NOT NULL -- 상품코드
  , mall_seq BIGINT NOT NULL -- 판매처순번
  , category_id INTEGER -- 카테고리코드
  , category_id3 INTEGER -- 소분류코드
  , product_name TEXT -- 상품명
  , sales_price INTEGER -- 판매가
  , register_date DATE -- 등록일자
  , update_date DATE -- 갱신일자
  , PRIMARY KEY (product_id)
);

-- [네이버 쇼핑 상품 가격]
CREATE TABLE IF NOT EXISTS ss_hcenter.price (
    product_id BIGINT NOT NULL -- 상품코드
  , mall_seq BIGINT NOT NULL -- 판매처순번
  , category_id INTEGER -- 카테고리코드
  , sales_price INTEGER NOT NULL -- 판매가
  , created_at TIMESTAMP NOT NULL -- 수집일시
  , PRIMARY KEY (created_at, product_id)
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS ssh_price__mall_idx ON ss_hcenter.price (mall_seq);

-- [네이버 쇼핑 상품 - 카탈로그 관계 (시간별)]
CREATE TABLE IF NOT EXISTS ss_hcenter.product_catalog (
    product_id BIGINT NOT NULL -- 상품코드
  , catalog_id BIGINT NOT NULL -- 카탈로그코드
  , created_at TIMESTAMP NOT NULL -- 수집일시
  , PRIMARY KEY (created_at, product_id)
) PARTITION BY RANGE (created_at);

-- [네이버 쇼핑 판매처 방문 통계]
CREATE TABLE IF NOT EXISTS ss_hcenter.pageview (
    mall_seq BIGINT NOT NULL -- 판매처순번
  , page_id BIGINT NOT NULL -- 식별코드
  , page_click BIGINT -- 페이지 조회수
  , user_click BIGINT -- 페이지 조회 유저수
  , time_on_site BIGINT -- 체류시간(초)
  , ymd DATE NOT NULL -- 날짜
  , PRIMARY KEY (ymd, mall_seq, page_id)
) PARTITION BY RANGE (ymd);

-- [네이버 쇼핑 판매처 상품 매출]
CREATE TABLE IF NOT EXISTS ss_hcenter.sales (
    product_id BIGINT NOT NULL -- 상품코드
  , mall_seq BIGINT NOT NULL -- 판매처순번
  , category_id3 INTEGER -- 소분류코드
  , click_count INTEGER -- 조회수
  , payment_count INTEGER -- 결제수
  , payment_amount INTEGER -- 결제금액
  , payment_date DATE NOT NULL -- 결제일자
  , PRIMARY KEY (payment_date, product_id)
) PARTITION BY RANGE (payment_date);

-- [네이버 쇼핑 상품 재고]
CREATE TABLE IF NOT EXISTS ss_hcenter.stock (
    product_id BIGINT NOT NULL -- 상품코드
  , price INTEGER -- 판매가
  , sales_price INTEGER -- 할인가
  , stock_quantity INTEGER -- 재고수량
  , review_count INTEGER -- 리뷰수
  , review_score NUMERIC(3, 2) -- 평점
  , soldout BOOLEAN -- 품절여부
  , created_at TIMESTAMP NOT NULL -- 수집일시
  , PRIMARY KEY (created_at, product_id)
) PARTITION BY RANGE (created_at);

-- ============================================================
-- pg_partman 일별 파티션 초기화
-- ============================================================

-- 테이블별 시작일은 아래 VALUES 블록에서 조정한다.
-- 시작일(start_partition)로부터 (현재 + premake) 시점까지 초기 파티션을 일괄 생성한다.
CREATE OR REPLACE FUNCTION public.bootstrap_daily_partitions(
    p_parent_table TEXT,
    p_control_column TEXT,
    p_start_partition TEXT,
    p_interval TEXT,
    p_premake_days INTEGER
) RETURNS VOID AS $$
DECLARE
  has_partitions BOOLEAN;
  start_partition_ts TIMESTAMP;
  safe_start_partition_ts TIMESTAMP;
  end_partition_ts TIMESTAMP;
BEGIN
  start_partition_ts := p_start_partition::TIMESTAMP;
  safe_start_partition_ts := GREATEST(start_partition_ts, (CURRENT_DATE - INTERVAL '30 days')::TIMESTAMP);
  end_partition_ts := (CURRENT_DATE + p_premake_days)::TIMESTAMP;

  SELECT EXISTS (
    SELECT 1
    FROM pg_inherits i
    JOIN pg_class p ON p.oid = i.inhparent
    JOIN pg_namespace n ON n.oid = p.relnamespace
    WHERE n.nspname || '.' || p.relname = p_parent_table
  ) INTO has_partitions;

  IF NOT has_partitions THEN
    DELETE FROM partman.part_config
    WHERE parent_table = p_parent_table;

    PERFORM partman.create_parent(
      p_parent_table := p_parent_table,
      p_control := p_control_column,
      p_type := 'range',
      p_interval := p_interval,
      p_premake := p_premake_days,
      p_start_partition := safe_start_partition_ts::TEXT,
      p_automatic_maintenance := 'on',
      p_jobmon := false
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM partman.part_config
    WHERE parent_table = p_parent_table
  ) THEN
    PERFORM partman.create_partition_time(
      p_parent_table := p_parent_table,
      p_partition_times := ARRAY(
        SELECT generate_series(start_partition_ts, end_partition_ts, p_interval::interval)
      )::timestamptz[]
    );

    UPDATE partman.part_config
      SET premake = p_premake_days,
          automatic_maintenance = 'on',
          infinite_time_partitions = true
    WHERE parent_table = p_parent_table;
  END IF;
END;
$$ LANGUAGE plpgsql;

SELECT public.bootstrap_daily_partitions('core.expense',		              'ymd',					      '2026-05-01',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('core.extra_sales',              'sales_date',		      '2026-01-01',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('core.opex',				              'end_date',			      '2025-01-13',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('core.order_status',		          'order_date',			    '2025-04-03',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('cj_eflexs.invoice',			        'pickup_date',			  '2023-05-01',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('cj_eflexs.invoice_order',		    'order_date',			    '2023-05-01',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('cj_eflexs.stock',				        'updated_at',			    '2026-05-27 00:00:00',	'1 day',  35);
SELECT public.bootstrap_daily_partitions('cj_loisparcel.invoice',		      'register_date',		  '2025-08-01',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('coupang_ads.report_pa',		      'ymd',				        '2023-10-31',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('coupang_ads.report_nca',		    'ymd',				        '2025-01-07',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('coupang_rfm.inventory',		      'updated_at',			    '2026-05-27 00:00:00',	'1 day',  35);
SELECT public.bootstrap_daily_partitions('coupang_rfm.sales',			        'sales_date',			    '2023-08-07',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('coupang_rfm.shipping',		      'sales_date',			    '2023-08-04',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('ecount.inventory',			        'updated_at',			    '2026-05-27 00:00:00',	'1 day',  35);
SELECT public.bootstrap_daily_partitions('google_ads.insight',			      'ymd',				        '2023-09-06',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('meta_ads.insight',			        'ymd',				        '2024-05-20',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('naver_shp.rank',				        'created_at',			    '2025-08-15 00:00:00',  '1 day',  35);
SELECT public.bootstrap_daily_partitions('sabangnet.order',				        'order_dt',			      '2024-11-04 00:00:00',  '1 day',  35);
SELECT public.bootstrap_daily_partitions('sabangnet.order_invoice',		    'order_dt',			      '2024-11-04 00:00:00',  '1 day',  35);
SELECT public.bootstrap_daily_partitions('sabangnet.order_dispatch',		  'register_dt',			  '2023-12-12 00:00:00',  '1 day',  35);
SELECT public.bootstrap_daily_partitions('searchad.report_sad',			      'ymd',				        '2025-04-03',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('searchad.report_gfa',			      'ymd',				        '2024-06-25',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('searchad.contract',			        'contract_end_date',  '2022-06-23',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('searchad.rank',				          'created_at',			    '2025-08-15 00:00:00',  '1 day',  35);
SELECT public.bootstrap_daily_partitions('smartstore.order',			        'payment_dt',			    '2022-04-07 00:00:00',  '1 day',  35);
SELECT public.bootstrap_daily_partitions('smartstore.order_detail',		    'payment_dt',			    '2022-04-07 00:00:00',  '1 day',  35);
SELECT public.bootstrap_daily_partitions('smartstore.order_delivery',		  'payment_dt',			    '2022-04-07 00:00:00',  '1 day',  35);
SELECT public.bootstrap_daily_partitions('smartstore.order_status',	      'payment_dt',			    '2022-03-28 00:00:00',  '1 day',  35);
SELECT public.bootstrap_daily_partitions('smartstore.marketing_channel',  'ymd',				        '2024-06-09',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('ss_hcenter.product_catalog',		'created_at',			    '2025-08-15 00:00:00',	'1 day',  35);
SELECT public.bootstrap_daily_partitions('ss_hcenter.pageview',			      'ymd',				        '2023-12-13',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('ss_hcenter.price',			        'created_at',			    '2025-07-19 00:00:00',	'1 day',  35);
SELECT public.bootstrap_daily_partitions('ss_hcenter.sales',			        'payment_date',			  '2023-07-20',				    '1 day',  35);
SELECT public.bootstrap_daily_partitions('ss_hcenter.stock',			        'created_at',			    '2026-03-07 00:00:00',	'1 day',  35);
