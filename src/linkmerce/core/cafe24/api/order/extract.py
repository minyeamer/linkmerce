from __future__ import annotations
import datetime as dt

from linkmerce.core.cafe24.api import Cafe24Api

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Iterable


def _strftime(datetime: dt.datetime) -> str:
    """카페24 API 요청 일시를 KST ISO-8601 형식의 문자열로 변환한다."""
    if not datetime.tzinfo:
        return datetime.isoformat(timespec="seconds") + "+09:00"
    return datetime.isoformat(timespec="seconds")


def _parse_datetime_range(
        start_datetime: dt.datetime | str,
        end_datetime: dt.datetime | str | Literal[":end_of_day:"],
        days_limit: int = 89,
        format: str = "%Y-%m-%dT%H:%M:%S%z",
    ) -> dict[str, dt.datetime]:
    """카페24 API 조회 기간을 파싱 및 검증한다."""
    from linkmerce.utils.date import strptime
    start_datetime = strptime(start_datetime, format, astimezone="Asia/Seoul")

    if isinstance(end_datetime, str):
        if end_datetime == ":end_of_day:":
            end_datetime = start_datetime.replace(hour=23, minute=59, second=59)
            return {"start_datetime": start_datetime, "end_datetime": end_datetime}

    end_datetime = strptime(end_datetime, format, astimezone="Asia/Seoul")
    if end_datetime < start_datetime:
        raise ValueError("The end_datetime must not be earlier than the start_datetime.")
    elif (end_datetime - start_datetime).days > days_limit:
        raise ValueError(f"Date range must be less than {days_limit} days per request.")
    return {"start_datetime": start_datetime, "end_datetime": end_datetime}


class Order(Cafe24Api):
    """카페24 Admin API로 주문 목록 조회 결과를 수집하는 클래스.

    - **API**: https://{mall_id}.cafe24api.com/api/v2/admin/orders
    - **Docs**: https://apidocs.cafe24.com/docs/admin/get-orders
    - **Referer**: https://{mall_id}.cafe24.com/admin/php/shop1/s_new/order_list.php

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.

    **NOTE** `token_info`를 파일 경로로 전달하면 토큰 갱신 시 파일 내용도 업데이트한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `CursorAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        커서 요청 간 대기 시간(초). 기본값은 `0.5`
    """

    path = "/admin/orders"
    datetime_format = "%Y-%m-%dT%H:%M:%S%z"
    days_limit = 89
    default_options = {"CursorAll": {"request_delay": 0.5}}

    @Cafe24Api.with_session
    def extract(
            self,
            start_datetime: dt.datetime | str,
            end_datetime: dt.datetime | str | Literal[":end_of_day:"] = ":end_of_day:",
            date_type: str = "order_date",
            order_status: str | Iterable[str] | None = None,
            shop_no: int = 1,
            embed: str | None = None,
            page_size: int = 100,
            offset: int = 0,
            **kwargs
        ) -> list[dict]:
        """최대 3개월(89일) 내 검색 기간에 대한 주문 목록을 페이지 단위로 수집해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_datetime: dt.datetime | str
            검색 시작 일시. `dt.datetime` 객체 또는 ISO 8601 형식의 문자열을 입력한다.
        end_datetime: dt.datetime | str
            검색 종료 일시. `dt.datetime` 객체 또는 ISO 8601 형식의 문자열을 입력한다.
                - `":end_of_day:"`: `start_datetime`의 하루 중 마지막 시점 (기본값)
        date_type: str
            검색날짜 유형. `date_type` 속성의 키를 전달할 수 있다. 기본값은 주문일(`"order_date"`)
        order_status: str | Iterable[str] | None
            주문상태. `order_status` 속성의 키를 단일 값 또는 배열로 전달할 수 있다. 생략하면 전체 상태를 조회한다.
        shop_no: int
            멀티쇼핑몰 번호. 기본값은 `1`
        embed: str | None
            함께 조회할 하위 리소스. 쉼표(,)로 구분하여 여러 개를 지정할 수 있다.
            - 사용 가능한 값: items, receivers, buyer, benefits, coupons, return, cancellation, exchange, refunds
        page_size: int
            조회결과 최대건수. 기본값은 `100`, 최대 `1000`
        offset: int
            조회결과 시작위치. 기본값은 `0`, 최대 `15000`

        Returns
        -------
        list[dict]
            주문 목록
        """
        status = order_status if isinstance(order_status, str) else ','.join(order_status or list())
        return (self.cursor_all(self.request_json, self.get_next_cursor, next_cursor = offset)
                .run(
                    **_parse_datetime_range(start_datetime, end_datetime, self.days_limit),
                    date_type = date_type,
                    order_status = status,
                    shop_no = shop_no,
                    embed = embed,
                    page_size = page_size,
                    mall_id = self.get_config("mall_id"),
                ))

    def get_next_cursor(self, response: dict, **context) -> int | None:
        """다음 페이지를 가리키는 `href` 속성값으로부터 `offset` 파라미터를 추출한다."""
        for link in (response.get("links") or list()):
            if isinstance(link, dict) and (link.get("rel") == "next") and link.get("href"):
                from urllib.parse import parse_qs, urlparse
                query = parse_qs(urlparse(str(link["href"])).query)
                return int(query["offset"][0])
        return None

    def build_request_params(
            self,
            start_datetime: dt.datetime,
            end_datetime: dt.datetime,
            date_type: str = "order_date",
            order_status: str = str(),
            shop_no: int = 1,
            embed: str | None = None,
            page_size: int = 100,
            next_cursor: int = 0,
            **kwargs
        ) -> dict:
        return {
            "shop_no": shop_no,
            "start_date": _strftime(start_datetime),
            "end_date": _strftime(end_datetime),
            "date_type": date_type,
            **({"order_status": order_status} if order_status else dict()),
            **({"embed": embed} if embed else dict()),
            "limit": page_size,
            "offset": next_cursor,
        }

    @property
    def date_type(self) -> dict[str, str]:
        """검색날짜 유형 코드와 한글명 매핑을 반환한다."""
        return {
            "order_date": "주문일", "pay_date": "결제일", "shipbegin_date": "배송시작일",
            "shipend_date": "배송완료일", "cancel_date": "주문취소일", "place_date": "발주일",
            "cancel_request_date": "취소신청일", "cancel_accept_date": "취소접수일",
            "cancel_complete_date": "취소완료일", "exchange_request_date": "교환신청일",
            "exchange_accept_date": "교환접수일", "exchange_complete_date": "교환완료일",
            "return_request_date": "반품신청일", "return_accept_date": "반품접수일",
            "return_complete_date": "반품완료일", "purchaseconfirmation_date": "구매확정일",
            "memo_date": "메모 작성일", "updatereceiver_date": "수령자정보 수정일",
        }

    @property
    def order_status(self) -> dict[str, str]:
        """주문상태 코드와 한글명 매핑을 반환한다."""
        return {
            "N00": "입금전", "N01": "교환접수 - 교환상품", "N02": "입금전 - 카드결제대기",
            "N03": "교환접수 - 카드결제대기", "N10": "상품준비중", "N20": "배송준비중",
            "N21": "배송대기", "N22": "배송보류", "N30": "배송중", "N40": "배송완료", "N50": "구매확정",
            "C00": "취소신청", "C10": "취소접수 - 관리자", "C11": "취소접수거부 - 관리자",
            "C34": "취소처리중 - 환불전", "C35": "취소처리중 - 환불완료", "C36": "취소처리중 - 환불보류",
            "C40": "취소완료", "C41": "취소 완료 - 환불전", "C42": "취소 완료 - 환불요청중",
            "C43": "취소 완료 - 환불보류", "C47": "입금전취소 - 구매자", "C48": "입금전취소 - 자동취소",
            "C49": "입금전취소 - 관리자", "E00": "교환신청", "E10": "교환접수", "E11": "교환접수거부",
            "E12": "교환보류", "E13": "교환접수 - 수거완료(자동)", "E20": "교환준비", "E30": "교환처리중 - 수거전",
            "E31": "교환처리중 - 수거완료", "E32": "교환처리중 - 입금전", "E33": "교환처리중 - 입금완료",
            "E34": "교환처리중 - 환불전", "E35": "교환처리중 - 환불완료", "E36": "교환처리중 - 환불보류",
            "E40": "교환완료", "R00": "반품신청", "R10": "반품접수", "R11": "반품 접수 거부",
            "R12": "반품보류", "R13": "반품접수 - 수거완료(자동)", "R20": "반품 수거 완료",
            "R30": "반품처리중 - 수거전", "R31": "반품처리중 - 수거완료", "R34": "반품처리중 - 환불전",
            "R35": "반품처리중 - 환불완료", "R36": "반품처리중 - 환불보류", "R40": "반품완료 - 환불완료",
            "R41": "반품완료 - 환불전", "R42": "반품완료 - 환불요청중", "R43": "반품완료 - 환불보류",
        }


class OrderDetail(Order):
    """카페24 Admin API로 주문번호별 주문 상세 조회 결과를 수집하는 클래스.

    - **API**: https://{mall_id}.cafe24api.com/api/v2/admin/orders/{order_id}
    - **Docs**: https://apidocs.cafe24.com/docs/admin/get-orders-by-order-id
    - **Referer**: https://{mall_id}.cafe24.com/admin/php/shop1/s_new/order_list.php?order_id={order_id}

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.

    **NOTE** `token_info`를 파일 경로로 전달하면 토큰 갱신 시 파일 내용도 업데이트한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        주문별 요청 간 대기 시간(초). 기본값은 `0.5`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    response_key = "order"
    default_options = {"RequestEach": {"request_delay": 0.5}}

    @Cafe24Api.with_session
    def extract(
            self,
            order_id: str | Iterable[str],
            shop_no: int = 1,
            embed: str | None = None,
            **kwargs
        ) -> dict | list[dict]:
        """주문번호별 주문 상세 내역을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        order_id: str | Iterable[str]
            주문번호. 단일 값 또는 배열
        shop_no: int
            멀티쇼핑몰 번호. 기본값은 `1`
        embed: str | None
            함께 조회할 하위 리소스. 쉼표(,)로 구분하여 여러 개를 지정할 수 있다.
            - 사용 가능한 값: items, receivers, buyer, benefits, coupons, return, cancellation, exchange, refunds

        Returns
        -------
        dict | list[dict]
            주문 상세 내역. `order_id` 타입에 따라 반환 타입이 다르다.
                - `order_id`가 `str` 타입일 때 -> `dict`
                - `order_id`가 `Iterable[str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json)
                .partial(shop_no=shop_no, embed=embed, mall_id=self.get_config("mall_id"))
                .expand(order_id=order_id)
                .run())

    def build_request_message(self, order_id: str, **kwargs) -> dict:
        """주문번호를 카페24 주문 상세 조회 경로에 붙인다."""
        kwargs["url"] = self.concat_path(self.url, order_id)
        return super().build_request_message(**kwargs)

    def build_request_params(
            self,
            shop_no: int = 1,
            embed: str | None = None,
            **kwargs
        ) -> dict:
        return {"shop_no": shop_no, **({"embed": embed} if embed else dict())}


class OrderChange(Cafe24Api):
    """카페24 Admin API로 품목별 상태 변경 주문 목록을 수집하는 클래스.

    **NOTE** 해당 API는 특정 클라이언트만 사용할 수 있으며, 카페24 개발센터에 사용 권한을 신청해야 한다.

    - **API**: https://{mall_id}.cafe24api.com/api/v2/admin/orders/changes
    - **Docs**: https://apidocs.cafe24.com/docs/admin/get-orders-changes

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.

    **NOTE** `token_info`를 파일 경로로 전달하면 토큰 갱신 시 파일 내용도 업데이트한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `CursorAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        커서 요청 간 대기 시간(초). 기본값은 `0.5`
    """

    path = "/admin/orders/changes"
    datetime_format = "%Y-%m-%dT%H:%M:%S%z"
    days_limit = 3
    default_options = {"CursorAll": {"request_delay": 0.5}}

    @Cafe24Api.with_session
    def extract(
            self,
            start_datetime: dt.datetime | str,
            end_datetime: dt.datetime | str | Literal[":end_of_day:"] = ":end_of_day:",
            shop_no: int = 1,
            page_size: int = 100,
            offset: int = 0,
            **kwargs
        ) -> list[dict]:
        """최대 3일 내 검색 기간에 대한 품목별 상태 변경 주문 목록을 페이지 단위로 수집해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_datetime: dt.datetime | str
            검색 시작 일시. `dt.datetime` 객체 또는 ISO 8601 형식의 문자열을 입력한다.
        end_datetime: dt.datetime | str
            검색 종료 일시. `dt.datetime` 객체 또는 ISO 8601 형식의 문자열을 입력한다.
                - `":end_of_day:"`: `start_datetime`의 하루 중 마지막 시점 (기본값)
        shop_no: int
            멀티쇼핑몰 번호. 기본값은 `1`
        page_size: int
            조회결과 최대건수. 기본값은 `100`, 최대 `1000`
        offset: int
            조회결과 시작위치. 기본값은 `0`, 최대 `15000`

        Returns
        -------
        list[dict]
            품목별 상태 변경 주문 목록
        """
        return (self.cursor_all(self.request_json, self.get_next_cursor, next_cursor = offset)
                .run(
                    **_parse_datetime_range(start_datetime, end_datetime, self.days_limit),
                    shop_no = shop_no,
                    page_size = page_size,
                    mall_id = self.get_config("mall_id"),
                ))

    def get_next_cursor(self, response: dict, **context) -> int | None:
        """다음 페이지를 가리키는 `href` 속성값으로부터 `offset` 파라미터를 추출한다."""
        for link in (response.get("links") or list()):
            if isinstance(link, dict) and (link.get("rel") == "next") and link.get("href"):
                from urllib.parse import parse_qs, urlparse
                query = parse_qs(urlparse(str(link["href"])).query)
                return int(query["offset"][0])
        return None

    def build_request_params(
            self,
            start_datetime: dt.datetime,
            end_datetime: dt.datetime,
            shop_no: int = 1,
            next_cursor: int = 0,
            page_size: int = 100,
            **kwargs
        ) -> dict:
        return {
            "shop_no": shop_no,
            "start_date": _strftime(start_datetime),
            "end_date": _strftime(end_datetime),
            "limit": page_size,
            "offset": next_cursor,
        }
