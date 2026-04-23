from __future__ import annotations
from linkmerce.core.cj.eflexs import CjEflexs

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Stock(CjEflexs):
    """CJ eFLEXs 상세 재고 현황을 조회하는 클래스.

    - **Menu**: 재고관리 > 현황 > 상세재고현황 (`IMSI0002M`)
    - **API**: https://eflexs-x.cjlogistics.com/IMSI0002M/selectDtlStckSearch.do
    - **Referer**: https://eflexs-x.cjlogistics.com/index.do

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        CJ eFLEXs 로그인을 위한 User ID
    passwd: str
        CJ eFLEXs 로그인을 위한 Password
    mail_info: dict[str, str]
        2단계 인증을 위한 이메일 정보. 다음 키값을 포함해야 한다.
            - `origin`: 메일 서비스 도메인.
            - `email`: 메일 계정 아이디.
            - `passwd`: 메일 계정 비밀번호.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    menu = "IMSI0002M"
    path = "/selectDtlStckSearch.do"
    date_format = "%Y%m%d"
    default_options = {"RequestEach": {"request_delay": 1}}

    @CjEflexs.with_session
    @CjEflexs.with_auth_info
    def extract(
            self,
            customer_id: int | str | Iterable,
            start_date: dt.date | str | Literal[":last_week:"] = ":last_week:",
            end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
            **kwargs
        ) -> JsonObject:
        """고객(`customer_id`)별 상세 재고 현황을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        customer_id: int | str | Iterable
            조회할 고객 ID. 여러 고객을 조회하려면 리스트로 전달한다.
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 전달한다.
                - `":last_week:"` 전달 시 오늘 기준 7일 전 날짜로 대체된다.
                - 기본값은 `":last_week:"`
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 전달한다.
                - `":start_date:"` 전달 시 `start_date`와 동일한 날짜 값으로 대체된다.
                - `":today:"` 전달 시 오늘 날짜로 대체된다.
                - 기본값은 `":today:"`

        Returns
        -------
        list[dict]
            고객별 상세 재고 현황
        """
        return (self.request_each(self.request_json)
                .partial(**self.set_date(start_date, end_date))
                .expand(customer_id=customer_id)
                .run())

    def set_date(self, start_date: dt.date | str, end_date: dt.date | str) -> dict[str, dt.date]:
        """미리 정의된 날짜 문자열이 주어진다면 `dt.date` 객체로 치환한다."""
        import datetime as dt

        if start_date == ":last_week:":
            start_date = dt.date.today() - dt.timedelta(days=7)

        if end_date == ":start_date:":
            end_date = start_date
        elif end_date == ":today:":
            end_date = dt.date.today()

        return {"start_date": start_date, "end_date": end_date}

    def build_request_data(
            self,
            customer_id: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            page: int = 0,
            page_size: int = 100000,
            **kwargs
        ) -> dict:
        return {
            "pgmId": self.menu,
            "requestDataIds": "dmMainParam",
            "@d1#strrId": str(customer_id),
            "@d1#oWhCd": None,
            "@d1#srchZoneCd": None,
            "@d1#srchZoneNm": None,
            "@d1#srchItemNm": None,
            "@d1#srchItemCd": None,
            "@d1#srchWcellNm": None,
            "@d1#srchWcellTcd": None,
            "@d1#srchLotNo": None,
            "@d1#srchItemRarcode": None,
            "@d1#srchHldScd": None,
            "@d1#fromCloseDate": str(start_date).replace('-', ''),
            "@d1#toCloseDate": str(end_date).replace('-', ''),
            "@d1#srchMallId": None,
            "@d1#page": page,
            "@d1#pageRow": page_size,
            "@d1#srchLotNo7": None,
            "@d1#srchLotNo10": None,
            "@d1#itemGcd": None,
            "@d#": "@d1#",
            "@d1#": "dmMainParam",
            "@d1#tp": "dm",
        }
