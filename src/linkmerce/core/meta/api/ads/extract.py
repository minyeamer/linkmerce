from __future__ import annotations
from linkmerce.core.meta.api import MetaApi

from typing import TypedDict, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class AdAccount(TypedDict):
    """메타 광고 계정 정보."""
    account_status: int
    id: str # act_{ACCOUNT_ID}
    name: str


class MetaAds(MetaApi):
    """메타 마케팅 API 요청을 처리하는 공통 클래스.

    - **Docs**: https://developers.facebook.com/docs/marketing-api/reference/v24.0

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_token: str
        메타 액세스 토큰
    app_id: str | None
        메타 앱 ID (토큰 자동 갱신 시 필요)
    app_secret: str | None
        메타 앱 시크릿 (토큰 자동 갱신 시 필요)

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method: str = "GET"
    version: str = "v24.0"
    path: str | None = None
    default_options = {"RequestEach": {"request_delay": 1}}

    def _extract_backend(self, account_ids: Sequence[str] = list(), **kwargs) -> JsonObject:
        """광고 계정(`account_ids`)별 광고 데이터를 조회하는 공통 로직."""
        if not account_ids:
            account_ids = [account["id"] for account in self.list_accounts()]
        return (self.request_each(self.request_json_by_account)
                .partial(**kwargs)
                .expand(account_id=account_ids)
                .run())

    def request_json_by_account(self, account_id: str, **kwargs) -> JsonObject:
        """광고 계정별로 API 요청을 실행한다."""
        kwargs["url"] = self.concat_path(self.origin, self.version, account_id, self.path)
        return self.request_json_safe(**kwargs)

    def list_accounts(self) -> list[AdAccount]:
        """광고 계정 목록을 조회한다."""
        import json
        url = self.concat_path(self.origin, self.version, "/me/adaccounts")
        params = {"access_token": self.access_token, "fields": "id,name"}
        with self.request("GET", url, params=params) as response:
            return json.loads(response.text)["data"]

    def time_range(self, since: dt.date | str, until: dt.date | str) -> str:
        """날짜 범위를 JSON 문자열로 변환한다."""
        import json
        return json.dumps({"since": str(since), "until": str(until)})


class _AdObjects(MetaAds):
    """캠페인, 광고세트, 광고 등 광고 객체 목록을 조회하는 공통 클래스."""

    @property
    def fields(self) -> list[str]:
        """조회할 필드 목록을 정의한다."""
        return list()

    @MetaAds.with_session
    @MetaAds.auto_refresh_token
    def extract(
            self,
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            account_ids: Sequence[str] = list(),
            fields: Sequence[str] = list(),
            **kwargs
        ) -> JsonObject:
        """광고 객체 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date : dt.date | str | None
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 전달한다. 기본값은 `None`
        end_date : dt.date | str | None
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 전달한다. 기본값은 `None`
        account_ids : Sequence[str]
            조회할 광고 계정 ID 목록. 생략 시 사용 가능한 모든 계정을 조회한다.
        fields : Sequence[str]
            조회할 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.

        Returns
        -------
        list[dict]
            메타 광고 객체 목록
        """
        return self._extract_backend(account_ids, start_date=start_date, end_date=end_date, fields=fields)

    def build_request_params(
            self,
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            fields: Sequence[str] = list(),
            **kwargs
        ) -> dict[str, str]:
        return {
            "access_token": self.access_token,
            "fields": ','.join(fields if fields else self.fields),
            **({"time_range": self.time_range(start_date, end_date)} if start_date and end_date else dict()),
        }


class Campaigns(_AdObjects):
    """메타 광고 캠페인 보고서를 조회하는 클래스.

    - **API**: https://graph.facebook.com/v24.0/act_<AD_ACCOUNT_ID>/campaigns
    - **Docs**: https://developers.facebook.com/docs/marketing-api/reference/ad-account/campaigns/v24.0
    - **Referer**: https://adsmanager.facebook.com/adsmanager/manage/campaigns

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_token: str
        메타 액세스 토큰
    app_id: str | None
        메타 앱 ID (토큰 자동 갱신 시 필요)
    app_secret: str | None
        메타 앱 시크릿 (토큰 자동 갱신 시 필요)

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    path = "/campaigns"

    @property
    def fields(self) -> list[str]:
        return [
            "id", "name", "objective", "status", "effective_status", "created_time", # "insights",
        ]


class Adsets(_AdObjects):
    """메타 광고세트 보고서를 조회하는 클래스.

    - **API**: https://graph.facebook.com/v24.0/act_<AD_ACCOUNT_ID>/adsets
    - **Docs**: https://developers.facebook.com/docs/marketing-api/reference/ad-account/adsets/v24.0
    - **Referer**: https://adsmanager.facebook.com/adsmanager/manage/adsets

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_token: str
        메타 액세스 토큰
    app_id: str | None
        메타 앱 ID (토큰 자동 갱신 시 필요)
    app_secret: str | None
        메타 앱 시크릿 (토큰 자동 갱신 시 필요)

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    path = "/adsets"

    @property
    def fields(self) -> list[str]:
        return [
            "id", "name", "campaign_id", "status", "effective_status", "daily_budget", "created_time",
            # "targeting", "insights",
        ]


class Ads(_AdObjects):
    """메타 광고 보고서를 조회하는 클래스.

    - **API**: https://graph.facebook.com/v24.0/act_<AD_ACCOUNT_ID>/ads
    - **Docs**: https://developers.facebook.com/docs/marketing-api/reference/ad-account/ads/v24.0
    - **Referer**: https://adsmanager.facebook.com/adsmanager/manage/ads

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_token: str
        메타 액세스 토큰
    app_id: str | None
        메타 앱 ID (토큰 자동 갱신 시 필요)
    app_secret: str | None
        메타 앱 시크릿 (토큰 자동 갱신 시 필요)

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    path = "/ads"

    @property
    def fields(self) -> list[str]:
        return [
            "id", "name", "campaign_id", "adset_id", "status", "effective_status", "creative", "created_time",
            # "configured_status", "source_ad_id", "tracking_specs", "insights",
        ]


class Insights(MetaAds):
    """메타 광고 성과 보고서를 날짜별로 조회하는 클래스.

    - **API**: https://graph.facebook.com/v24.0/act_<AD_ACCOUNT_ID>/insights
    - **Docs**: https://developers.facebook.com/docs/marketing-api/reference/ad-account/insights/v24.0

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_token: str
        메타 액세스 토큰
    app_id: str | None
        메타 앱 ID (토큰 자동 갱신 시 필요)
    app_secret: str | None
        메타 앱 시크릿 (토큰 자동 갱신 시 필요)

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    path = "/insights"

    @MetaAds.with_session
    @MetaAds.auto_refresh_token
    def extract(
            self,
            ad_level: Literal["campaign", "adset", "ad"],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["daily", "total"] = "daily",
            account_ids: Sequence[str] = list(),
            fields: Sequence[str] = list(),
            **kwargs
        ) -> JsonObject:
        """메타 광고 성과 보고서를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        ad_level : Literal["campaign", "adset", "ad"]
            보고서 집계 기준
                - `"campaign"`: 캠페인
                - `"adset"`: 광고세트
                - `"ad"`: 광고
        start_date : dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 전달한다.
        end_date : dt.date | str | Literal[":start_date:"]
            조회 종료일. `":start_date:"` 전달 시 `start_date`와 동일한 날짜로 대체된다.
            기본값은 `":start_date:"`
        date_type : Literal["daily", "total"]
            보고서 기간 구분
                - `"total"`: 합계
                - `"daily"`: 일별
        account_ids : Sequence[str]
            조회할 광고 계정 ID 목록. 생략 시 사용 가능한 모든 계정을 조회한다.
        fields : Sequence[str]
            조회할 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.

        Returns
        -------
        list[dict]
            메타 광고 성과 보고서
        """
        dates = dict(start_date=start_date, end_date=(start_date if end_date == ":start_date:" else end_date))
        return self._extract_backend(account_ids, ad_level=ad_level, **dates, date_type=date_type, fields=fields)

    def build_request_params(
            self,
            ad_level: Literal["campaign", "adset", "ad"],
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily", "total"] = "daily",
            fields: Sequence[str] = list(),
            **kwargs
        ) -> dict[str, str]:
        return {
            "access_token": self.access_token,
            "fields": ','.join(fields if fields else self.fields),
            "level": ad_level,
            "time_range": self.time_range(start_date, end_date),
            **({"time_increment": 1} if date_type == "daily" else dict()),
            "limit": 5000,
        }

    @property
    def fields(self) -> list[str]:
        return [
            "date_start", "date_stop",
            "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name",
            "impressions", "reach", "frequency", "clicks", "inline_link_clicks",
            "spend", # "actions", "action_values",
        ]
