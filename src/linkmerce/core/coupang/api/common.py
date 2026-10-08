from __future__ import annotations

from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class CoupangApi(Extractor):
    """쿠팡 Open API의 HMAC 인증과 조회 요청을 처리하는 공통 클래스.

    - **Docs**: https://developers.coupang.com/ko/api

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    """

    method: str | None = None
    origin = "https://api-gateway.coupang.com"
    path: str | None = None
    config_fields = ["access_key", "secret_key", "vendor_id"]

    @property
    def vendor_id(self) -> str:
        return self.get_config("vendor_id")

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path.format(vendor_id=self.vendor_id))

    def request_json(self, **kwargs) -> dict:
        """HTTP 요청을 수행하고 응답 본문을 JSON 형식의 객체로 반환한다."""
        response = super().request_json(**kwargs)
        if isinstance(response, dict) and (response.get("code") not in ("SUCCESS", 200, "200")):
            from linkmerce.common.exceptions import RequestError
            code = response.get("code") or "ERROR"
            message = response.get("message") or "오류가 발생하였습니다."
            raise RequestError(f"{code}: {message}")
        return response

    def set_request_headers(self, **kwargs):
        common = {"Accept": "application/json", "Content-Type": "application/json"}
        super().set_request_headers(headers=common)

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        return self.get_request_headers() | {"Authorization": self.build_authorization(**kwargs)}

    def build_authorization(self, method: str | None = None, **kwargs) -> str:
        """UTC 서명 시각과 전송 URL을 사용해 HmacSHA256 인증 문자열을 생성한다."""
        from urllib.parse import urlencode
        import datetime as dt
        import hmac, hashlib

        datetime = dt.datetime.now(dt.timezone.utc).strftime("%y%m%dT%H%M%SZ")
        method = method if method else self.method
        query = urlencode(self.build_request_params(**kwargs) or dict())
        message = datetime + method + self._get_path(**kwargs) + query

        secret_key: str = self.get_config("secret_key")

        signature = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
        return (f"CEA algorithm=HmacSHA256, access-key={self.get_config('access_key')}, "
                f"signed-date={datetime}, signature={signature}")

    def _get_path(self, url: str | None = None, **kwargs) -> str:
        """키워드 인자에 완성된 URL이 있으면 `path`를 추출하고, 없으면 기본 `path`에 `vendor_id`를 대입하여 반환한다."""
        if url is not None:
            from urllib.parse import urlsplit
            return urlsplit(url).path
        return self.path.format(vendor_id=self.vendor_id)


class CoupangTestApi(CoupangApi):
    """쿠팡 Open API 경로에 대한 요청을 처리하는 테스트 클래스.

    - **Docs**: https://developers.coupang.com/ko/api

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    """

    @CoupangApi.with_session
    def extract(
            self,
            method: str,
            path: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | None = None,
            json: dict | None = None,
            headers: dict[str, str] = None,
            **kwargs
        ) -> dict:
        """쿠팡 Open API 경로와 메시지를 전달하면 응답 결과를 JSON 형식으로 반환한다.

        Parameters
        ----------
        method: str
            HTTP 메서드
        path: str
            쿠팡 Open API 경로
        params: dict | list[tuple] | bytes | None
            쿠팡 Open API 요청 파라미터
        data: dict | list[tuple] | bytes | None
            쿠팡 Open API 요청 본문
        json: dict | None
            쿠팡 Open API 요청 본문 (JSON)
        headers: dict[str, str]
            쿠팡 Open API 요청 헤더

        Returns
        -------
        dict
            쿠팡 Open API 응답 결과
        """
        url = self.concat_path(self.origin, path)
        message = self.build_request_message(method=method, url=url, **kwargs)

        if params is not None: message["params"] = params
        if data is not None: message["data"] = data
        if json is not None: message["json"] = json
        if isinstance(headers, dict):
            if isinstance(message["headers"], dict):
                message["headers"].update(headers)
            else:
                message["headers"] = headers

        with self.get_session().request(**message) as response:
            return response.json()


def strftime(datetime: dt.datetime) -> str:
    """쿠팡 API 요청 일시를 KST ISO-8601 형식의 문자열로 변환한다."""
    if not datetime.tzinfo:
        return datetime.isoformat(timespec="minutes") + "+09:00"
    return datetime.isoformat(timespec="minutes")


def split_datetime_context(
        start_datetime: dt.datetime | str,
        end_datetime: dt.datetime | str | Literal[":end_of_day:", ":max_window:"],
        format: str = "%Y-%m-%dT%H:%M%z",
        days_interval: int = 1,
    ) -> dict[str, dt.datetime] | list[dict[str, dt.datetime]]:
    """분 단위 발주서 조회 기간을 최대 23시간 59분의 연속 구간으로 분할한다."""
    from linkmerce.utils.date import strptime, dt
    start_datetime = strptime(start_datetime, format, astimezone="Asia/Seoul").replace(second=0, microsecond=0)

    if isinstance(end_datetime, str):
        if end_datetime == ":end_of_day:":
            end_datetime = start_datetime.replace(hour=23, minute=59)
            return {"start_datetime": start_datetime, "end_datetime": end_datetime}
        elif end_datetime == ":max_window:":
            end_datetime = start_datetime + dt.timedelta(hours=23, minutes=59)
            return {"start_datetime": start_datetime, "end_datetime": end_datetime}

    end_datetime = strptime(end_datetime, format, astimezone="Asia/Seoul").replace(second=0, microsecond=0)
    if end_datetime < start_datetime:
        raise ValueError("The end_datetime must not be earlier than the start_datetime.")

    context = list()
    while start_datetime <= end_datetime:
        segment_end = min(start_datetime + dt.timedelta(days=(days_interval-1), hours=23, minutes=59), end_datetime)
        context.append({"start_datetime": start_datetime, "end_datetime": segment_end})
        start_datetime = segment_end + dt.timedelta(minutes=1)
    return context[0] if len(context) == 1 else context
