from __future__ import annotations

from linkmerce.common.extract import Extractor
from linkmerce.common.transform import JsonTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class GmarketAdCenter(Extractor):
    """Gmarket 광고센터 로그인 쿠키를 가지고 데이터를 조회하는 공통 클래스.

    - **URL**: https://adcenter.esmplus.com

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    method: str | None = None
    origin = "https://adcenter.esmplus.com"
    path: str | None = None
    action_name: str | None = None

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    @property
    def next_action(self) -> str:
        return self.get_next_action(self.path, self.action_name)

    def post_init(self, **kwargs):
        self.require_cookies()

    def request_text(self, **kwargs) -> str:
        """HTTP 요청을 수행하고 응답 본문을 UTF-8 텍스트로 반환한다."""
        return self.request_content(**kwargs).decode("utf-8")

    def set_request_headers(self, **kwargs):
        super().set_request_headers(
            accept = "text/x-component",
            contents = {"type": "text", "charset": "UTF-8"},
            host = self.origin,
            origin = self.origin,
            **kwargs,
        )

    def get_next_action(self, path: str, action_name: str) -> str:
        """요청 경로의 RSC 응답과 JavaScript 리소스에서 함수 이름에 대응하는 최신 action ID를 조회한다.

        Parameters
        ----------
        path: str
            Gmarket 광고센터 내 요청 경로
        action_name: str
            JavaScript의 `createServerReference`에 등록된 함수 이름

        Returns
        -------
        str
            요청 헤더의 `next-action` 값
        """
        import re
        from uuid import uuid4
        from linkmerce.common.exceptions import RequestError

        url = self.concat_path(self.origin, path)
        params = {"_rsc": uuid4().hex}

        excludes = {"next-action", "next-router-prefetch", "next-router-state-tree", "rsc"}
        headers = {key: value for key, value in self.get_request_headers().items()
                if key.lower() not in excludes} | {"RSC": "1", "Cache-Control": "no-cache"}

        with self.request("GET", url, params=params, headers=headers, timeout=30) as response:
            response.raise_for_status()
            if "text/x-component" not in (response.headers.get("Content-Type") or str()):
                raise RequestError("Expected an RSC response while discovering server actions.")
            chunks = dict.fromkeys(re.findall(
                r'/_next/static/chunks/[\w/-]+\.js', response.content.decode("utf-8"),
            ))

        pattern = re.compile(
            r'createServerReference\)\(\s*[\'"]([a-f0-9]{42})[\'"],'
            r'[^()]*?findSourceMapURL,\s*[\'"]' + re.escape(action_name) + r'[\'"]\s*\)',
        )
        for chunk in chunks:
            with self.request("GET", self.origin + chunk, headers=headers, timeout=30) as response:
                response.raise_for_status()
                match = pattern.search(response.content.decode("utf-8"))
                if match:
                    return match.group(1)
        raise RequestError(f"Server action '{action_name}' not found for path '{path}'.")


class GmarketAdParser(JsonTransformer):
    """Gmarket 광고센터 응답 텍스트에서 JSON 데이터를 추출 및 파싱하는 공통 클래스."""

    scope: str | None = None
    fields: dict | list | None = None
    extends: dict | None = None
    on_missing: Literal["ignore", "raise"] = "raise"

    def transform(self, obj: str, **kwargs) -> list[dict]:
        """HTTP 응답 데이터 파싱 > scope 탐색 > 필드 선택 및 변환 순서로 파이프라인을 실행한다."""
        data = self.parse(obj, **kwargs)
        data = self.get_scope(data, **kwargs)
        return self.select_fields(data, **kwargs)

    def parse(self, obj: str, **kwargs) -> dict:
        """텍스트를 줄바꿈 문자로 구분하고, JSON 데이터가 담긴 줄을 찾아서 딕셔너리 객체로 변환한다."""
        import json

        for line in obj.split('\n'):
            if line.startswith("1:"):
                result = json.loads(line[2:])
                if isinstance(result, dict) and result.get("success"):
                    return result
                else:
                    from linkmerce.common.exceptions import RequestError
                    raise RequestError("Gmarket Ad Center request failed.")

        from linkmerce.common.exceptions import ParseError
        raise ParseError("Could not find the '1:' record in the response.")


def get_date_pair(
        start_date: dt.date | str | Literal[":today:"] = ":today:",
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
    ) -> dict[str, str]:
    """Gmarket 광고센터 조회용 날짜 쌍을 생성한다."""
    import datetime as dt

    if isinstance(start_date, str) and (start_date == ":today:"):
        start_date = dt.date.today()
    start_date = str(start_date).replace('-', '')

    if isinstance(end_date, str) and (end_date == ":start_date:"):
        return {"start_date": start_date, "end_date": start_date}
    return {"start_date": start_date, "end_date": str(end_date).replace('-', '')}
