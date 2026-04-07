from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def has_accounts(cookies: str) -> bool:
    """스마트스토어센터 로그인 쿠키로 판매자 정보가 조회되는지 검증한다."""
    from linkmerce.core.smartstore.sscenter.common import has_accounts
    import requests

    with requests.Session() as session:
        cookies_map = dict([kv.split('=', maxsplit=1) for kv in cookies.split("; ") if '=' in kv])
        session.cookies.update(cookies_map)
        return has_accounts(session)


def login(
        userid: str | None = None,
        passwd: str | None = None,
        channel_seq: int | str | None = None,
        cookies: str | None = None,
        save_to: str | Path | None = None,
    ) -> str:
    """스마트스토어센터에 로그인하고 쿠키를 반환한다."""
    from linkmerce.core.smartstore.sscenter.common import SmartstoreCenterLogin
    from linkmerce.api.common import handle_cookies
    handler = SmartstoreCenterLogin()
    handler.login(userid, passwd, channel_seq, cookies)
    return handle_cookies(handler, save_to)
