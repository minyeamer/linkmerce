from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def has_accounts(cookies: str) -> bool:
    """네이버 스마트스토어센터 로그인 쿠키로 판매자 정보가 조회되는지 검증한다.

    Parameters
    ----------
    cookies: str
        스마트스토어센터 로그인 쿠키 문자열

    Returns
    -------
    bool
        판매자 정보가 조회되면 `True`, 아니면 `False`를 반환한다.
    """
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
    """네이버 스마트스토어센터에 로그인하고 쿠키 문자열을 반환한다.

    Parameters
    ----------
    userid: str | None
        스마트스토어센터 판매자 아이디/이메일
    passwd: str | None
        스마트스토어센터 판매자 비밀번호
    channel_seq: int | str | None
        채널 번호. 로그인 후 접속된 채널이 다르면 채널 번호에 맞는 채널로 전환한다.
    cookies: str | None
        네이버 로그인 쿠키 문자열. 판매자 아이디 로그인을 대체하여 네이버 아이디로 로그인한다.
    save_to: str | Path | None
        로그인 후 쿠키를 저장할 파일 경로. 상위 경로가 없으면 자동 생성한다.

    Returns
    -------
    str
        네이버 스마트스토어센터 쿠키 문자열
    """
    from linkmerce.core.smartstore.sscenter.common import SmartstoreCenterLogin
    from linkmerce.api.common import handle_cookies
    handler = SmartstoreCenterLogin()
    handler.login(userid, passwd, channel_seq, cookies)
    return handle_cookies(handler, save_to)
