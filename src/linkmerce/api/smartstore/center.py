from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def has_accounts(cookies: str) -> bool:
    from linkmerce.core.smartstore.center.common import has_accounts
    import requests

    with requests.Session() as session:
        return has_accounts(session, cookies)


def login(
        userid: str | None = None,
        passwd: str | None = None,
        channel_seq: int | str | None = None,
        cookies: str | None = None,
        save_to: str | Path | None = None,
    ) -> str:
    from linkmerce.core.smartstore.center.common import SmartstoreLogin
    handler = SmartstoreLogin()
    handler.login(userid, passwd, channel_seq, cookies)
    cookies = handler.get_cookies()
    if cookies and save_to:
        with open(save_to, 'w', encoding="utf-8") as file:
            file.write(cookies)
    return cookies
