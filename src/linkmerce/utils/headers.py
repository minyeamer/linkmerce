from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal


CHROME_VERSION = 138


def build_headers(
        authority: str = str(),
        accept: str = "*/*",
        encoding: str = "gzip, deflate, br",
        language: Literal["ko","en"] | str = "ko",
        connection: str = "keep-alive",
        contents: Literal["form", "javascript", "json", "text", "multipart"] | str | dict = str(),
        cookies: str = str(),
        host: str = str(),
        origin: str = str(),
        priority: str = "u=0, i",
        referer: str = str(),
        client: str = str(),
        mobile: bool = False,
        platform: str = str(),
        metadata: Literal["cors", "navigate"] | dict[str,str] = "cors",
        https: bool = False,
        user_agent: str = str(),
        ajax: bool = False,
        version: int = CHROME_VERSION,
        **kwargs
    ) -> dict[str,str]:
    return {
        **({"authority": get_hostname(authority)} if authority else dict()),
        **({"accept": accept} if accept else dict()),
        **({"accept-encoding": encoding} if encoding else dict()),
        **({"accept-language": get_default_language(language)} if language else dict()),
        **({"connection": connection} if connection else dict()),
        **({"content-type": get_content_type(contents)} if contents else dict()),
        **({"cookie": cookies} if cookies else dict()),
        **({"host": get_hostname(host)} if host else dict()),
        **({"origin": origin} if origin else dict()),
        **({"priority": priority} if priority else dict()),
        **({"referer": referer} if referer else dict()),
        "sec-ch-ua": (client or get_default_client(version)),
        "sec-ch-ua-mobile": f"?{int(mobile)}",
        "sec-ch-ua-platform": (platform or get_current_platform()),
        **get_fetch_metadata(metadata),
        **({"upgrade-insecure-requests": "1"} if https else dict()),
        "user-agent": (user_agent or get_user_agent(version)),
        **({"x-requested-with": "XMLHttpRequest"} if ajax else dict()),
        **kwargs
    }


def add_headers(headers: dict[str,str], **kwargs) -> dict[str,str]:
    apply_map = dict(
        authority = dict(key="authority", func=get_hostname),
        encoding = dict(key="accept-encoding"),
        language = dict(key="accept-encoding", func=get_default_language),
        contents = dict(key="content-type", func=get_content_type),
        cookies = dict(key="cookie"),
        host = dict(key="host", func=get_hostname),
        client = dict(key="sec-ch-ua"),
        mobile = dict(key="sec-ch-ua-mobile", func=(lambda x: f"?{int(x)}")),
        platform = dict(key="sec-ch-ua-platform"),
        user_agent = dict(key="user-agent"),
    )
    for key, value in kwargs.items():
        key_lower = key.lower()
        if key_lower in apply_map:
            apply = apply_map[key_lower]
            headers[apply["key"]] = apply["func"](value) if "func" in apply else value
        elif (key_lower == "https") and value:
            headers["upgrade-insecure-requests"] = "1"
        elif (key_lower == "ajax") and value:
            headers["x-requested-with"] = "XMLHttpRequest"
        elif key_lower == "metadata":
            headers.update(get_fetch_metadata(value))
        else:
            headers[key] = value
    return headers


def get_hostname(url: str) -> str:
    for prefix in ["://"]:
        if prefix in url:
            url = url.split(prefix, maxsplit=1)[1]
    for suffix in ['/','?','#']:
        if suffix in url:
            url = url.split(suffix, maxsplit=1)[0]
    return url


def get_default_language(value: Literal["ko","en"] | str = "ko") -> str:
    if value == "ko":
        return "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
    elif value == "en":
        return "en-US,en;q=0.9"
    else:
        return value


def get_content_type(contents: Literal["form", "javascript", "json", "text", "multipart"] | str | dict):
    if isinstance(contents, str):
        if contents == "form":
            return "application/x-www-form-urlencoded"
        elif contents == "javascript":
            return "javascript"
        elif contents == "json":
            return "application/json"
        elif contents == "text":
            return "text/plain"
        elif contents == "multipart":
            return "multipart/form-data"
        else:
            return contents
    elif isinstance(contents, dict):
        content_type = get_content_type(contents["type"])
        for key, value in contents.items(): # boundary, charset, ...
            if key != "type":
                content_type += f"; {key}={value}"
        return content_type
    else:
        raise TypeError("Invalid type for contents. A string or dictionary type is allowed.")


def get_default_client(version: int = CHROME_VERSION) -> str:
    return f'"Not)A;Brand";v="8", "Chromium";v="{version}", "Google Chrome";v="{version}"'


def get_current_platform() -> str:
    import platform
    os_name = platform.system()
    return "macOS" if os_name == "Darwin" else os_name


def get_fetch_metadata(metadata: Literal["cors", "navigate"] | dict[str,str] = "navigate") -> dict[str,str]:
    if isinstance(metadata, str):
        if metadata == "cors":
            return {"sec-fetch-dest": "empty", "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin", "sec-fetch-user": "?1"}
        elif metadata == "navigate":
            return {"sec-fetch-dest": "document", "sec-fetch-mode": "navigate", "sec-fetch-site": "none", "sec-fetch-user": "?1"}
        else:
            return dict()
    elif isinstance(metadata, dict):
        return metadata
    else:
        return dict()


def get_user_agent(version: int = CHROME_VERSION) -> str:
    import platform
    system = platform.system()

    if system == "Windows":
        x64 = {"amd64", "x86_64", "x64"}
        arch_token = "Win64; x64" if (arch := platform.machine()).lower() in x64 else arch
        platform_token = "Windows NT 10.0; {}".format(arch_token or "Win64; x64")
    elif system == "Darwin":
        mac_ver = (platform.mac_ver()[0] or "26.1").replace('.','_')
        platform_token = "Macintosh; Intel Mac OS X {}".format(mac_ver)
    elif system == "Linux":
        arch_token = platform.machine() or "x86_64"
        platform_token = "X11; Linux {}".format(arch_token)
    else:
        platform_token = system or "Windows NT 10.0; Win64; x64"

    return f"Mozilla/5.0 ({platform_token}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version}.0.0.0 Safari/537.36"


def zip_headers(header_lines: str) -> dict[str,str]:
    headers = dict()
    lines = header_lines.split('\n')
    for seq in range(len(lines)//2):
        if str(lines[seq*2]).startswith(':'):
            if lines[seq*2] != ":authority":
                continue
            else:
                lines[seq*2] = "authority"
        headers[lines[seq*2]] = lines[seq*2+1]
    return headers
