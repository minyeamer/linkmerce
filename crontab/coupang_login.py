from typing import Literal
import argparse
import os


LOGIN_RETRIES = 3


def read_yaml(file_path: str) -> dict | list:
    from ruamel.yaml import YAML
    with open(file_path, 'r', encoding="utf-8") as file:
        yaml = YAML(typ="safe")
        return yaml.load(file.read())


def extract_path(cookies: str) -> str:
    import re
    return re.search(r"Path\(([^)]+)\)", cookies).groups()[0]


def preview(cookies: str, size: int = 100) -> str:
    return (cookies[:size] + "...") if len(cookies) > size else cookies


def login_coupang(credentials: dict, domain: Literal["advertising","wing"], log_file: str | None = None) -> dict[str,dict]:
    if domain == "advertising":
        from linkmerce.api.coupang.advertising import login
    else:
        from linkmerce.api.coupang.wing import login

    from typing import Sequence
    import logging
    import random
    import time

    logging.basicConfig(
        filename = log_file,
        level = logging.INFO,
        format = "[%(asctime)s] %(levelname)s - %(message)s",
        datefmt = "%Y-%m-%d, %H:%M:%S",
    )

    cookies_arr = cookies if isinstance((cookies := credentials["coupang"][domain]), Sequence) else [cookies]
    cookies_map = {cookie["vendor_id"]: cookie["cookies"] for cookie in cookies_arr}

    users = users if isinstance((users := credentials["coupang"]["users"]), Sequence) else [users]
    exceptions = list()
    results = {user["vendor_id"]: None for user in users if user["vendor_id"] in cookies_map}
    random.shuffle(users)

    for user in users:
        vendor_id = user["vendor_id"]
        if vendor_id in cookies_map:
            for i in range(LOGIN_RETRIES):
                try:
                    cookies = login(
                        userid = user["userid"],
                        passwd = user["passwd"],
                        domain = "wing",
                        save_to = extract_path(cookies_map[vendor_id]),
                    )
                    logging.info(f"[{vendor_id}] SUCCESS ({i})")
                    results[vendor_id] = preview(cookies)
                    break
                except Exception as exception:
                    logging.error(f"[{vendor_id}] FAILED ({i}) - [{exception.__class__.__name__}] {exception}")
                    results[vendor_id] = preview(str(exception))
                    if i == (LOGIN_RETRIES-1):
                        exceptions.append(exception)
                    else:
                        time.sleep(1)
            time.sleep(1)

    if exceptions:
        raise exceptions[0]
    else:
        logging.info(str(results).replace('\'', '\"'))
        return results


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--credentials", "-c", type=str, required=True, help="Credentials file path")
    parser.add_argument("--domain", "-d", type=str, required=True, help="Coupang domain (advertising, wing)")
    parser.add_argument("--chdir", "-r", type=str, default=None, help="Execution path (Optional)")
    parser.add_argument("--logfile", "-l", type=str, default=None, help="Log file path (Optional)")
    return parser.parse_args()

if __name__ == "__main__":
    try:
        args = parse_arguments()
        credentials_path = args.credentials
        domain = args.domain
        chdir = args.chdir
        log_file = args.logfile
    except:
        raise ValueError("The following arguments are required: credentials, playwright.")

    if chdir:
        if os.path.exists(chdir):
            os.chdir(chdir)
        else:
            raise ValueError("The execution path does not exist.")

    try:
        credentials = read_yaml(credentials_path)
    except:
        raise ValueError("Unable to read credentials.")

    login_coupang(credentials, domain, log_file)
