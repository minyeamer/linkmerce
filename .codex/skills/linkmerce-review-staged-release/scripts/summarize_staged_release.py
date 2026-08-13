from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import re
import subprocess


REPO = Path(__file__).resolve().parents[4]


def git(*args: str) -> tuple[int, str]:
    result = subprocess.run(
        ["git", *args], cwd = REPO, text = True, encoding = "utf-8",
        errors = "replace", capture_output = True, check = False,
    )
    return result.returncode, result.stdout or result.stderr


def layer(path: str) -> str:
    prefixes = {
        "core": "src/linkmerce/core/", "api": "src/linkmerce/api/",
        "tests": "src/tests/", "airflow": "airflow/dags/",
        "dbt_bigquery": "dbt_bigquery/", "dbt_postgres": "dbt_postgres/",
        "warehouse": "postgres/", "skills": ".codex/skills/",
        "instructions": ".github/instructions/",
    }
    return next((name for name, prefix in prefixes.items() if path.startswith(prefix)), "other")


def suggested_scope(paths: set[str]) -> str:
    def all_match(predicate) -> bool:
        return bool(paths) and all(predicate(path) for path in paths)

    extract = lambda path: path == "src/linkmerce/common/extract.py" or (
        path.startswith("src/linkmerce/core/") and path.endswith("/extract.py")
    )
    transform = lambda path: path == "src/linkmerce/common/transform.py" or (
        path.startswith("src/linkmerce/core/")
        and path.endswith(("/transform.py", "/models.sql"))
    )
    exclusive = (
        ("api", lambda path: path.startswith("src/linkmerce/api/")),
        ("extract", extract),
        ("transform", transform),
        ("extensions", lambda path: path.startswith("src/linkmerce/extensions/")),
        ("test", lambda path: path.startswith("src/tests/")),
        ("fastapi", lambda path: path.startswith("airflow_trigger/fastapi/")),
        ("streamlit", lambda path: path.startswith("airflow_trigger/streamlit/")),
    )
    for scope, predicate in exclusive:
        if all_match(predicate):
            return scope

    counts = {
        "core": sum(path.startswith("src/") for path in paths),
        "airflow": sum(path.startswith("airflow/dags/") for path in paths),
        "dbt": sum(path.startswith(("dbt_bigquery/", "dbt_postgres/")) for path in paths),
        "postgres": sum(path.startswith("postgres/") for path in paths),
        "skills": sum(
            path.startswith((".agents/skills/", ".codex/skills/", ".github/instructions/"))
            for path in paths
        ),
    }
    scope, count = max(counts.items(), key=lambda item: item[1])
    return scope if count else "undetermined"


def staged_text(path: str) -> str:
    code, output = git("show", f":{path}")
    return output if code == 0 else ""


def version(text: str, package: str | None = None) -> str | None:
    if package:
        pattern = (
            r'(?ms)^\[\[package\]\]\s+name\s*=\s*"'
            + re.escape(package)
            + r'"\s+version\s*=\s*"([^"]+)"'
        )
    else:
        pattern = r'(?m)^version\s*=\s*"([^"]+)"'
    match = re.search(pattern, text)
    return match.group(1) if match else None


def main() -> int:
    code, output = git("diff", "--cached", "--name-status", "--diff-filter=ACMRD")
    if code:
        print(f"ERROR Unable to read staged changes: {output.strip()}")
        return 1
    if not output.strip():
        print("ERROR No staged changes found.")
        return 1

    groups: dict[str, list[str]] = defaultdict(list)
    for line in output.splitlines():
        status, *parts = line.split("\t")
        path = parts[-1].replace("\\", "/")
        groups[layer(path)].append(f"{status} {path}")
    for name in sorted(groups):
        print(f"[{name}]")
        for item in groups[name]:
            print(f"  {item}")

    check_code, check_output = git("diff", "--cached", "--check")
    print("PASS staged diff whitespace check" if check_code == 0 else f"ERROR {check_output.strip()}")

    staged = {item.split(" ", 1)[1] for items in groups.values() for item in items}
    print(f"SUGGESTED COMMIT SCOPE {suggested_scope(staged)}")
    if {"pyproject.toml", "uv.lock"} & staged:
        pyproject = version(staged_text("pyproject.toml"))
        lock = version(staged_text("uv.lock"), "linkmerce")
        if pyproject != lock:
            print(f"ERROR Version mismatch: pyproject.toml={pyproject} uv.lock={lock}")
            check_code = 1
        else:
            print(f"PASS Version files agree: {pyproject}")

    _, log = git("log", "-8", "--pretty=format:%s")
    print("[recent commit subjects]")
    print(log.strip())
    return 1 if check_code else 0


if __name__ == "__main__":
    raise SystemExit(main())
