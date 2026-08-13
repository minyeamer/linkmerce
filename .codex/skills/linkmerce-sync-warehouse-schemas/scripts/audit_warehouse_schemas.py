from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess


REPO = Path(__file__).resolve().parents[4]
DDL_PATH = "postgres/init.sql"
BQ_PATH = "postgres/resources/bq_schemas.json"
SCHEMA_SURFACES = {DDL_PATH, BQ_PATH, "src/env/config.yaml"}


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd = REPO, text = True, encoding = "utf-8",
        errors = "replace", capture_output = True, check = False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description = "Audit LinkMerce warehouse schema parity.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--staged", action="store_true")
    group.add_argument("--working-tree", action="store_true")
    group.add_argument("--base-ref")
    group.add_argument("--all", action="store_true")
    parser.add_argument("--target-ref", default="HEAD")
    return parser.parse_args()


def changed_files(args: argparse.Namespace) -> set[str]:
    if args.all:
        return set()
    command = ["diff"]
    if args.staged:
        command.append("--cached")
    elif args.base_ref:
        command.extend([args.base_ref, args.target_ref])
    command.extend(["--name-only", "--diff-filter=ACMR"])
    paths = set(git(*command).splitlines())
    if args.working_tree:
        paths.update(git("ls-files", "--others", "--exclude-standard").splitlines())
    return {path.replace("\\", "/") for path in paths}


def content(args: argparse.Namespace, path: str) -> str:
    if args.staged:
        return git("show", f":{path}")
    if args.base_ref:
        return git("show", f"{args.target_ref}:{path}")
    return (REPO / path).read_text(encoding="utf-8")


def postgres_tables(sql: str) -> dict[str, list[str]]:
    tables: dict[str, list[str]] = dict()
    pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS\s+([\w.]+)\s*\((.*?)\)\s*(?:;|WITH\s*\()",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(sql):
        columns = list()
        for raw in match.group(2).splitlines():
            line = raw.strip().lstrip(",").strip()
            if not line or line.upper().startswith(("PRIMARY KEY", "UNIQUE", "CONSTRAINT", "FOREIGN KEY")):
                continue
            name = re.match(r'"?([A-Za-z_]\w*)"?\s+', line)
            if name:
                columns.append(name.group(1))
        tables[match.group(1)] = columns
    return tables


def bigquery_tables(raw: str) -> dict[str, list[str]]:
    data = json.loads(raw)
    return {
        table: [field["name"] for field in fields if isinstance(field, dict) and "name" in field]
        for table, fields in data.items()
    }


def main() -> int:
    args = parse_args()
    changed = changed_files(args)
    relevant = args.all or bool(changed & SCHEMA_SURFACES) or any(
        path.startswith(("src/linkmerce/core/", "airflow/dags/", "dbt_bigquery/", "dbt_postgres/"))
        for path in changed
    )
    if not relevant:
        print("PASS No changed warehouse contract surface found.")
        return 0

    pg = postgres_tables(content(args, DDL_PATH))
    bq = bigquery_tables(content(args, BQ_PATH))
    errors = list()
    warnings = list()
    for table in sorted(pg.keys() & bq.keys()):
        if pg[table] != bq[table]:
            message = f"{table}: ordered columns differ: postgres={pg[table]} bigquery={bq[table]}"
            if args.all or DDL_PATH in changed or BQ_PATH in changed:
                errors.append(message)
            else:
                warnings.append(message)
    for table in sorted(pg.keys() - bq.keys()):
        warnings.append(f"{table}: Postgres table has no BigQuery schema entry")
    for table in sorted(bq.keys() - pg.keys()):
        warnings.append(f"{table}: BigQuery schema entry has no Postgres table")

    for message in errors:
        print(f"ERROR {message}")
    for message in warnings:
        print(f"WARN  {message}")
    print(f"SUMMARY errors={len(errors)} warnings={len(warnings)} compared={len(pg.keys() & bq.keys())}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
