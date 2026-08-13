from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[4]
PROJECTS = ("dbt_bigquery", "dbt_postgres")
MODEL_PREFIX = {project: f"{project}/models/" for project in PROJECTS}


def run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd = REPO_ROOT,
        check = True,
        capture_output = True,
        text = True,
        encoding = "utf-8",
    )
    return result.stdout


def parse_name_status(output: str) -> dict[str, str]:
    changes: dict[str, str] = {}
    for line in output.splitlines():
        if not line:
            continue
        parts = line.split("\t")
        status = parts[0]
        if status.startswith(("R", "C")) and len(parts) == 3:
            changes[parts[1].replace("\\", "/")] = "D"
            changes[parts[2].replace("\\", "/")] = "A"
        elif len(parts) == 2:
            changes[parts[1].replace("\\", "/")] = status[0]
    return changes


def changed_files(args: argparse.Namespace) -> dict[str, str]:
    if args.base_ref or args.target_ref:
        if not (args.base_ref and args.target_ref):
            raise ValueError("--base-ref and --target-ref must be used together")
        return parse_name_status(
            run_git("diff", "--name-status", args.base_ref, args.target_ref)
        )

    if args.staged:
        return parse_name_status(run_git("diff", "--cached", "--name-status"))

    changes = parse_name_status(run_git("diff", "--name-status"))
    changes.update(parse_name_status(run_git("diff", "--cached", "--name-status")))
    for path in run_git("ls-files", "--others", "--exclude-standard").splitlines():
        changes[path.replace("\\", "/")] = "A"
    return changes


def model_files(project: str) -> dict[str, Path]:
    root = REPO_ROOT / project / "models"
    return {
        path.relative_to(root).as_posix(): path
        for path in root.rglob("*.sql")
    }


def config_value(sql: str, key: str) -> str | None:
    match = re.search(
        rf"\b{re.escape(key)}\s*=\s*(['\"])(?P<value>.+?)\1",
        sql,
    )
    return match.group("value") if match else None


def dependencies(sql: str, function: str) -> set[tuple[str, ...]]:
    if function == "source":
        pattern = r"source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)"
    else:
        pattern = r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)"
    matches = re.findall(pattern, sql)
    return {
        match if isinstance(match, tuple) else (match,)
        for match in matches
    }


def declared_models(project: str) -> set[str]:
    text = (REPO_ROOT / project / "models" / "models.yml").read_text(encoding="utf-8")
    return set(re.findall(r'^\s{2}- name:\s*["\']?([^"\'\s]+)', text, re.MULTILINE))


def declared_selectors(project: str) -> set[str]:
    text = (REPO_ROOT / project / "selectors.yml").read_text(encoding="utf-8")
    return set(re.findall(r"^\s{2}- name:\s*(\S+)", text, re.MULTILINE))


def declared_sources(project: str) -> set[tuple[str, str]]:
    lines = (REPO_ROOT / project / "models" / "sources.yml").read_text(
        encoding = "utf-8"
    ).splitlines()
    result: set[tuple[str, str]] = set()
    source_name: str | None = None
    in_tables = False
    for line in lines:
        source_match = re.match(r"^\s{2}- name:\s*(\S+)", line)
        if source_match:
            source_name = source_match.group(1)
            in_tables = False
            continue
        if re.match(r"^\s{4}tables:\s*$", line):
            in_tables = True
            continue
        table_match = re.match(r"^\s{6}- name:\s*(\S+)", line)
        if source_name and in_tables and table_match:
            result.add((source_name, table_match.group(1)))
    return result


def manifest_columns(project: str) -> dict[str, list[str]]:
    path = REPO_ROOT / project / "target" / "manifest.json"
    if not path.exists():
        return {}
    manifest = json.loads(path.read_text(encoding="utf-8"))
    result: dict[str, list[str]] = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        columns = node.get("columns") or {}
        if columns:
            result[node["name"]] = list(columns)
    return result


def audit_environment_commands(errors: list[str]) -> None:
    skill_root = REPO_ROOT / ".codex" / "skills" / "linkmerce-sync-dbt-models"
    wrong_env = re.compile(r"conda\s+run\s+-n\s+main\s+dbt\b", re.IGNORECASE)
    bare_dbt = re.compile(
        r"^\s*dbt\s+(?:build|compile|docs|parse|run|seed|snapshot|test)\b",
        re.IGNORECASE | re.MULTILINE,
    )
    for path in skill_root.rglob("*"):
        if path.suffix not in {".md", ".py", ".yaml", ".yml"}:
            continue
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(REPO_ROOT).as_posix()
        if wrong_env.search(text):
            errors.append(f"{relative}: dbt must not run in the main Conda environment")
        if bare_dbt.search(text):
            errors.append(f"{relative}: dbt commands must use 'conda run -n dbt dbt'")


def main() -> int:
    parser = argparse.ArgumentParser(
        description = "Audit paired LinkMerce BigQuery and PostgreSQL dbt models",
    )
    surface = parser.add_mutually_exclusive_group()
    surface.add_argument("--staged", action="store_true")
    surface.add_argument("--working-tree", action="store_true")
    parser.add_argument("--base-ref")
    parser.add_argument("--target-ref")
    args = parser.parse_args()

    errors: list[str] = []
    try:
        changes = changed_files(args)
    except (subprocess.CalledProcessError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2

    models = {project: model_files(project) for project in PROJECTS}
    bigquery_paths = set(models["dbt_bigquery"])
    postgres_paths = set(models["dbt_postgres"])
    for path in sorted(bigquery_paths - postgres_paths):
        errors.append(f"Missing PostgreSQL model pair: {path}")
    for path in sorted(postgres_paths - bigquery_paths):
        errors.append(f"Missing BigQuery model pair: {path}")

    paired_paths = sorted(bigquery_paths & postgres_paths)
    for relative in paired_paths:
        sql = {
            project: models[project][relative].read_text(encoding="utf-8")
            for project in PROJECTS
        }
        for key in ("schema", "alias"):
            values = {project: config_value(sql[project], key) for project in PROJECTS}
            if values["dbt_bigquery"] != values["dbt_postgres"]:
                errors.append(
                    f"{relative}: configured {key} differs "
                    f"({values['dbt_bigquery']!r} != {values['dbt_postgres']!r})"
                )
        for function in ("source", "ref"):
            values = {
                project: dependencies(sql[project], function)
                for project in PROJECTS
            }
            if values["dbt_bigquery"] != values["dbt_postgres"]:
                errors.append(f"{relative}: {function} dependencies differ")

    model_names = {
        project: {Path(path).stem for path in project_paths}
        for project, project_paths in models.items()
    }
    for project in PROJECTS:
        undocumented = model_names[project] - declared_models(project)
        for name in sorted(undocumented):
            errors.append(f"{project}/models/models.yml: missing model {name}")

        readme = (REPO_ROOT / project / "README.md").read_text(encoding="utf-8")
        for name in sorted(model_names[project]):
            if name not in readme:
                errors.append(f"{project}/README.md: missing model path entry {name}")

        declared = declared_sources(project)
        referenced: set[tuple[str, ...]] = set()
        for sql_path in models[project].values():
            referenced.update(
                dependencies(sql_path.read_text(encoding="utf-8"), "source")
            )
        for source in sorted(referenced - declared):
            errors.append(
                f"{project}/models/sources.yml: missing source {source[0]}.{source[1]}"
            )

    selectors = {project: declared_selectors(project) for project in PROJECTS}
    if selectors["dbt_bigquery"] != selectors["dbt_postgres"]:
        missing_pg = sorted(selectors["dbt_bigquery"] - selectors["dbt_postgres"])
        missing_bq = sorted(selectors["dbt_postgres"] - selectors["dbt_bigquery"])
        if missing_pg:
            errors.append(f"dbt_postgres/selectors.yml: missing selectors {missing_pg}")
        if missing_bq:
            errors.append(f"dbt_bigquery/selectors.yml: missing selectors {missing_bq}")

    columns = {project: manifest_columns(project) for project in PROJECTS}
    for name in sorted(set(columns["dbt_bigquery"]) & set(columns["dbt_postgres"])):
        if columns["dbt_bigquery"][name] != columns["dbt_postgres"][name]:
            errors.append(f"{name}: manifest column order differs")

    for path, status in changes.items():
        project = next(
            (name for name, prefix in MODEL_PREFIX.items() if path.startswith(prefix)),
            None,
        )
        if not project or not path.endswith(".sql") or status not in {"A", "D"}:
            continue
        if f"{project}/README.md" not in changes:
            errors.append(
                f"{path}: added or deleted model requires {project}/README.md update"
            )

    audit_environment_commands(errors)

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        print(f"Audit failed with {len(errors)} error(s).")
        return 1

    print(
        f"Audit passed: {len(paired_paths)} paired models, "
        f"{len(changes)} changed paths reviewed."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
