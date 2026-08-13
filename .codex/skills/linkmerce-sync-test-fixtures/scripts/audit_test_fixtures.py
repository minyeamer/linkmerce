from __future__ import annotations

import argparse
import ast
from pathlib import Path
import re
import subprocess


REPO = Path(__file__).resolve().parents[4]
CORE = "src/linkmerce/core/"


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd = REPO, text = True, encoding = "utf-8",
        errors = "replace", capture_output = True, check = False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description = "Audit LinkMerce ETL test and fixture coverage.")
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


def classes(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return list()
    result: list[str] = list()
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name.startswith("_"):
            continue
        if path.name == "extract.py":
            covered = any(
                isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                and item.name in {"extract", "extract_async"}
                for item in node.body
            )
        else:
            covered = any(
                isinstance(item, (ast.Assign, ast.AnnAssign))
                and any(
                    isinstance(target, ast.Name) and target.id == "tables"
                    for target in (item.targets if isinstance(item, ast.Assign) else [item.target])
                )
                for item in node.body
            )
        if covered:
            result.append(node.name)
    return result


def imported_names(text: str) -> set[str]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return set()
    return {
        alias.asname or alias.name
        for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }


def main() -> int:
    args = parse_args()
    changed = changed_files(args)
    extract_test = (REPO / "src/tests/test_extract.py").read_text(encoding="utf-8")
    transform_test = (REPO / "src/tests/test_transform.py").read_text(encoding="utf-8")
    extract_imports = imported_names(extract_test)
    transform_imports = imported_names(transform_test)
    if args.all:
        modules = list((REPO / CORE).rglob("extract.py")) + list((REPO / CORE).rglob("transform.py"))
    else:
        modules = [REPO / path for path in changed if path.startswith(CORE) and path.endswith(("/extract.py", "/transform.py"))]

    errors: list[str] = list()
    warnings: list[str] = list()
    for module in modules:
        relative = module.relative_to(REPO).as_posix()
        expected = extract_imports if module.name == "extract.py" else transform_imports
        for name in classes(module):
            if name in expected:
                continue
            message = f"{relative} - `{name}` is not imported by the matching ETL test module"
            (warnings if args.all else errors).append(message)

    marker_text = (REPO / "src/tests/pytest.ini").read_text(encoding="utf-8")
    changed_domains = {
        Path(path).parts[3] for path in changed
        if path.startswith(CORE) and len(Path(path).parts) > 3
    }
    for domain in sorted(changed_domains):
        normalized = domain.replace("-", "_")
        if not re.search(rf"(?m)^\s*{re.escape(normalized)}\s*:", marker_text):
            warnings.append(f"src/tests/pytest.ini - no marker named `{normalized}` for changed domain `{domain}`")

    for message in errors:
        print(f"ERROR {message}")
    for message in warnings:
        print(f"WARN  {message}")
    print(f"SUMMARY errors={len(errors)} warnings={len(warnings)} modules={len(modules)}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
