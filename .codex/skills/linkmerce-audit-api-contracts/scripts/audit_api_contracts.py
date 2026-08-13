from __future__ import annotations

import argparse
import ast
from pathlib import Path
import re
import subprocess


REPO = Path(__file__).resolve().parents[4]


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd = REPO, text = True, encoding = "utf-8",
        errors = "replace", capture_output = True, check = False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description = "Audit LinkMerce Extractor, Transformer, and API contracts.")
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


def repository_paths(args: argparse.Namespace) -> list[str]:
    if args.base_ref:
        listing = git("ls-tree", "-r", "--name-only", args.target_ref, "--", "src/linkmerce")
    else:
        listing = git("ls-files", "src/linkmerce")
        if args.working_tree:
            listing += git("ls-files", "--others", "--exclude-standard", "src/linkmerce")
    return sorted({path for path in listing.splitlines() if path.endswith(".py")})


def source(args: argparse.Namespace, path: str) -> str:
    if args.staged:
        try:
            return git("show", f":{path}")
        except RuntimeError:
            return (REPO / path).read_text(encoding="utf-8")
    if args.base_ref:
        return git("show", f"{args.target_ref}:{path}")
    return (REPO / path).read_text(encoding="utf-8")


def returns_type(doc: str | None) -> str | None:
    if not doc:
        return None
    lines = doc.splitlines()
    for index, line in enumerate(lines):
        if line.strip() != "Returns":
            continue
        for candidate in lines[index + 1:index + 5]:
            value = candidate.strip()
            if value and set(value) != {"-"}:
                return value
    return None


def normalized(value: str) -> str:
    return re.sub(r"\s+", "", value).replace("typing.", "").removesuffix(":")


def class_tables(node: ast.ClassDef) -> dict[str, str] | None:
    for item in node.body:
        if not isinstance(item, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "tables" for target in item.targets):
            continue
        try:
            value = ast.literal_eval(item.value)
        except (ValueError, TypeError):
            return None
        return value if isinstance(value, dict) else None
    return None


def audit_file(path: str, text: str) -> tuple[list[str], list[str]]:
    errors: list[str] = list()
    warnings: list[str] = list()
    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        return [f"{path}:{exc.lineno} - syntax error: {exc.msg}"], warnings

    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name not in {"extract", "extract_async"}:
            continue
        annotation = ast.unparse(node.returns) if node.returns else None
        documented = returns_type(ast.get_docstring(node, clean=False))
        if annotation and documented and normalized(annotation) != normalized(documented):
            errors.append(f"{path}:{node.lineno} - return annotation `{annotation}` differs from Returns `{documented}`")
        if annotation and any(name in annotation for name in ("DuckDBConnection", "DuckDBPyConnection", "Transformer")):
            errors.append(f"{path}:{node.lineno} - Extractor return annotation contains a downstream Transformer shape: `{annotation}`")

    if path.startswith("src/linkmerce/api/"):
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            decorated = any("with_duckdb_connection" in ast.unparse(item) for item in node.decorator_list)
            if not decorated:
                continue
            annotation = ast.unparse(node.returns) if node.returns else None
            documented = returns_type(ast.get_docstring(node, clean=False))
            if annotation and documented and normalized(annotation) != normalized(documented):
                errors.append(f"{path}:{node.lineno} - API return annotation `{annotation}` differs from Returns `{documented}`")

    if path.endswith("/transform.py"):
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and class_tables(node):
                doc = ast.get_docstring(node, clean=False) or ""
                section = doc.partition("- **Extractor**")[2].partition("- **Parser")[0]
                if "->" in section:
                    errors.append(f"{path}:{node.lineno} - Transformer Extractor section must contain class name only")
                if not section.strip():
                    warnings.append(f"{path}:{node.lineno} - unable to identify Transformer Extractor documentation")
    return errors, warnings


def main() -> int:
    args = parse_args()
    changed = changed_files(args)
    candidates = repository_paths(args)
    if not args.all:
        roots = {
            str(Path(path).parent).replace("\\", "/")
            for path in changed if path.startswith("src/linkmerce/core/")
        }
        candidates = [
            path for path in candidates
            if path in changed or path.startswith("src/linkmerce/api/") and any(part in path for part in {Path(root).parts[-2] for root in roots})
            or str(Path(path).parent).replace("\\", "/") in roots
        ]
    errors: list[str] = list()
    warnings: list[str] = list()
    for path in candidates:
        found_errors, found_warnings = audit_file(path, source(args, path))
        errors.extend(found_errors)
        warnings.extend(found_warnings)
    for message in errors:
        print(f"ERROR {message}")
    for message in warnings:
        print(f"WARN  {message}")
    print(f"SUMMARY errors={len(errors)} warnings={len(warnings)} files={len(candidates)}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
