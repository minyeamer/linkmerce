from __future__ import annotations

import argparse
import ast
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tokenize


REPO = Path(__file__).resolve().parents[4]
PYTHON_AREAS = ("src/linkmerce/", "src/tests/", "airflow/dags/", ".codex/skills/")


class Audit:
    def __init__(self) -> None:
        self.errors: list[str] = list()
        self.warnings: list[str] = list()

    def error(self, path: str, message: str, line: int | None = None) -> None:
        location = f"{path}:{line}" if line else path
        self.errors.append(f"ERROR {location} - {message}")

    def warn(self, path: str, message: str, line: int | None = None) -> None:
        location = f"{path}:{line}" if line else path
        self.warnings.append(f"WARN  {location} - {message}")


def git(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd = REPO,
        text = True,
        encoding = "utf-8",
        errors = "replace",
        capture_output = True,
        check = False,
    )
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description = "Audit a LinkMerce ETL integration diff without importing project modules.",
    )
    surface = parser.add_mutually_exclusive_group(required=True)
    surface.add_argument("--staged", action="store_true", help="Audit the Git index")
    surface.add_argument("--working-tree", action="store_true", help="Audit unstaged and untracked files")
    surface.add_argument("--base-ref", help="Audit a Git range beginning at this ref")
    parser.add_argument("--target-ref", default="HEAD", help="Range target; requires --base-ref")
    return parser.parse_args()


def diff_command(args: argparse.Namespace, *, names_only: bool = False) -> list[str]:
    command = ["diff"]
    if args.staged:
        command.append("--cached")
    elif args.base_ref:
        command.extend([args.base_ref, args.target_ref])
    command.extend(["--diff-filter=ACMR"])
    command.append("--name-only" if names_only else "--unified=0")
    return command


def changed_files(args: argparse.Namespace) -> list[str]:
    paths = set(git(*diff_command(args, names_only=True)).splitlines())
    if args.working_tree:
        paths.update(git("ls-files", "--others", "--exclude-standard").splitlines())
    return sorted(path.replace("\\", "/") for path in paths if path)


def added_files(args: argparse.Namespace) -> set[str]:
    command = diff_command(args, names_only=True)
    command[command.index("--diff-filter=ACMR")] = "--diff-filter=A"
    paths = set(git(*command).splitlines())
    if args.working_tree:
        paths.update(git("ls-files", "--others", "--exclude-standard").splitlines())
    return {path.replace("\\", "/") for path in paths if path}


def added_lines(args: argparse.Namespace, paths: list[str]) -> dict[str, set[int]]:
    lines: dict[str, set[int]] = {path: set() for path in paths}
    current: str | None = None
    next_line = 0
    diff = git(*diff_command(args))
    for text in diff.splitlines():
        if text.startswith("+++ b/"):
            current = text[6:].replace("\\", "/")
        elif text.startswith("@@") and current:
            match = re.search(r"\+(\d+)(?:,(\d+))?", text)
            next_line = int(match.group(1)) if match else 0
        elif current and text.startswith("+") and not text.startswith("+++"):
            lines.setdefault(current, set()).add(next_line)
            next_line += 1
        elif current and not text.startswith("-"):
            next_line += 1

    if args.working_tree:
        tracked = set(git("ls-files").splitlines())
        for path in paths:
            if path not in tracked:
                content = (REPO / path).read_text(encoding="utf-8")
                lines[path] = set(range(1, len(content.splitlines()) + 1))
    return lines


def content_for(args: argparse.Namespace, path: str) -> str:
    if args.staged:
        return git("show", f":{path}")
    if args.base_ref:
        return git("show", f"{args.target_ref}:{path}")
    return (REPO / path).read_text(encoding="utf-8")


def repository_paths(args: argparse.Namespace, prefix: str) -> list[str]:
    if args.base_ref:
        output = git("ls-tree", "-r", "--name-only", args.target_ref, "--", prefix)
    else:
        output = git("ls-files", prefix)
        if args.working_tree:
            output += git("ls-files", "--others", "--exclude-standard", prefix)
    return sorted(set(output.splitlines()))


def line_intersects(node: ast.AST, changed: set[int]) -> bool:
    start = getattr(node, "lineno", 0)
    end = getattr(node, "end_lineno", start)
    return any(start <= line <= end for line in changed)


def doc_intersects(node: ast.AST, changed: set[int]) -> bool:
    if not getattr(node, "body", None):
        return False
    first = node.body[0]
    return (
        isinstance(first, ast.Expr)
        and isinstance(first.value, ast.Constant)
        and isinstance(first.value.value, str)
        and line_intersects(first, changed)
    )


def require_order(audit: Audit, path: str, node: ast.AST, doc: str, headings: list[str]) -> None:
    positions = [doc.find(heading) for heading in headings]
    if any(position < 0 for position in positions):
        missing = [heading for heading, position in zip(headings, positions) if position < 0]
        audit.error(path, f"docstring missing section(s): {', '.join(missing)}", node.lineno)
    elif positions != sorted(positions):
        audit.error(path, f"docstring section order must be: {' -> '.join(headings)}", node.lineno)


def returns_type(doc: str) -> str | None:
    lines = doc.splitlines()
    for index, line in enumerate(lines):
        if line.strip() == "Returns" and index + 2 < len(lines):
            if set(lines[index + 1].strip()) == {"-"}:
                return lines[index + 2].strip()
    return None


def audit_docstrings(audit: Audit, path: str, tree: ast.Module, changed: set[int]) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not (line_intersects(node, changed) or doc_intersects(node, changed)):
            continue
        doc = ast.get_docstring(node, clean=False)

        if path.endswith("/extract.py") and isinstance(node, ast.ClassDef):
            if not doc:
                audit.error(path, "changed Extractor class requires a docstring", node.lineno)
            else:
                require_order(audit, path, node, doc, ["Attributes", "----------"])

        if path.endswith("/extract.py") and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name not in {"extract", "extract_async"}:
                continue
            if not doc:
                audit.error(path, f"{node.name} requires a docstring", node.lineno)
                continue
            positional = [*node.args.posonlyargs, *node.args.args]
            documented_args = [arg for arg in positional if arg.arg not in {"self", "cls"}]
            documented_args.extend(node.args.kwonlyargs)
            headings = ["Parameters", "Returns"] if documented_args else ["Returns"]
            require_order(audit, path, node, doc, headings)
            if node.returns:
                annotation = ast.unparse(node.returns)
                documented = returns_type(doc)
                if documented and documented != annotation:
                    audit.error(
                        path,
                        f"return annotation `{annotation}` differs from Returns `{documented}`",
                        node.lineno,
                    )

        if path.endswith("/transform.py") and isinstance(node, ast.ClassDef):
            has_tables = any(
                isinstance(item, (ast.Assign, ast.AnnAssign))
                and any(
                    isinstance(target, ast.Name) and target.id == "tables"
                    for target in (item.targets if isinstance(item, ast.Assign) else [item.target])
                )
                for item in node.body
            )
            if not has_tables:
                continue
            if not doc:
                audit.error(path, "changed Transformer class requires a docstring", node.lineno)
                continue
            extractor_heading = "- **Extractor**"
            parser_heading = "- **Parsers**" if "- **Parsers**" in doc else "- **Parser**"
            table_heading = "- **Tables**" if "- **Tables**" in doc else "- **Table**"
            require_order(audit, path, node, doc, [extractor_heading, parser_heading, table_heading])
            start = doc.find(extractor_heading)
            end = doc.find(parser_heading)
            if start >= 0 and end > start and "->" in doc[start:end]:
                audit.error(path, "Transformer Extractor section must contain class name only", node.lineno)

        if path.startswith("src/linkmerce/api/") and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            decorated = any("with_duckdb_connection" in ast.unparse(item) for item in node.decorator_list)
            if not decorated:
                continue
            if not doc:
                audit.error(path, "changed public API requires a docstring", node.lineno)
                continue
            table_heading = "**Tables**" if "**Tables**" in doc else "**Table**"
            require_order(audit, path, node, doc, [table_heading, "Parameters", "Returns"])
            if node.returns:
                annotation = ast.unparse(node.returns)
                documented = returns_type(doc)
                if documented and documented != annotation:
                    audit.error(
                        path,
                        f"return annotation `{annotation}` differs from Returns `{documented}`",
                        node.lineno,
                    )


def bracket_spans(
        tokens: list[tokenize.TokenInfo],
    ) -> list[tuple[tuple[int, int], tuple[int, int]]]:
    stack: list[tokenize.TokenInfo] = list()
    spans: list[tuple[tuple[int, int], tuple[int, int]]] = list()
    pairs = {")": "(", "]": "[", "}": "{"}
    for token in tokens:
        if token.type != tokenize.OP:
            continue
        if token.string in "([{":
            stack.append(token)
        elif token.string in pairs and stack:
            opening = stack.pop()
            if opening.string == pairs[token.string]:
                spans.append((opening.start, token.end))
    return spans


def audit_multiline_equals(audit: Audit, path: str, source: str, changed: set[int]) -> None:
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except tokenize.TokenError as exc:
        audit.error(path, f"tokenization failed: {exc}")
        return
    spans = bracket_spans(tokens)
    source_lines = source.splitlines()
    for token in tokens:
        if token.type != tokenize.OP or token.string != "=" or token.start[0] not in changed:
            continue
        line_no, column = token.start
        containers = [span for span in spans if span[0] < token.start < span[1]]
        if not containers:
            continue
        immediate = max(containers, key=lambda span: span[0])
        if immediate[0][0] == immediate[1][0]:
            continue
        line = source_lines[line_no - 1]
        before = line[column - 1] if column else ""
        after = line[column + 1] if column + 1 < len(line) else ""
        if before != " " or after != " ":
            audit.error(path, "multiline parameter or keyword `=` requires one space on both sides", line_no)


def audit_function_indents(
        audit: Audit,
        path: str,
        source: str,
        tree: ast.Module,
        changed: set[int],
    ) -> None:
    lines = source.splitlines()
    tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        candidates = [
            index for index, token in enumerate(tokens)
            if token.type == tokenize.NAME
            and token.string == node.name
            and token.start[0] == node.lineno
        ]
        if not candidates:
            continue
        opening_index = next(
            (index for index in range(candidates[0] + 1, len(tokens)) if tokens[index].string == "("),
            None,
        )
        if opening_index is None:
            continue
        depth = 0
        closing: tokenize.TokenInfo | None = None
        for token in tokens[opening_index:]:
            if token.string == "(":
                depth += 1
            elif token.string == ")":
                depth -= 1
                if depth == 0:
                    closing = token
                    break
        opening = tokens[opening_index]
        if closing is None or opening.start[0] == closing.start[0]:
            continue
        signature_lines = set(range(opening.start[0], closing.start[0] + 1))
        if not (signature_lines & changed):
            continue
        closing_indent = len(lines[closing.start[0] - 1]) - len(lines[closing.start[0] - 1].lstrip())
        expected_closing = node.col_offset + 4
        if closing_indent != expected_closing:
            audit.error(
                path,
                f"multiline function closing parenthesis requires indent {expected_closing}",
                closing.start[0],
            )
        expected_parameter = node.col_offset + 8
        for line_no in range(opening.start[0] + 1, closing.start[0]):
            text = lines[line_no - 1]
            stripped = text.lstrip()
            if not stripped or stripped.startswith("#"):
                continue
            if not re.match(r"(?:\*{0,2}[A-Za-z_]\w*|/)(?:\s*[:,=]|\s*$)", stripped):
                continue
            indent = len(text) - len(stripped)
            if indent != expected_parameter:
                audit.error(
                    path,
                    f"multiline function parameter requires indent {expected_parameter}",
                    line_no,
                )


def literal_tables(tree: ast.Module) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = dict()
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for item in node.body:
            if not isinstance(item, ast.Assign):
                continue
            if not any(isinstance(target, ast.Name) and target.id == "tables" for target in item.targets):
                continue
            try:
                value = ast.literal_eval(item.value)
            except (ValueError, TypeError):
                continue
            if isinstance(value, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()):
                result[node.name] = value
    return result


def literal_params(tree: ast.Module) -> set[str]:
    result: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "params" for target in node.targets):
            continue
        try:
            value = ast.literal_eval(node.value)
        except (ValueError, TypeError):
            continue
        if isinstance(value, dict):
            result.update(key for key in value if isinstance(key, str))
    return result


def audit_transform_sql(audit: Audit, args: argparse.Namespace, path: str, tree: ast.Module) -> None:
    tables_by_class = literal_tables(tree)
    if not tables_by_class:
        return
    model_path = str(Path(path).with_name("models.sql")).replace("\\", "/")
    try:
        sql = content_for(args, model_path)
    except (RuntimeError, FileNotFoundError):
        audit.error(path, "Transformer has no sibling models.sql")
        return
    table_keys = {key for tables in tables_by_class.values() for key in tables}
    placeholders = set(re.findall(r"\{\{\s*([A-Za-z_]\w*)\s*\}\}", sql)) - {"rows"}
    unknown = placeholders - table_keys
    missing = table_keys - placeholders
    if unknown:
        audit.error(model_path, f"SQL placeholder(s) absent from Transformer.tables: {sorted(unknown)}")
    if missing:
        audit.error(model_path, f"Transformer table key(s) absent from models.sql: {sorted(missing)}")
    params = literal_params(tree)
    sql_params = set(re.findall(r"\$([A-Za-z_]\w*)", sql))
    unknown_params = sql_params - params
    unused_params = params - sql_params
    if unknown_params:
        audit.error(model_path, f"SQL parameter(s) absent from Transformer.params: {sorted(unknown_params)}")
    if unused_params:
        audit.error(path, f"Transformer.params key(s) absent from models.sql: {sorted(unused_params)}")


def api_decorator_tables(node: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, str] | None:
    for decorator in node.decorator_list:
        if not isinstance(decorator, ast.Call) or "with_duckdb_connection" not in ast.unparse(decorator.func):
            continue
        for keyword in decorator.keywords:
            try:
                value = ast.literal_eval(keyword.value)
            except (ValueError, TypeError):
                continue
            if keyword.arg == "table" and isinstance(value, str):
                return {"table": value}
            if keyword.arg == "tables" and isinstance(value, dict):
                if all(isinstance(key, str) and isinstance(item, str) for key, item in value.items()):
                    return value
    return None


def audit_api_tables(
        audit: Audit,
        args: argparse.Namespace,
        path: str,
        tree: ast.Module,
        changed: set[int],
    ) -> None:
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not line_intersects(node, changed):
            continue
        api_tables = api_decorator_tables(node)
        if api_tables is None:
            continue
        transform_import: tuple[str, str] | None = None
        for item in ast.walk(node):
            if not isinstance(item, ast.ImportFrom) or not item.module or not item.module.endswith(".transform"):
                continue
            for name in item.names:
                if name.asname == "T":
                    transform_import = (item.module, name.name)
        if transform_import is None:
            audit.error(path, f"API `{node.name}` does not import its Transformer as `T`", node.lineno)
            continue
        module, class_name = transform_import
        transform_path = module.replace(".", "/") + ".py"
        if transform_path.startswith("linkmerce/"):
            transform_path = "src/" + transform_path
        try:
            transform_tree = ast.parse(content_for(args, transform_path), filename=transform_path)
        except (RuntimeError, FileNotFoundError, SyntaxError) as exc:
            audit.error(path, f"cannot inspect Transformer `{transform_path}`: {exc}", node.lineno)
            continue
        transform_tables = literal_tables(transform_tree).get(class_name)
        if transform_tables is None:
            audit.error(transform_path, f"cannot resolve literal tables for Transformer `{class_name}`")
        elif api_tables != transform_tables:
            audit.error(
                path,
                f"API decorator tables {api_tables} differ from {class_name}.tables {transform_tables}",
                node.lineno,
            )


def audit_dag_doc(audit: Audit, path: str, tree: ast.Module, changed: set[int]) -> None:
    if not path.startswith("airflow/dags/") or not changed:
        return
    if not tree.body or not isinstance(tree.body[0], ast.Expr) or not line_intersects(tree.body[0], changed):
        return
    doc = ast.get_docstring(tree, clean=False)
    if not doc:
        audit.error(path, "changed DAG requires module doc_md content")
        return
    headings = ["## 인증(Credentials)", "## 추출(Extract)", "## 변환(Transform)", "## 적재(Load)"]
    positions = [doc.find(heading) for heading in headings]
    if any(position < 0 for position in positions) or positions != sorted(positions):
        audit.error(path, f"DAG doc heading order must be: {' -> '.join(headings)}")


def audit_python(audit: Audit, args: argparse.Namespace, path: str, changed: set[int]) -> None:
    try:
        source = content_for(args, path)
    except (RuntimeError, FileNotFoundError) as exc:
        audit.error(path, f"cannot read review content: {exc}")
        return
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as exc:
        audit.error(path, f"Python syntax error: {exc.msg}", exc.lineno)
        return
    audit_multiline_equals(audit, path, source, changed)
    audit_function_indents(audit, path, source, tree, changed)
    audit_docstrings(audit, path, tree, changed)
    audit_dag_doc(audit, path, tree, changed)
    if path.endswith("/transform.py"):
        audit_transform_sql(audit, args, path, tree)
    if path.startswith("src/linkmerce/api/"):
        audit_api_tables(audit, args, path, tree, changed)


def audit_json(audit: Audit, args: argparse.Namespace, path: str) -> None:
    try:
        json.loads(content_for(args, path))
    except (json.JSONDecodeError, RuntimeError, FileNotFoundError) as exc:
        audit.error(path, f"invalid JSON: {exc}")


def audit_dbt_pairs(audit: Audit, paths: list[str]) -> None:
    changed = set(paths)
    for path in paths:
        if path.startswith("dbt_bigquery/"):
            pair = "dbt_postgres/" + path.removeprefix("dbt_bigquery/")
        elif path.startswith("dbt_postgres/"):
            pair = "dbt_bigquery/" + path.removeprefix("dbt_postgres/")
        else:
            continue
        if Path(path).suffix not in {".sql", ".yml", ".md"}:
            continue
        if (REPO / pair).exists() and pair not in changed:
            audit.warn(path, f"paired dbt file is unchanged: {pair}")


def audit_versions(audit: Audit, args: argparse.Namespace, paths: list[str]) -> None:
    if not ({"pyproject.toml", "uv.lock"} & set(paths)):
        return
    try:
        pyproject = content_for(args, "pyproject.toml")
        lockfile = content_for(args, "uv.lock")
    except (RuntimeError, FileNotFoundError) as exc:
        audit.error("pyproject.toml", f"cannot compare release versions: {exc}")
        return
    project = re.search(r"(?m)^version = \"([^\"]+)\"", pyproject)
    package = re.search(
        r"(?ms)^name = \"linkmerce\"\s+version = \"([^\"]+)\"",
        lockfile,
    )
    if not project or not package:
        audit.error("uv.lock", "cannot locate LinkMerce versions in pyproject.toml and uv.lock")
    elif project.group(1) != package.group(1):
        audit.error("uv.lock", f"version {package.group(1)} differs from pyproject {project.group(1)}")


def audit_new_extractors(
        audit: Audit,
        args: argparse.Namespace,
        new_paths: set[str],
    ) -> None:
    api_paths = repository_paths(args, "src/linkmerce/api")
    api_content = {
        path: content_for(args, path)
        for path in api_paths
        if path.endswith(".py")
    }
    try:
        extract_tests = content_for(args, "src/tests/test_extract.py")
        transform_tests = content_for(args, "src/tests/test_transform.py")
    except (RuntimeError, FileNotFoundError):
        extract_tests = ""
        transform_tests = ""

    for path in sorted(new_paths):
        if not path.startswith("src/linkmerce/core/") or not path.endswith("/extract.py"):
            continue
        package = path.removeprefix("src/").removesuffix("/extract.py").replace("/", ".")
        siblings = [
            str(Path(path).with_name("__init__.py")).replace("\\", "/"),
            str(Path(path).with_name("transform.py")).replace("\\", "/"),
            str(Path(path).with_name("models.sql")).replace("\\", "/"),
        ]
        for sibling in siblings:
            try:
                content_for(args, sibling)
            except (RuntimeError, FileNotFoundError):
                audit.error(path, f"new Extractor requires sibling `{sibling}`")
        extract_module = package + ".extract"
        transform_module = package + ".transform"
        if not any(extract_module in text and transform_module in text for text in api_content.values()):
            audit.error(path, "new Extractor-Transformer pair is not linked from src/linkmerce/api")
        if extract_module not in extract_tests:
            audit.error("src/tests/test_extract.py", f"missing Extractor test import for `{extract_module}`")
        if transform_module not in transform_tests:
            audit.error("src/tests/test_transform.py", f"missing Transformer test import for `{transform_module}`")


def main() -> int:
    args = parse_args()
    audit = Audit()
    try:
        paths = changed_files(args)
        new_paths = added_files(args)
        changed = added_lines(args, paths)
    except RuntimeError as exc:
        print(f"ERROR git - {exc}")
        return 2

    if not paths:
        print("No files found on the selected review surface.")
        return 0

    for path in paths:
        if path.endswith(".py") and path.startswith(PYTHON_AREAS):
            audit_python(audit, args, path, changed.get(path, set()))
        elif path.endswith(".json"):
            audit_json(audit, args, path)

    audit_dbt_pairs(audit, paths)
    audit_versions(audit, args, paths)
    audit_new_extractors(audit, args, new_paths)

    print(f"Audited {len(paths)} changed file(s).")
    for message in audit.errors:
        print(message)
    for message in audit.warnings:
        print(message)
    if audit.errors:
        print(f"FAILED with {len(audit.errors)} error(s) and {len(audit.warnings)} warning(s).")
        return 1
    print(f"PASSED with {len(audit.warnings)} warning(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
