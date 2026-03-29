from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Sequence
    from pathlib import Path


class Models:
    """SQL 파일을 읽고 관리하는 클래스."""

    def __init__(self, path: str | Path):
        self.set_path(path)

    def get_path(self) -> str | Path:
        """현재 설정된 SQL 파일 경로를 반환한다."""
        return self.__path

    def set_path(self, path: str | Path):
        """SQL 파일 경로를 설정한다. 경로가 존재하지 않으면 `FileNotFoundError`를 발생시킨다."""
        import os
        if os.path.exists(path):
            self.__path = path
        else:
            raise FileNotFoundError("Models path not found.")

    def read_models(self, name: str, keys: Sequence[str] = list(), full_scan: bool = False) -> dict[str, str]:
        """지정된 이름의 SQL 쿼리를 파일에서 읽어 딕셔너리로 반환한다."""
        return read_models(self.get_path(), name, keys, full_scan)


def read_models(path: str, name: str, keys: Sequence[str] = list(), full_scan: bool = False) -> dict[str, str]:
    """SQL 파일에서 지정된 이름과 키에 해당하는 쿼리를 읽어 반환한다."""
    if full_scan or (not keys):
        queires = read_models_all_lines(path, name, keys)
    else:
        queires = read_models_by_line(path, name, keys)

    if keys and (set(keys) != set(queires)):
        missing = set(keys) - set(queires)
        raise KeyError("Missing keys in models: {}".format(', '.join(missing)))
    return queires


def read_models_all_lines(path: str, name: str, keys: Sequence[str] = list()) -> dict[str, str]:
    """SQL 파일 전체를 읽어 해당 이름의 모든 쿼리를 파싱한다.

    하나의 쿼리는 `-- 이름: 키` 주석 라인으로 시작하여, 다음 주석 라인 또는 라인 끝까지의 범위로 구분한다."""
    queries, indices = dict(), list()
    with open(path, 'r', encoding="utf-8") as file:
        lines = file.read().split('\n')

    for index, line in enumerate(lines):
        if line.startswith("--"):
            indices.append(index)
            if line.startswith(f"-- {name}:"):
                queries[line[len(f"-- {name}:"):].strip()] = index

    indices.append(None)
    index_map = {index: indices[i+1] for i, index in enumerate(indices[:-1])}
    return {key: '\n'.join(lines[index+1:index_map[index]]).strip()
                for key, index in queries.items() if (not keys) or (key in keys)}


def read_models_by_line(path: str, name: str, keys: Sequence[str]) -> dict[str, str]:
    """SQL 파일을 한 줄씩 읽으며 지정된 키에 해당하는 쿼리만 추출한다.

    하나의 쿼리는 `-- 이름: 키` 주석 라인으로 시작하여, 다음 주석 라인 또는 라인 끝까지의 범위로 구분한다."""
    queries, lines = dict(), list()

    key_set, key = set(keys), None
    def init_key(line: str) -> str:
        if line.startswith(f"-- {name}:"):
            key = line[len(f"-- {name}:"):].strip()
            if key in key_set:
                return key

    with open(path, 'r', encoding="utf-8") as file:
        for line in file:
            if not key_set:
                break
            elif not key:
                key = init_key(line)
            elif not line.startswith(f"--"):
                lines.append(line)
            else:
                if lines:
                    queries[key] = ''.join(lines).strip()
                    key_set.remove(key)
                key = init_key(line)
                lines = list()
        if lines:
            queries[key] = ''.join(lines).strip()
    return queries
