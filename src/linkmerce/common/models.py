from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Sequence
    from pathlib import Path


class Models:
    def __init__(self, path: str | Path):
        self.set_path(path)

    def get_path(self) -> str | Path:
        return self.__path

    def set_path(self, path: str | Path):
        import os
        if os.path.exists(path):
            self.__path = path
        else:
            raise FileNotFoundError("Models path not found.")

    def read_models(self, name: str, keys: Sequence[str] = list(), full_scan: bool = False) -> dict[str,str]:
        return read_models(self.get_path(), name, keys, full_scan)


def read_models(path: str, name: str, keys: Sequence[str] = list(), full_scan: bool = False) -> dict[str,str]:
    if full_scan or (not keys):
        queires = read_models_all_lines(path, name, keys)
    else:
        queires = read_models_by_line(path, name, keys)

    if keys and (set(keys) != set(queires)):
        missing = set(keys) - set(queires)
        raise KeyError("Missing keys in models: {}".format(', '.join(missing)))
    return queires


def read_models_all_lines(path: str, name: str, keys: Sequence[str] = list()) -> dict[str,str]:
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


def read_models_by_line(path: str, name: str, keys: Sequence[str]) -> dict[str,str]:
    queries, lines = dict(), list()

    key_set, key = set(keys), None
    def init_key(line: str) -> str:
        if line.startswith(f"-- {name}"):
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
