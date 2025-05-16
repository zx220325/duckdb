#!/usr/bin/env python3

import re
import subprocess
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from os import PathLike


SYMBOL_NAME_RE = re.compile("""[\w~]+::(?:\w|~|::)+""")

duckdb_namespaces: set[str] = set()


def main():
    for path in sys.argv[1:]:
        _process_file(path)

    global duckdb_namespaces
    for namespace in sorted(duckdb_namespaces):
        print(namespace)


def _process_file(file: "PathLike"):
    symbols = _run_nm(file)
    for s in symbols:
        for n in SYMBOL_NAME_RE.findall(s):
            namespace = n.split("::")[0]
            if namespace.startswith("duckdb"):
                global duckdb_namespaces
                duckdb_namespaces.add(namespace)


def _run_nm(file: "PathLike") -> list[str]:
    process = subprocess.run(
        ["nm", "-C", "-f", "just-symbols", str(file)],
        capture_output=True,
        check=True,
        encoding="utf-8",
    )
    return process.stdout.splitlines()


main()
