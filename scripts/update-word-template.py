"""Обновляет стандартный DOCX-шаблон без изменения его оформления."""

from __future__ import annotations

import argparse
import os
import tempfile
import zipfile
from pathlib import Path


def replace_marker(path: Path) -> None:
    with zipfile.ZipFile(path, "r") as source:
        document = source.read("word/document.xml")
        if b"{{PROTECTION}}" in document:
            return
        if b"AES-256" not in document:
            raise RuntimeError("В шаблоне не найдено значение AES-256 для миграции")
        document = document.replace(b"AES-256", b"{{PROTECTION}}", 1)
        entries = [(item, document if item.filename == "word/document.xml" else source.read(item.filename)) for item in source.infolist()]
    handle, temporary_name = tempfile.mkstemp(suffix=".docx", dir=path.parent)
    os.close(handle)
    temporary = Path(temporary_name)
    try:
        with zipfile.ZipFile(temporary, "w") as target:
            for item, payload in entries:
                target.writestr(item, payload)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("template", type=Path)
    arguments = parser.parse_args()
    replace_marker(arguments.template.resolve())


if __name__ == "__main__":
    main()
