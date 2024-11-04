import re
from typing import NamedTuple, Literal, Protocol
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

Credentials = tuple[str, str]
ProcessResult = tuple[pd.DataFrame, str]
AdminLevel = Literal["0", "1", "2"]


class DefaultPathProtocol(Protocol):
    def __call__(self, source: str, path: str | None | Path = None) -> Path:
        ...


class PartialDate(NamedTuple):
    year: int
    month: int | None = None
    day: int | None = None

    @staticmethod
    def from_string(date: str) -> "PartialDate":
        if re.match(r"^[12]\d\d\d$", date):
            return PartialDate(int(date))
        if re.match(r"^[12]\d\d\d-[01]\d$", date):
            dt = datetime.strptime(date, "%Y-%m")
            return PartialDate(dt.year, dt.month)
        dt = datetime.fromisoformat(date)
        return PartialDate(dt.year, dt.month, dt.day)

    def to_string(self, sep="-") -> str:
        s = str(self.year)
        if self.month is not None:
            s += f"{sep}{self.month:02d}"
            if self.day is not None:
                s += f"{sep}{self.day:02d}"
        return s

    @property
    def zero_padded_month(self) -> str:
        return f"{self.month:02d}"

    @property
    def zero_padded_day(self) -> str:
        return f"{self.day:02d}"

    @property
    def scope(self) -> Literal["annual", "monthly", "daily"]:
        if self.day is not None:
            return "daily"
        if self.month is not None:
            return "monthly"
        return "annual"

    __str__ = to_string


@dataclass
class URLCollection:
    base_url: str
    files: list[str]
    relative_path: str = "."
    unpack: bool = True  # automatically unpack .zip, .7z and .gz files
    unpack_create_folder: bool = (
        False  # do not create folders by default when unpacking
    )

    def show(self, show_links: bool = False) -> str:
        "Pretty printer for URLCollection"
        file_list_str = (
            self.files[0] if len(self.files) == 1 else f" [{len(self.files)} links]"
        )
        s = f"{self.base_url}/{file_list_str}"
        return (
            s
            if not show_links
            else (
                s + "\n" + "\n".join(f"    {file}" for file in self.files)
                if len(self.files) > 1
                else s
            )
        )

    __str__ = show

    def disk_files(self, root: str | Path) -> list[Path]:
        "List of files on disk corresponding to this URLCollection"
        return [(Path(root) / self.relative_path / Path(f).name) for f in self.files]

    def missing_files(self, root: str | Path) -> list[Path]:
        """Return True if all files in this URLCollection exist."""
        return [p for p in self.disk_files(root) if not p.exists()]


class DataFile(NamedTuple):
    file: str
    relative_path: str
    data: str | bytes
