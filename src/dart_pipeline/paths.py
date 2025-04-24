"Standard data paths for DART-Pipeline"

import os
import platform
from pathlib import Path
from typing import Literal

if platform.system() == "Windows":
    DATA_HOME = Path(os.getenv("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
else:
    DATA_HOME = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))


def get_path(section: Literal["sources", "output", "scratch"], *args) -> Path:
    """Returns standardised path for a metric source

    By convention, DART-Pipeline stores downloaded cached data in
    ~/.local/share/dart-pipeline on POSIX systems and
    %LOCALAPPDATA%\\dart-pipeline on Windows systems. The base path can be
    overridden by either passing the ``root`` parameter here or by setting the
    DART_PIPELINE_DATA_HOME environment variable.

    Parameters
    ----------
    section
        One of ``sources``, ``output`` or ``scratch``, where ``scratch``
        is used for intermediate files (like CDO resampling output)
    source
        Source, such as ``era5`` or ``worldpop``
    root
        Base path to use. If specified, takes precedence over DART_PIPELINE_DATA_HOME

    Returns
    -------
    Path to folder for a particular ISO3 source combination in a section
    """

    root = Path(os.getenv("DART_PIPELINE_DATA_HOME") or DATA_HOME / "dart-pipeline")
    args = list(args)
    last = args.pop() if args and "." in args[-1] else None
    if not (path := Path(root, section, *args)).exists():
        path.mkdir(parents=True, exist_ok=True)
    return path / last if last else path
