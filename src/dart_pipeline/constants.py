"""Global constants."""

COMPRESSED_FILE_EXTS = [
    ".tif.gz",
    ".tar.gz",
    ".tar.bz2",
    ".zip",
    ".7z",
    ".gz",
    ".nc.gz",
    ".ctl.gz",
    ".grd.gz",
]

COL_BLUE = "\033[0;34m"
COL_CYAN = "\033[0;36m"
COL_OFF = "\033[0m"

MSG_SOURCE = COL_BLUE + "  source" + COL_OFF
MSG_PROCESS = COL_CYAN + " process" + COL_OFF

# Smallest single-precision floating-point number
MIN_FLOAT = -3.4028234663852886e38

# Column names in the output CSVs
OUTPUT_COLUMNS = [
    "iso3",
    "admin_level_0",
    "admin_level_1",
    "admin_level_2",
    "admin_level_3",
    "year",
    "month",
    "day",
    "week",
    "metric",
    "value",
    "unit",
    "resolution",
    "creation_date",
]
