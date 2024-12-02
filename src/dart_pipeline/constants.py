"""Global constants."""

COMPRESSED_FILE_EXTS = [".tif.gz", ".tar.gz", ".tar.bz2", ".zip", ".7z"]

INTEGER_PARAMS = ["year"]

DEFAULT_SOURCES_ROOT = "data/sources"
DEFAULT_OUTPUT_ROOT = "data/processed"
DEFAULT_PLOTS_ROOT = "data/plots"

COL_BLUE = "\033[0;34m"
COL_CYAN = "\033[0;36m"
COL_OFF = "\033[0m"

MSG_SOURCE = COL_BLUE + "  source" + COL_OFF
MSG_PROCESS = COL_CYAN + " process" + COL_OFF

TERRACLIMATE_METRICS = [
    "aet",  # water_evaporation_amount_mm
    "def",  # water_potential_evaporation_amount_minus_water_evaporatio
    "pdsi",  # palmer_drought_severity_index (unitless)
    "pet",  # water_potential_evaporation_amount_mm
    "ppt",  # precipitation_amount_mm
    "q",  # runoff_amount_mm
    "soil",  # soil_moisture_content_mm
    "srad",  # downwelling_shortwave_flux_in_air_W_per_m_squared
    "swe",  # liquid_water_content_of_surface_snow_mm
    "tmax",  # air_temperature_max_degC
    "tmin",  # air_temperature_min_degC
    "vap",  # water_vapor_partial_pressure_in_air_kPa
    "vpd",  # vapor_pressure_deficit_kPa
    "ws",  # wind_speed_m_per_s
]

PERU_REGIONS = [
    "AMAZONAS",
    "ANCASH",
    "AREQUIPA",
    "AYACUCHO",
    "CAJAMARCA",
    "CALLAO",
    "CUSCO",
    "HUANUCO",
    "ICA",
    "JUNIN",
    "LA LIBERTAD",
    "LAMBAYEQUE",
    "LIMA",
    "LORETO",
    "MADRE DE DIOS",
    "MOQUEGUA",
    "PASCO",
    "PIURA",
    "PUNO",
    "SAN MARTIN",
    "TUMBES",
    "UCAYALI",
]
