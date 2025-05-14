"""List of ERA5 metrics"""

from .. import MetricInfo

ACCUM_METRICS = [
    "hydrological_balance",
    "total_precipitation",
    "spi",
    "spei",
    "spi_corrected",
    "spei_corrected",
    "hydrological_balance_corrected",
    "total_precipitation_corrected",
    "surface_solar_radiation_downwards",
]

VARIABLE_MAPPINGS = {
    "2m_temperature": "t2m",
    "surface_solar_radiation_downwards": "ssrd",
    "2m_dewpoint_temperature": "d2m",
    "surface_pressure": "sp",
    "evaporation": "e",
    "total_precipitation": "tp",
    "total_precipitation_corrected": "tp_corrected",
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
}

depends_hydrological_balance = ["total_precipitation", "evaporation"]
METRICS: dict[str, MetricInfo] = {
    "2m_temperature": {
        "description": "2 meters air temperature",
        "unit": "K",
        "range": (225, 325),
        "part_of": "era5",
    },
    "surface_solar_radiation_downwards": {
        "description": "Accumulated solar radiation downwards",
        "unit": "J/m^2",
        "range": (0, 1e9),
        "part_of": "era5",
    },
    "total_precipitation": {
        "description": "Total precipitation",
        "unit": "m",
        "range": (0, 1200),
        "part_of": "era5",
    },
    "wind_speed": {
        "description": "Wind speed",
        "depends": ["10m_u_component_of_wind", "10m_v_component_of_wind"],
        "range": (0, 110),
        "unit": "m/s",
        "part_of": "era5",
    },
    "relative_humidity": {
        "description": "Relative humidity",
        "depends": ["2m_temperature", "2m_dewpoint_temperature", "surface_pressure"],
        "range": (0, 100),
        "unit": "percentage",
        "part_of": "era5",
    },
    "specific_humidity": {
        "description": "Specific humidity",
        "depends": ["2m_temperature", "2m_dewpoint_temperature", "surface_pressure"],
        "range": (0, 30),
        "unit": "g/kg",
        "part_of": "era5",
    },
    "hydrological_balance": {
        "description": "Hydrological balance",
        "depends": depends_hydrological_balance,
        "unit": "m",
        "part_of": "era5",
    },
    "spi": {
        "description": "Standardised precipitation",
        "depends": ["total_precipitation"],
        "unit": "unitless",
        "part_of": "era5",
    },
    # actually depends on potential_evapotranspiration which depends on 2m_temperature.daily_{min,mean,max}
    "spei": {
        "description": "Standardised precipitation-evaporation index",
        "depends": ["total_precipitation", "2m_temperature"],
        "unit": "unitless",
        "part_of": "era5",
    },
    "total_precipitation_corrected": {
        "description": "Bias-corrected total precipitation",
        "unit": "m",
        "part_of": "era5",
    },
    "spi_corrected": {
        "description": "Bias-corrected standardised precipitation",
        "depends": ["total_precipitation"],
        "unit": "unitless",
        "part_of": "era5",
    },
    "spei_corrected": {
        "description": "Bias-corrected standardised precipitation-evaporation index",
        "depends": ["total_precipitation", "2m_temperature"],
        "unit": "unitless",
        "part_of": "era5",
    },
    "hydrological_balance_corrected": {
        "description": "Bias-corrected hydrological balance",
        "depends": depends_hydrological_balance,
        "unit": "m",
        "part_of": "era5",
    },
    "spi.gamma": {
        "description": "Fitted gamma distribution from historical data for SPI",
        "unit": "unitless",
        "depends": ["total_precipitation"],
    },
    "spei.gamma": {
        "description": "Fitted gamma distribution from historical data for SPEI",
        "unit": "unitless",
        "depends": ["2m_temperature", "total_precipitation"],
    },
    "prep_bias_correct": {
        "description": "Virtual metric to prepare aggregated data for bias correction module",
        "unit": "various",
        "depends": ["2m_temperature", "total_precipitation"],
    },
}

VARIABLES = sorted(set(sum([METRICS[m].get("depends", [m]) for m in METRICS], [])))

INSTANT_METRICS = [m for m in METRICS if m not in ACCUM_METRICS]
DERIVED_METRICS_SEPARATE_IMPL = [
    "spi",
    "spei",
    "spi.gamma",
    "spei.gamma",
    "prep_bias_correct",
]
