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
]

VARIABLE_MAPPINGS = {
    "2m_temperature": "t2m",
    "surface_solar_radiation_downwards": "ssrd",
    "2m_dewpoint_temperature": "d2m",
    "surface_pressure": "sp",
    "evaporation": "e",
    "total_precipitation": "tp",
    "total_precipitation_corrected": "tp_bc",
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
}

depends_hydrological_balance = ["total_precipitation", "evaporation"]
METRICS: dict[str, MetricInfo] = {
    "core": {
        "long_name": "Virtual metric to run all core metrics at once, indicated by part of: era5.core",
        "units": "various",
    },
    "2m_temperature": {
        "long_name": "2 meters air temperature",
        "units": "K",
        "standard_name": "air_temperature",
        "valid_min": 225,
        "valid_max": 325,
        "part_of": "era5.core",
        "short_name": "t2m",
        "short_name_min": "mn2t",
        "short_name_max": "mx2t",
    },
    "total_precipitation": {
        "long_name": "Total precipitation",
        "units": "m",
        "valid_min": 0,
        "valid_max": 1200,
        "part_of": "era5.core",
        "short_name": "tp",
    },
    "relative_humidity": {
        "long_name": "Relative humidity",
        "depends": ["2m_temperature", "2m_dewpoint_temperature", "surface_pressure"],
        "valid_min": 0,
        "valid_max": 100,
        "standard_name": "relative_humidity",
        "units": "percent",
        "part_of": "era5.core",
        "short_name": "r",
    },
    "specific_humidity": {
        "long_name": "Specific humidity",
        "depends": ["2m_temperature", "2m_dewpoint_temperature", "surface_pressure"],
        "valid_min": 0,
        "valid_max": 30,
        "standard_name": "specific_humidity",
        "units": "g kg-1",
        "part_of": "era5.core",
        "short_name": "q",
    },
    "hydrological_balance": {
        "long_name": "Hydrological balance",
        "depends": depends_hydrological_balance,
        "units": "m",
        "part_of": "era5.core",
        "short_name": "hb",
    },
    "spi": {
        "long_name": "Standardised precipitation",
        "depends": ["total_precipitation"],
        "units": "1",
        "short_name": "spi",
    },
    # actually depends on potential_evapotranspiration which depends on 2m_temperature.daily_{min,mean,max}
    "spei": {
        "long_name": "Standardised precipitation-evaporation index",
        "depends": ["total_precipitation", "2m_temperature"],
        "units": "unitless",
        "short_name": "spei",
    },
    "total_precipitation_corrected": {
        "long_name": "Bias-corrected total precipitation",
        "units": "m",
        "short_name": "tp_bc",
    },
    "spi_corrected": {
        "long_name": "Bias-corrected standardised precipitation",
        "depends": ["total_precipitation"],
        "units": "1",
        "short_name": "spi_bc",
    },
    "spei_corrected": {
        "long_name": "Bias-corrected standardised precipitation-evaporation index",
        "depends": ["total_precipitation", "2m_temperature"],
        "units": "1",
        "short_name": "spei_bc",
    },
    "hydrological_balance_corrected": {
        "long_name": "Bias-corrected hydrological balance",
        "depends": depends_hydrological_balance,
        "units": "m",
        "part_of": "era5.core",
        "short_name": "hb_bc",
    },
    "spi.gamma": {
        "long_name": "Fitted gamma distribution from historical data for SPI",
        "units": "1",
        "depends": ["total_precipitation"],
    },
    "spei.gamma": {
        "long_name": "Fitted gamma distribution from historical data for SPEI",
        "units": "unitless",
        "depends": ["2m_temperature", "total_precipitation"],
    },
    "spi_corrected.gamma": {
        "long_name": "Fitted gamma distribution from historical data for SPI with corrected precipitation",
        "units": "1",
        "depends": ["total_precipitation"],
    },
    "spei_corrected.gamma": {
        "long_name": "Fitted gamma distribution from historical data for SPEI with corrected precipitation",
        "units": "1",
        "depends": ["2m_temperature", "total_precipitation"],
    },
    "prep_bias_correct": {
        "long_name": "Virtual metric to prepare aggregated data for bias correction module",
        "units": "various",
        "depends": ["2m_temperature", "total_precipitation"],
    },
}

VARIABLES = sorted(set(sum([METRICS[m].get("depends", [m]) for m in METRICS], [])))

INSTANT_METRICS = [m for m in METRICS if m not in ACCUM_METRICS]
DERIVED_METRICS_SEPARATE_IMPL = [
    "spi",
    "spei",
    "spi_corrected",
    "spei_corrected",
    "spi.gamma",
    "spei.gamma",
    "spi_corrected.gamma",
    "spei_corrected.gamma",
    "prep_bias_correct",
]
