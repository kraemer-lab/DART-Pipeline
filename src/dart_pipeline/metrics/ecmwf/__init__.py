import datetime
from typing import Final
from pathlib import Path

import ecmwf.opendata

from ...paths import get_path
from ...metrics import register_metrics, register_fetch, MetricInfo

METRICS: dict[str, MetricInfo] = {
    "forecast": {"description": "ECMWF open forecast data", "unit": "various"}
}

register_metrics(
    "ecmwf",
    description="ECMWF non-ERA5 data, including forecasts",
    license_text="""Access to Copernicus Products is given for any purpose in so far
as it is lawful, whereas use may include, but is not limited to: reproduction;
distribution; communication to the public; adaptation, modification and
combination with other data and information; or any combination of the
foregoing.""",
    metrics=METRICS,
)

VALID_START_HOURS = [0, 6, 12, 18]
VALID_OFFSETS = [-4, -3, -2, -1, 0]


@register_fetch("ecmwf.forecast")
def get_ecmwf_forecast_open_data(start_hour: int = 0, offset: int = 0) -> Path:
    """
    Downloads data from the Ensemble prediction model (ENFO) from the ECMWF
    open data server. The data downloaded will be in a grib file, and will
    contain the forecast for the whole world.

    Parameters
    ----------
    start_hour
        Starting hour (in UTC timezone) of the forecast, must be one
        of [0, 6, 12, 18]. Default is 0 UTC.
    offset
        Start date of the forecast, with 0 representing the forecast starting on the
        present day, -1 representing the forecast starting yesterday, -2 the day before,
        and so on, till -4. Default is 0 for current forecast

    Returns
    -------
    Open ECMWF forecast data in grib file with the following variables:
    2 meter temp, 2 meter dewpoint, surface pressure,precipitation,
    10 meter u and v wind components, evaporation, surface solar radiation
    """
    step: Final[int] = 6
    if start_hour not in VALID_START_HOURS:
        raise ValueError(
            f"start_hour must be one of {VALID_START_HOURS}, got {start_hour=}"
        )
    if offset not in VALID_OFFSETS:
        raise ValueError(f"offset must be one of {VALID_OFFSETS}, got {offset=}")

    date = datetime.datetime.now(datetime.timezone.utc).date() + datetime.timedelta(
        days=offset
    )
    output_path = get_path(
        "sources", "WLD", "ecmwf", f"WLD-{date}-ecmwf.forecast.grib2"
    )

    client = ecmwf.opendata.Client(source="ecmwf")
    client.retrieve(
        time=start_hour,
        date=offset,
        stream="enfo",  # ensemble prediction forecast
        type=[
            "pf",  # perturbed simulation
            "cf",  # control simulation
        ],
        # 360h = 15 days, limit of forecast model
        step=list(range(0, 361, step)),
        param=[
            "2t",  # 2 meter temperature
            "2d",  # 2 meter dew point
            "sp",  # surfare pressure
            "tp",  # total precipitation
            "10u",  # 10m u-component wind speed
            "10v",  # 10m v-component wind speed
            "ssrd",  # surface solar radiation downwards
        ],
        target=str(output_path),
    )
    return output_path
