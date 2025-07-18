"""Module to process ECMWF Forecast Open Data"""

import logging
from pathlib import Path
from datetime import datetime, date

import ecmwf.opendata
import requests.exceptions
from geoglue.region import gadm

from ...paths import get_path
from ...metrics import register_metrics, register_fetch, register_process, MetricInfo
from ...util import iso3_admin_unpack

from .forecast import VARIABLES, forecast_grib_to_netcdf, forecast_zonal_stats

METRICS: dict[str, MetricInfo] = {
    "forecast": {"long_name": "ECMWF open forecast data", "units": "various"}
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

logger = logging.getLogger(__name__)


def forecast_path(date: str | date) -> Path:
    return get_path("sources", "WLD", "ecmwf", f"WLD-{date}-ecmwf.forecast.grib2")


@register_fetch("ecmwf.forecast")
def get_forecast_open_data(
    iso3: str,
    date: str | None = None,
    start_hour: int = 0,
    step_hours: int = 6,
    overwrite: bool = False,
) -> list[Path]:
    """
    Downloads data from the Ensemble prediction model (ENFO) from the ECMWF
    open data server. The data downloaded is in a GRIB2 file for the whole world, and is
    converted to netCDF files (one each for instant and accumulative variables)
    cropped to the country bounds.

    Parameters
    ----------
    iso3 : str
        ISO3 code of country
    date : str
        Date in ISO format (YYYY-MM-DD) for which to download forecast,
        can be today or at most 4 days in the past. Default is to use today's forecast
    start_hour : int
        Starting hour (in UTC timezone) of the forecast, must be one
        of [0, 6, 12, 18]. Default is 0 UTC.
    step_hours : int
        Timestep interval for the forecast, minimum is 6, must be a multiple of 6.
        The default timestep interval is 6h
    overwrite : bool
        Whether to overwrite existing data file if downloaded, default=False

    Returns
    -------
    list[Path]
        A 2-element list containing the GRIB2 file converted to netCDF, with one
        file each for instant (named ``*.instant.nc``), and accumulative
        variables (``*.accum.nc``).
    """
    if "-" in iso3:
        iso3, _ = iso3_admin_unpack(iso3)  # ignore admin
    date = date or datetime.today().date().isoformat()
    if start_hour not in VALID_START_HOURS:
        raise ValueError(
            f"start_hour must be one of {VALID_START_HOURS}, got {start_hour=}"
        )
    if step_hours % 6 or step_hours < 6:
        raise ValueError(f"{step_hours=} must be a multiple of 6, with a minimum of 6")
    offset = (datetime.fromisoformat(date).date() - datetime.today().date()).days
    if offset > 0:
        raise ValueError("Can't fetch a forecast from the future")
    if offset < -4:
        raise ValueError("Can't fetch a forecast more than 4 days in the past")
    output_path = forecast_path(date)

    if not output_path.exists() or overwrite:
        logger.info(
            "Retrieving forecast with step_hours=%d for %r", step_hours, VARIABLES
        )
        client = ecmwf.opendata.Client(source="ecmwf")
        try:
            client.retrieve(
                time=start_hour,
                date=offset,
                stream="enfo",  # ensemble prediction forecast
                # perturbed and control simulation
                type=["pf", "cf"],
                step=list(
                    range(0, 361, step_hours)
                ),  # 360h = 15d, forecast model limit
                param=VARIABLES,
                target=str(output_path),
            )
        except requests.exceptions.HTTPError:
            logger.error(
                "Temporary error in forecast retrieval: try after a while, or fetch yesterday's forecast instead"
            )
            raise
        logger.info("Downloaded forecast in: %s", output_path)
    else:
        logger.info("Using already retrieved forecast file: %s", output_path)
    region = gadm(iso3, 1)
    extents = region.bbox.int()
    instant, accum = forecast_grib_to_netcdf(forecast_path(date), extents)
    sources_path = get_path("sources", iso3, "ecmwf")
    instant_file = sources_path / f"{iso3}-{date}-ecmwf.forecast.instant.nc"
    accum_file = sources_path / f"{iso3}-{date}-ecmwf.forecast.accum.nc"
    instant.to_netcdf(instant_file)
    logger.info("Wrote %s", instant_file)
    accum.to_netcdf(accum_file)
    logger.info("Wrote %s", accum_file)
    return [instant_file, accum_file]


@register_process("ecmwf.forecast")
def process_forecast(iso3: str, date: str) -> list[Path]:
    "Processes corrected forecast (run after dart-bias-correct)"

    iso3, admin = iso3_admin_unpack(iso3)
    corrected_forecast_file = get_path(
        "sources", iso3, "ecmwf", f"{iso3}-{date}-ecmwf.forecast.corrected.nc"
    )
    if not corrected_forecast_file.exists():
        raise FileNotFoundError(f"""Corrected forecast file not found at expected location:
        {corrected_forecast_file}
        See https://dart-pipeline.readthedocs.io/en/latest/corrected-forecast.html for information
        on how to generate this file
        """)
    ds = forecast_zonal_stats(iso3, date, admin)
    output = get_path(
        "output", iso3, "ecmwf", f"{iso3}-{admin}-{date}-ecmwf.forecast.nc"
    )
    ds.to_netcdf(output)
    return [output]
