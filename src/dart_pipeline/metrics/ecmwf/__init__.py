"""Module to process ECMWF Forecast Open Data"""

import logging
from pathlib import Path
from datetime import datetime, date

import ecmwf.opendata
from geoglue.region import gadm

from ...paths import get_path
from ...metrics import register_metrics, register_fetch, MetricInfo

from .forecast import VARIABLES, forecast_grib_to_netcdf

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

logger = logging.getLogger(__name__)


def forecast_path(date: str | date) -> Path:
    return get_path("sources", "WLD", "ecmwf", f"WLD-{date}-ecmwf.forecast.grib2")


@register_fetch("ecmwf.forecast")
def get_forecast_open_data(
    iso3: str,
    date: str,
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
        can be today or at most 4 days in the past
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
        client.retrieve(
            time=start_hour,
            date=offset,
            stream="enfo",  # ensemble prediction forecast
            # perturbed and control simulation
            type=["pf", "cf"],
            step=list(range(0, 361, step_hours)),  # 360h = 15d, forecast model limit
            param=VARIABLES,
            target=str(output_path),
        )
        logger.info("Downloaded forecast in: %s", output_path)
    else:
        logger.info("Using already retrieved forecast file: %s", output_path)
    region = gadm(iso3, 1)
    extents = region.bbox
    sel_kwargs = {
        "latitude": slice(extents.maxy, extents.miny),
        "longitude": slice(extents.minx, extents.maxx),
    }
    instant, accum = forecast_grib_to_netcdf(forecast_path(date), sel_kwargs)
    sources_path = get_path("sources", iso3, "ecmwf")
    instant_file = sources_path / f"{iso3}-{date}-ecmwf.forecast.instant.nc"
    accum_file = sources_path / f"{iso3}-{date}-ecmwf.forecast.accum.nc"
    instant.to_netcdf(instant_file)
    logger.info("Wrote %s", instant_file)
    accum.to_netcdf(accum_file)
    logger.info("Wrote %s", accum_file)
    return [instant_file, accum_file]
