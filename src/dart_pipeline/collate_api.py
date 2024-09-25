"""
Collate module for API based retrievals which require
direct download to a file
"""
from pathlib import Path

import cdsapi


def download_era5_reanalysis_data(path: Path):
    """
    Download ERA5 atmospheric reanalysis data.

    How to use the Climate Data Store (CDS) Application Program Interface
    (API): https://cds.climate.copernicus.eu/api-how-to

    A Climate Data Store account is needed, see https://pypi.org/project/cdsapi/
    """

    c = cdsapi.Client()
    request = {
        "date": "2013-01-01",  # The hyphens can be omitted
        # 1 is top level, 137 the lowest model level in ERA5. Use '/' to
        # separate values.
        "levelist": "1/10/100/137",
        "levtype": "ml",
        # Full information at https://apps.ecmwf.int/codes/grib/param-db/
        # The native representation for temperature is spherical harmonics
        "param": "130",
        # Denotes ERA5. Ensemble members are selected by 'enda'
        "stream": "oper",
        # You can drop :00:00 and use MARS short-hand notation, instead of
        # '00/06/12/18'
        "time": "00/to/23/by/6",
        "type": "an",
        # North, West, South, East. Default: global
        "area": "80/-50/-25/0",
        # Latitude/longitude. Default: spherical harmonics or reduced Gaussian
        # grid
        "grid": "1.0/1.0",
        # Output needs to be regular lat-lon, so only works in combination
        # with 'grid'!
        "format": "netcdf",
    }
    c.retrieve(
        # Requests follow MARS syntax
        # Keywords 'expver' and 'class' can be dropped. They are obsolete
        # since their values are imposed by 'reanalysis-era5-complete'
        "reanalysis-era5-complete",
        request,
        # Output file. Adapt as you wish.
        path / "ERA5-ml-temperature-subarea.nc",
    )
