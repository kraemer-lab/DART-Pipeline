from dart_pipeline.types import URLCollection
from dart_pipeline.metrics.aphrodite.collate import (
    aphrodite_temperature_data,
    aphrodite_precipitation_data,
)


def test_aphrodite_temperature_data():
    """Test the collation of links for APHRODITE temperature data."""
    result = aphrodite_temperature_data()
    # Base URL
    base_url = "http://aphrodite.st.hirosaki-u.ac.jp"
    # Expected output
    expected = [
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/050deg_nc",
            [
                "APHRO_MA_TAVE_050deg_V1808.2015.nc.gz",  # 19 MB
                "APHRO_MA_TAVE_050deg_V1808.nc.ctl.gz",  # 347 B
            ],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/050deg",
            ["read_aphro_v1808.f90"],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/025deg_nc",
            [
                "APHRO_MA_TAVE_025deg_V1808.2015.nc.gz",  # 64 MB
                "APHRO_MA_TAVE_025deg_V1808.nc.ctl.gz",  # 485 B
            ],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/025deg",
            [
                "APHRO_MA_TAVE_025deg_V1808.2015.gz",  # 64 MB
                "APHRO_MA_TAVE_025deg_V1808.ctl.gz",  # 312 B
                "read_aphro_v1808.f90",  # 2.6 KB
            ],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/005deg_nc",
            [
                "APHRO_MA_TAVE_CLM_005deg_V1808.nc.gz",  # 1.2 GB
            ],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1808_TEMP/APHRO_MA/005deg",
            [
                "APHRO_MA_TAVE_CLM_005deg_V1808.ctl.gz",  # 334 B
                "APHRO_MA_TAVE_CLM_005deg_V1808.grd.gz",  # 1.4 GB
                "read_aphro_clm_v1808.f90",  # 2.1 KB
            ],
        ),
    ]

    assert result == expected


def test_aphrodite_precipitation_data():
    """Test the collation of links for APHRODITE precipitation data."""
    result = aphrodite_precipitation_data()
    # Base URL
    base_url = "http://aphrodite.st.hirosaki-u.ac.jp"
    # Expected output
    expected = [
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/005deg",
            ["APHRO_MA_PREC_CLM_005deg_V1901.ctl.gz"],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/025deg",
            [
                "APHRO_MA_025deg_V1901.2015.gz",
                "APHRO_MA_025deg_V1901.ctl.gz",
            ],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/025deg_nc",
            ["APHRO_MA_025deg_V1901.2015.nc.gz"],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/050deg",
            [
                "APHRO_MA_050deg_V1901.2015.gz",
                "APHRO_MA_050deg_V1901.ctl.gz",
            ],
        ),
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/050deg_nc",
            ["APHRO_MA_050deg_V1901.2015.nc.gz"],
        ),
    ]

    assert result == expected
