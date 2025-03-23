from ...types import URLCollection


def aphrodite_precipitation_data() -> list[URLCollection]:
    "APHRODITE Daily accumulated precipitation (V1901) [requires account]"
    base_url = "http://aphrodite.st.hirosaki-u.ac.jp"
    return [
        # 0.05 degree
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/005deg",
            ["APHRO_MA_PREC_CLM_005deg_V1901.ctl.gz"],
        ),
        # 0.25 degree
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/025deg",
            [
                "APHRO_MA_025deg_V1901.2015.gz",
                "APHRO_MA_025deg_V1901.ctl.gz",
            ],
        ),
        # 0.25 degree nc
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/025deg_nc",
            ["APHRO_MA_025deg_V1901.2015.nc.gz"],
        ),
        # 0.50 degree
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/050deg",
            [
                "APHRO_MA_050deg_V1901.2015.gz",
                "APHRO_MA_050deg_V1901.ctl.gz",
            ],
        ),
        # 0.50 degree nc
        URLCollection(
            f"{base_url}/product/APHRO_V1901/APHRO_MA/050deg_nc",
            ["APHRO_MA_050deg_V1901.2015.nc.gz"],
        ),
    ]


def aphrodite_temperature_data() -> list[URLCollection]:
    "APHRODITE Daily mean temperature product (V1808) [requires account]"

    base_url = "http://aphrodite.st.hirosaki-u.ac.jp"
    return [
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
