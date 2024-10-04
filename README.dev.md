DART-Pipeline Developer Documentation
=====================================

Development Status
------------------
Done: ✅, in progress: ⏳, not working: ❌

`uv run dart-pipeline get`
 ├── ✅ `uv run dart-pipeline get economic/relative-wealth-index iso3=VNM` 6.309s
 ├── ❌ `uv run dart-pipeline get epidemiological/dengue/peru` 21m31.33s
 ├── ✅ `uv run dart-pipeline get geospatial/gadm iso3=VNM` 17.964s
 ├── ✅ `uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp` 12m56.12s
 ├── ❌ `uv run dart-pipeline get meteorological/aphrodite-daily-precip` 4.551s
 ├── `uv run dart-pipeline get meteorological/chirps-rainfall`
 │   ├── ✅ `uv run dart-pipeline get meteorological/chirps-rainfall year=2023` 7.24s
 │   └── ✅ `uv run dart-pipeline get meteorological/chirps-rainfall year=2023 month=05` 1m32.80s
 ├── ✅ `uv run dart-pipeline get meteorological/terraclimate year=2023` 9m30.29s
 ├── ✅ `uv run dart-pipeline get sociodemographic/meta-pop-density iso3=VNM` 10m25.46s
 ├── ❌ `uv run dart-pipeline get sociodemographic/worldpop-count iso3=VNM`
 └── ❌ `uv run dart-pipeline get sociodemographic/worldpop-density iso3=VNM`

`uv run dart-pipeline process`
 ├── `uv run dart-pipeline process economic/relative-wealth-index`
 │   ├── ✅ `uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=0` 3m57.924s
 │   ├── ✅ `uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=1` 7.687s
 │   ├── ✅ `uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=2` 24.059s
 │   └── ✅ `uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=3` 7m18.860s
 ├── `uv run dart-pipeline process epidemiological/dengue/peru`
 ├── `uv run dart-pipeline process geospatial/chirps-rainfall`
 │   ├── ✅ `uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023` 0.52s
 │   ├── ✅ `uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05` 0.649s
 │   └── ✅ `uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05 a=1` 0.683s
 ├── ❌ `uv run dart-pipeline process geospatial/worldpop-count`
 ├── ✅ `uv run dart-pipeline process meteorological/aphrodite-daily-mean-temp` 0.691s
 ├── ❌ `uv run dart-pipeline process meteorological/aphrodite-daily-precip`
 ├── ✅ `uv run dart-pipeline process meteorological/chirps-rainfall date=2023` 0.489s
 ├── ❌ `uv run dart-pipeline process meteorological/era5-reanalysis`
 ├── ❌ `uv run dart-pipeline process sociodemographic/worldpop-count iso3=VNM`
 └── ❌ `uv run dart-pipeline process sociodemographic/worldpop-density iso3=VNM year=2023`
