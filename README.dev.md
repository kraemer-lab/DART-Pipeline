DART-Pipeline Developer Documentation
=====================================

Development Status
------------------
Done: ✅, in progress: ⏳, not working: ❌

`uv run dart-pipeline get`
 ├── ✅ `uv run dart-pipeline get economic/relative-wealth-index iso3=VNM` 6s309ms
 ├── ✅ `uv run dart-pipeline get epidemiological/dengue/peru`42m37s846ms
 ├── `uv run dart-pipeline get geospatial/gadm`
 │    ├── ✅ `uv run dart-pipeline get geospatial/gadm iso3=PER` 46s252ms
 │    └── ✅ `uv run dart-pipeline get geospatial/gadm iso3=VNM` 17s964ms
 ├── ✅ `uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp` 12m56s120ms
 ├── ✅ `uv run dart-pipeline get meteorological/aphrodite-daily-precip`  44s273ms
 ├── `uv run dart-pipeline get meteorological/chirps-rainfall`
 │    ├── ✅ `uv run dart-pipeline get meteorological/chirps-rainfall year=2023` 14s731ms
 │    └── ✅ `uv run dart-pipeline get meteorological/chirps-rainfall year=2023 month=5` 2m3s728ms
 ├── ✅ `uv run dart-pipeline get meteorological/terraclimate year=2023` 9m30s290s
 ├── ✅ `uv run dart-pipeline get sociodemographic/meta-pop-density iso3=VNM` 7m59s810ms
 ├── `uv run dart-pipeline get sociodemographic/worldpop-count`
 │    ├── ✅ `uv run dart-pipeline get sociodemographic/worldpop-count iso3=PER` 22m6s650ms
 │    └── ✅ `uv run dart-pipeline get sociodemographic/worldpop-count iso3=VNM` 7m13s280ms
 └── `uv run dart-pipeline get sociodemographic/worldpop-density`
      ├── ✅ `uv run dart-pipeline get sociodemographic/worldpop-density iso3=PER` 6s622ms
      └── ✅ `uv run dart-pipeline get sociodemographic/worldpop-density iso3=VNM` 1s840ms

`uv run dart-pipeline process`
 ├── ✅ `uv run dart-pipeline process economic/relative-wealth-index iso3=VNM` 9s304ms
 ├── ✅ `uv run dart-pipeline process epidemiological/dengue/peru` 694ms
 ├── `uv run dart-pipeline process geospatial/chirps-rainfall`
 │    ├── ✅ `uv run dart-pipeline process geospatial/chirps-rainfall iso3=VNM partial_date=2023` 1s46ms
 │    └── ❌ `uv run dart-pipeline process geospatial/chirps-rainfall iso3=VNM partial_date=2023-05`
 ├── ✅ `uv run dart-pipeline process geospatial/gadm iso3=VNM admin_level=0` 531ms
 ├── ❌ `uv run dart-pipeline process geospatial/worldpop-count`
 ├── ✅ `uv run dart-pipeline process meteorological/aphrodite-daily-mean-temp` 691ms
 ├── ❌ `uv run dart-pipeline process meteorological/aphrodite-daily-precip`
 ├── ✅ `uv run dart-pipeline process meteorological/chirps-rainfall date=2023` 489ms
 ├── ❌ `uv run dart-pipeline process meteorological/era5-reanalysis`
 ├── ❌ `uv run dart-pipeline process sociodemographic/worldpop-count iso3=VNM`
 └── ❌ `uv run dart-pipeline process sociodemographic/worldpop-density iso3=VNM year=2023`
