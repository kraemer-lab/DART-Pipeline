DART-Pipeline Developer Documentation
=====================================
Done: ✅, in progress: ⏳, not working: ❌

Get
---

`uv run dart-pipeline get`
 ├── ✅ `uv run dart-pipeline get economic/relative-wealth-index 3=VNM` 6.309s
 ├── ✅ `uv run dart-pipeline get epidemiological/dengue/peru` 21m31.33s
 ├── `uv run dart-pipeline get geospatial/gadm`
 │    ├── ✅ `uv run dart-pipeline get geospatial/gadm 3=PER` 
 │    └── ✅ `uv run dart-pipeline get geospatial/gadm 3=VNM` 17.964s
 ├── ✅ `uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp` 16m6.556s
 ├── ✅ `uv run dart-pipeline get meteorological/aphrodite-daily-precip` 1m23.727s
 ├── `uv run dart-pipeline get meteorological/chirps-rainfall`
 │    ├── ✅ `uv run dart-pipeline get meteorological/chirps-rainfall d=2023` 19.750s
 │    └── ✅ `uv run dart-pipeline get meteorological/chirps-rainfall d=2023-05` 1m32.80s
 ├── ✅ `uv run dart-pipeline get meteorological/terraclimate year=2023` 9m30.29s
 ├── ✅ `uv run dart-pipeline get sociodemographic/meta-pop-density 3=VNM` 10m25.46s
 ├── ✅ `uv run dart-pipeline get sociodemographic/worldpop-count 3=VNM`
 └── ✅ `uv run dart-pipeline get sociodemographic/worldpop-density 3=VNM`

### Meteorological

#### CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations

```
uv run dart-pipeline get meteorological/chirps-rainfall d=2023
uv run dart-pipeline get meteorological/chirps-rainfall d=2023-05
```

Process
-------

`uv run dart-pipeline process`
 ├── `uv run dart-pipeline process economic/relative-wealth-index`
 │    ├── ✅ `uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=0` 3m57.924s
 │    ├── ✅ `uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=1` 7.687s
 │    ├── ✅ `uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=2` 24.059s
 │    └── ✅ `uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=3` 7m18.860s
 ├── `uv run dart-pipeline process epidemiological/dengue/peru`
 ├── `uv run dart-pipeline process geospatial/chirps-rainfall`
 │    ├── ✅ `uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023 a=0 l=INFO plots` 1.503s
 │    ├── ✅ `uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05 a=0 l=INFO plots` 1.500s
 │    └── ✅ `uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05-11 a=0 l=INFO plots` 1.474s
 ├── `uv run dart-pipeline process geospatial/worldpop-count`
 │    ├── ✅ `uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=0 d=2023 l=INFO plots`
 │    ├── ✅ `uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=1 d=2023 l=INFO plots`
 │    ├── ✅ `uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=2 d=2023 l=INFO plots`
 │    └── ✅ `uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=3 d=2023 l=INFO plots`
 ├── ✅ `uv run dart-pipeline process meteorological/aphrodite-daily-mean-temp` 0.691s
 ├── ❌ `uv run dart-pipeline process meteorological/aphrodite-daily-precip`
 ├── `uv run dart-pipeline process meteorological/chirps-rainfall`
 │    ├── ✅ `uv run dart-pipeline process meteorological/chirps-rainfall d=2023 l=INFO` 0.825s
 │    ├── ✅ `uv run dart-pipeline process meteorological/chirps-rainfall d=2023 l=INFO plots` 1.367s
 │    ├── ✅ `uv run dart-pipeline process meteorological/chirps-rainfall d=2023-05 l=INFO`
 │    └── ✅ `uv run dart-pipeline process meteorological/chirps-rainfall d=2023-05-11 l=INFO`
 ├── ❌ `uv run dart-pipeline process meteorological/era5-reanalysis`
 ├── `uv run dart-pipeline process meteorological/terraclimate`
 │    ├── ✅ `uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=PER a=0 l=INFO` 1m46.532s
 │    ├── ✅ `uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=PER a=1 l=INFO` 2m52.306s
 │    ├── ✅ `uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=VNM a=0 l=INFO plots` 2m22.568s
 │    └── ✅ `uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=VNM a=1 l=INFO plots` 4m58.389s
 ├── ❌ `uv run dart-pipeline process sociodemographic/worldpop-count iso3=VNM`
 └── ❌ `uv run dart-pipeline process sociodemographic/worldpop-density iso3=VNM year=2023`

### Economic

#### Relative Wealth Index

```
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=0 l=INFO plots
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=1 l=INFO plots
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=2 l=INFO plots
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=3 l=INFO plots
```

### Geospatial

```
uv run dart-pipeline process geospatial/chirps-rainfall
uv run dart-pipeline process geospatial/gadm
uv run dart-pipeline process geospatial/worldpop-count
```

#### CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations

```
uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023 a=0 l=INFO plots
uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05 a=0 l=INFO plots
uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05-11 a=0 l=INFO plots
```

#### Global Administrative Areas (GADM)

```
uv run dart-pipeline process geospatial/gadm 3=VNM a=0
```

#### WorldPop Population Counts

```
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=0 d=2023 l=INFO plots
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=1 d=2023 l=INFO plots
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=2 d=2023 l=INFO plots
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=3 d=2023 l=INFO plots
```

Functions
---------
Index of functions that are working and tested:

`__init__.py`

```
def list_all() -> list[str]:
def get(
def process_cli(source: str, **kwargs):
def check(source: str, only_one: bool = True, **kwargs):
def parse_params(params: list[str]) -> dict[str, str | int]:
def main():
```

`collate.py`

```
def gadm_data(iso3: str) -> URLCollection:
def relative_wealth_index(iso3: str) -> URLCollection:
def ministerio_de_salud_peru_data() -> list[DataFile]:
def aphrodite_precipitation_data() -> list[URLCollection]:
def aphrodite_temperature_data() -> list[URLCollection]:
✅ def chirps_rainfall_data(partial_date: str) -> list[URLCollection]:
def terraclimate_data(year: int) -> URLCollection:
def meta_pop_density_data(iso3: str) -> URLCollection:
def worldpop_pop_count_data(iso3: str) -> URLCollection:
def worldpop_pop_density_data(iso3: str) -> URLCollection:
```

`collate_api.py`

```
def download_era5_reanalysis_data(path: Path):
```

`process.py`

```
def process_ministerio_de_salud_peru_data(
def get_shapefile(iso3: str, admin_level: Literal["0", "1", "2"]) -> Path:
def process_gadm_admin_map_data(iso3: str, admin_level: AdminLevel) -> ProcessResult:
def process_aphrodite_precipitation_data() -> list[ProcessResult]:
def process_aphrodite_temperature_data() -> list[ProcessResult]:
def get_chirps_rainfall_data_path(date: PartialDate) -> Path:
def process_chirps_rainfall_data(date: str) -> ProcessResult:
def process_era5_reanalysis_data() -> ProcessResult:
def process_terraclimate(
def process_worldpop_pop_count_data(
def process_worldpop_pop_density_data(iso3: str, year: int) -> ProcessResult:
def process_gadm_chirps_data(
def process_gadm_worldpoppopulation_data(
def get_admin_region(lat: float, lon: float, polygons) -> str:
def process_relative_wealth_index_admin(iso3: str, admin_level: str):
def get_admin(x):
```

`types.py`

```
def __call__(self, source: str, path: str | None | Path = None) -> Path:
def from_string(date: str) -> "PartialDate":
def to_string(self, sep="-") -> str:
def zero_padded_month(self) -> str:
def zero_padded_day(self) -> str:
def scope(self) -> Literal["annual", "monthly", "daily"]:
def show(self, show_links: bool = False) -> str:
def disk_files(self, root: str | Path) -> list[Path]:
def missing_files(self, root: str | Path) -> list[Path]:
```

`util.py`

```
def abort(bold_text: str, rest: str):
def days_in_year(year: int) -> Literal[365, 366]:
def get_country_name(iso3: str) -> str:
def only_one_from_collection(coll: URLCollection) -> URLCollection:
def use_range(value: int | float, min: int | float, max: int | float, message: str):
def daterange(
def get_credentials(source: str, credentials: str | Path | None = None) -> Credentials:
def credentials_from_string(s: str, source: str) -> tuple[str, str]:
def download_file(
def download_files(
def default_path_getter(env_var: str, default: str) -> DefaultPathProtocol:
def default_path(source: str, path: str | Path | None = None) -> Path:
def walk(
def bold_brackets(s: str) -> str:
def unpack_file(path: Path | str, same_folder: bool = False):
def update_or_create_output(
```
