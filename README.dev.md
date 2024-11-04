DART-Pipeline Developer Documentation
=====================================

Development Status
------------------
Done: ✅, in progress: ⏳, not working: ❌

`uv run dart-pipeline get`
 ├── ✅ `uv run dart-pipeline get economic/relative-wealth-index iso3=VNM` 6.309s
 ├── ❌ `uv run dart-pipeline get epidemiological/dengue/peru` 21m31.33s
 ├── `uv run dart-pipeline get geospatial/gadm`
 │   ├── ✅ `uv run dart-pipeline get geospatial/gadm iso3=PER` 
 │   └── ✅ `uv run dart-pipeline get geospatial/gadm iso3=VNM` 17.964s
 ├── ✅ `uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp` 12m56.12s
 ├── ❌ `uv run dart-pipeline get meteorological/aphrodite-daily-precip` 4.551s
 ├── ✅ `uv run dart-pipeline get meteorological/chirps-rainfall year=2023` 14.731s
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
 ├── ❌ `uv run dart-pipeline process geospatial/chirps-rainfall iso3=VNM partial_date=2023`
 ├── ✅ `uv run dart-pipeline process geospatial/gadm iso3=VNM admin_level=0` 0.531s
 ├── ❌ `uv run dart-pipeline process geospatial/worldpop-count`
 ├── ✅ `uv run dart-pipeline process meteorological/aphrodite-daily-mean-temp` 0.691s
 ├── ❌ `uv run dart-pipeline process meteorological/aphrodite-daily-precip`
 ├── ✅ `uv run dart-pipeline process meteorological/chirps-rainfall date=2023` 0.489s
 ├── ❌ `uv run dart-pipeline process meteorological/era5-reanalysis`
 ├── `uv run dart-pipeline process meteorological/terraclimate`
 │   ├── ✅ `uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=PER a=0 l=INFO`
 │   ├── ✅ `uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=PER a=1 l=INFO`
 │   ├── ✅ `uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=VNM a=0 l=INFO plots`
 │   └── ✅ `uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=VNM a=1 l=INFO plots`
 ├── ❌ `uv run dart-pipeline process sociodemographic/worldpop-count iso3=VNM`
 └── ❌ `uv run dart-pipeline process sociodemographic/worldpop-density iso3=VNM year=2023`

Functions
---------

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
def chirps_rainfall_data(year: int, month: int | None = None) -> list[URLCollection]:
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
