INDEX
=====

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
✅ def ministerio_de_salud_peru_data() -> list[DataFile]:
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
def process_rwi(iso3: str, admin_level: str, plots=False):
✅ def process_dengueperu(
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