INDEX
=====

```
geospatial/aphroditeprecipitation.py
✅ def process_gadm_aphroditeprecipitation(
geospatial/aphroditetemperature.py
✅ def process_gadm_aphroditetemperature(
geospatial/era5reanalysis.py
✅ def process_gadm_era5reanalysis(
geospatial/worldpop_count.py
✅ def process_gadm_worldpopcount(
geospatial/worldpop_density.py
✅ def process_gadm_worldpopdensity(
```

```
meteorological/aphroditeprecipitation.py
✅ def process_aphroditeprecipitation(
meteorological/aphroditetemperature.py
✅ def process_aphroditetemperature(year=None, plots=False) -> \
meteorological/era5reanalysis.py
✅ def process_era5reanalysis(dataset, partial_date, plots=False):
```

```
population-weighted/relative-wealth-index
✅ def get_geo_id(lat: float, lon: float, polygons: dict) -> str:
✅ def get_quadkey(x, zoom_level):
✅ def process_gadm_popdensity_rwi(
```

```
sociodemographic/worldpop_count.py
✅ def process_gadm_worldpopcount(
sociodemographic/worldpop_density.py
✅ def process_worldpopdensity(
```

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
✅ def aphrodite_temperature_data(unpack) -> list[URLCollection]:
✅ def aphrodite_precipitation_data() -> list[URLCollection]:
✅ def chirps_rainfall_data(partial_date: str) -> list[URLCollection]:
def terraclimate_data(year: int) -> URLCollection:
def meta_pop_density_data(iso3: str) -> URLCollection:
def worldpop_pop_count_data(iso3: str) -> URLCollection:
✅ def worldpop_pop_density_data(iso3: str) -> URLCollection:
```

`collate_api.py`

```
✅ def download_era5_reanalysis_data(dataset: str, partial_date: str):
```

`plots.py`

```
✅ def plot_heatmap(data, title, colourbar_label, path):
✅ def plot_gadm_micro_heatmap(
✅ def plot_gadm_macro_heatmap(
✅ def plot_timeseries(df, title, path):
✅ def plot_scatter(x, y, z, title, colourbar_label, path):
✅ def plot_gadm_scatter(lon, lat, data, title, colourbar_label, path, gdf):
```

`process.py`

```
def process_rwi(iso3: str, admin_level: str, plots=False):
✅ def process_dengueperu(
✅ def process_gadm_chirps_rainfall(
def process_gadm_admin_map_data(iso3: str, admin_level: AdminLevel):
def get_chirps_rainfall_data_path(date: PartialDate) -> Path:
def process_chirps_rainfall(partial_date: str, plots=False) -> ProcessResult:
def process_terraclimate(
def get_admin_region(lat: float, lon: float, polygons) -> str:
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
