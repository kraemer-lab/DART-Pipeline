INDEX
=====

```
pytest tests/test_collate.py
✅ def test_gadm_data():
✅ def test_relative_wealth_index():
✅ def test_ministerio_de_salud_peru_data():
✅ def test_aphrodite_temperature_data():
✅ def test_aphrodite_precipitation_data():
✅ def test_chirps_rainfall_data():
✅ def test_meta_pop_density_data():
✅ def test_worldpop_pop_count_data():
✅ def test_worldpop_pop_density_data():
```

```
pytest tests/test_collate_api.py
✅ def test_download_era5_reanalysis_data(mock_cds_client, mock_source_path):
```

```
pytest tests/test_economic_relative_wealth-index.py
✅ def test_process_rwi(mock_savefig, mock_get_country, mock_source_path):
```

```
pytest tests/test_geospatial_aphroditeprecipitation.py
✅ def test_process_gadm_aphroditeprecipitation():
pytest tests/test_geospatial_aphroditetemperature.py
✅ def test_process_gadm_aphroditetemperature():
pytest tests/test_geospatial_era5reanalysis.py
✅ def test_process_gadm_era5reanalysis(
pytest tests/test_geospatial_relative_wealth-index.py
✅ def test_get_admin_region():
✅ def test_process_gadm_rwi(mock_plot, mock_get_country, mock_source_path, mock_get_shapefile):
pytest tests/test_geospatial_worldpop_count.py
✅ def test_process_gadm_worldpopcount(
pytest tests/test_geospatial_worldpop_density.py
✅ def test_process_gadm_worldpopdensity(
```

```
pytest tests/test_meteorological_aphroditeprecipitation.py
✅ def test_process_aphrodite_precipitation_data():
pytest tests/test_meteorological_aphroditetemperature.py
✅ def test_process_aphroditetemperature():
pytest tests/test_meteorological_era5reanalysis.py
✅ def test_process_era5reanalysis(
```

```
pytest tests/test_plots.py
✅ def test_plot_heatmap(
✅ def test_plot_gadm_micro_heatmap(
✅ def test_plot_gadm_macro_heatmap(
✅ def test_plot_timeseries(mock_mkdir, mock_savefig, mock_close):
✅ def test_plot_scatter(mock_mkdir, mock_savefig):
✅ def test_plot_gadm_scatter(mock_mkdir, mock_savefig, mock_close):
```

```
pytest tests/test_population_weighted_relative_wealth-index.py
✅ def test_process_gadm_popdensity_rwi(
```

```
pytest tests/test_process.py
✅ def test_process_rwi(
✅ def test_process_dengueperu(
✅ def test_process_gadm_chirps_rainfall(
✅ def test_process_chirps_rainfall(
✅ def test_process_terraclimate(
```

```
pytest tests/test_sociodemographic_worldpop_count.py
✅ def test_process_worldpopcountdata(
pytest tests/test_sociodemographic_worldpop_density.py
✅ def test_process_worldpopdensity(
```

```
pytest tests/test_util.py
✅ def test_download_file():
✅ def test_download_file_unzip():
✅ def test_download_file_without_unzip():
✅ def test_download_file_unzip_create_folder():
✅ def test_days_in_year(year, days):
✅ def test_get_country_name(iso3, name):
✅ def test_use_range():
✅ def test_update_or_create_output_create_new(
✅ def test_update_or_create_output_update_existing(
✅ def test_update_or_create_output_invalid_input():
```
