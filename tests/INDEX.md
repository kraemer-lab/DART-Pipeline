INDEX
=====

`pytest tests/test_collate.py`

```
✅ def test_gadm_data():
✅ def test_relative_wealth_index():
✅ def test_ministerio_de_salud_peru_data():
✅ def test_aphrodite_precipitation_data():
✅ def test_chirps_rainfall_data():
✅ def test_meta_pop_density_data():
✅ def test_worldpop_pop_count_data():
✅ def test_worldpop_pop_density_data():
```

`pytest tests/test_plots.py`

```
✅ def test_plot_heatmap(
✅ def test_plot_gadm_micro_heatmap(
✅ def test_plot_gadm_macro_heatmap(
✅ def test_plot_timeseries(mock_mkdir, mock_savefig, mock_close):
✅ def test_plot_scatter(mock_mkdir, mock_savefig):
```

`pytest tests/test_process.py`

```
✅ def test_process_rwi(
✅ def test_process_dengueperu(
✅ def test_process_gadm_aphroditeprecipitation(
✅ def test_process_gadm_chirps_rainfall(
✅ def test_process_gadm_worldpopcount(
✅ def test_process_aphrodite_precipitation_data(
✅ def test_process_chirps_rainfall(
✅ def test_process_terraclimate(
```

`pytest tests/test_util.py`

```
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
