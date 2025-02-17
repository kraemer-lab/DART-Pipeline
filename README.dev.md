DART-Pipeline Developer Documentation
=====================================
Done: ✅, in progress: ⏳, not working: ❌

Economic
--------

### Relative Wealth Index

```
uv run dart-pipeline get economic/relative-wealth-index 3=VNM
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=0
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=1
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=2
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=3
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=0 l=INFO plots
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=1 l=INFO plots
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=2 l=INFO plots
uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=3 l=INFO plots
```

Epidemiological
---------------

### Dengue - Peru

```
uv run dart-pipeline get epidemiological/dengue/peru
uv run dart-pipeline process epidemiological/dengue/peru l=INFO
uv run dart-pipeline process epidemiological/dengue/peru l=INFO plots
uv run dart-pipeline process epidemiological/dengue/peru a=1 l=INFO
uv run dart-pipeline process epidemiological/dengue/peru a=1 l=INFO plots
```

Geospatial
----------

### APHRODITE Daily Mean Temperature

```
uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp l=INFO unpack
uv run dart-pipeline get geospatial/gadm 3=VNM
uv run dart-pipeline process geospatial/aphrodite-daily-mean-temp 3=VNM a=0 d=2015-05-11 l=INFO
uv run dart-pipeline process geospatial/aphrodite-daily-mean-temp 3=VNM a=0 d=2015-05-11 l=INFO plots
uv run dart-pipeline process geospatial/aphrodite-daily-mean-temp 3=VNM a=1 d=2015-05-11 l=INFO plots
uv run dart-pipeline process geospatial/aphrodite-daily-mean-temp 3=VNM a=3 d=2015-05-11 l=INFO plots
```

### APHRODITE Precipitation

```
uv run dart-pipeline get meteorological/aphrodite-daily-precip -u
uv run dart-pipeline get geospatial/gadm 3=VNM
uv run dart-pipeline process geospatial/aphrodite-daily-precip 3=VNM a=0 d=2015-05-11 l=INFO
uv run dart-pipeline process geospatial/aphrodite-daily-precip 3=VNM a=0 d=2015-05-11 l=INFO plots
uv run dart-pipeline process geospatial/aphrodite-daily-precip 3=VNM a=1 d=2015-05-11 l=INFO plots
uv run dart-pipeline process geospatial/aphrodite-daily-precip 3=VNM a=3 d=2015-05-11 l=INFO plots
```

### CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations

```
uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023 a=0 l=INFO plots
uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05 a=0 l=INFO plots
uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05-11 a=0 l=INFO plots
```

### ERA5 atmospheric reanalysis

```
uv run dart-pipeline process geospatial/era5-reanalysis dataset=derived-era5-land-daily-statistics 3=VNM a=0 d=2024-10-01 l=INFO plots
uv run dart-pipeline process geospatial/era5-reanalysis dataset=derived-era5-land-daily-statistics 3=VNM a=1 d=2024-10-01 l=INFO plots
```

### Global Administrative Areas (GADM)

```
uv run dart-pipeline get geospatial/gadm 3=PER
uv run dart-pipeline get geospatial/gadm 3=VNM
uv run dart-pipeline process geospatial/gadm 3=VNM a=0
```

### WorldPop Population Counts

```
uv run dart-pipeline get sociodemographic/worldpop-count 3=VNM
uv run dart-pipeline get geospatial/gadm 3=VNM unpack
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=0 d=2020
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=1 d=2020
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=2 d=2020
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=3 d=2020
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=0 d=2020 l=INFO plots
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=1 d=2020 l=INFO plots
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=2 d=2020 l=INFO plots
uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=3 d=2020 l=INFO plots
```

### WorldPop Population Density

```
uv run dart-pipeline get sociodemographic/worldpop-density 3=PER
uv run dart-pipeline get geospatial/gadm 3=VNM
uv run dart-pipeline process geospatial/worldpop-density 3=VNM a=0 d=2020 l=INFO plots
```

Meteorological
--------------

### APHRODITE Daily Mean Temperature

```
uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp unpack
uv run dart-pipeline process meteorological/aphrodite-daily-mean-temp l=INFO
uv run dart-pipeline process meteorological/aphrodite-daily-mean-temp l=INFO plots
```

### APHRODITE Precipitation

```
uv run dart-pipeline get meteorological/aphrodite-daily-precip -u
uv run dart-pipeline process meteorological/aphrodite-daily-precip
uv run dart-pipeline process meteorological/aphrodite-daily-precip plots
```

### CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations

```
uv run dart-pipeline get meteorological/chirps-rainfall d=2023
uv run dart-pipeline get meteorological/chirps-rainfall d=2023-05
uv run dart-pipeline process meteorological/chirps-rainfall d=2023 l=INFO
uv run dart-pipeline process meteorological/chirps-rainfall d=2023 l=INFO plots
uv run dart-pipeline process meteorological/chirps-rainfall d=2023-05 l=INFO
uv run dart-pipeline process meteorological/chirps-rainfall d=2023-05-11 l=INFO
```

### ERA5 atmospheric reanalysis

```
uv run dart-pipeline get meteorological/era5-reanalysis dataset=derived-era5-land-daily-statistics d=2024-10-01 l=INFO
uv run dart-pipeline get meteorological/era5-reanalysis dataset=reanalysis-era5-complete d=2024-10-01 l=INFO
uv run dart-pipeline get meteorological/era5-reanalysis dataset=reanalysis-era5-single-levels d=2024-10-01 l=INFO
uv run dart-pipeline get meteorological/era5-reanalysis dataset=satellite-sea-ice-thickness d=2023 l=INFO

uv run dart-pipeline process meteorological/era5-reanalysis dataset=derived-era5-land-daily-statistics d=2024-10-01 l=INFO plots
```

### TerraClimate

```
uv run dart-pipeline get meteorological/terraclimate year=2023
uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=PER a=0 l=INFO
uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=PER a=1 l=INFO
uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=VNM a=0 l=INFO plots
uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=VNM a=1 l=INFO plots
```

### ERA5 atmospheric reanalysis

```
uv run dart-pipeline get meteorological/era5-reanalysis d=2023 dataset=satellite-sea-ice-thickness l=INFO
uv run dart-pipeline get meteorological/era5-reanalysis d=2024-10-01 dataset=derived-era5-land-daily-statistics l=INFO
uv run dart-pipeline get meteorological/era5-reanalysis d=2024-10-01 dataset=reanalysis-era5-complete l=INFO
uv run dart-pipeline get meteorological/era5-reanalysis d=2024-10-01 dataset=reanalysis-era5-single-levels l=INFO
```

Socio-Demographic
-----------------

### Meta Population Density

```
uv run dart-pipeline get sociodemographic/meta-pop-density 3=VNM
```

### WorldPop Population Count

```
uv run dart-pipeline get sociodemographic/worldpop-count 3=VNM
uv run dart-pipeline process sociodemographic/worldpop-count 3=VNM l=INFO
```

### WorldPop Population Density

```
uv run dart-pipeline get sociodemographic/worldpop-density 3=VNM
uv run dart-pipeline get sociodemographic/worldpop-density 3=PER

uv run dart-pipeline process sociodemographic/worldpop-density 3=VNM d=2020 l=INFO plots
uv run dart-pipeline process sociodemographic/worldpop-density 3=PER d=2020 l=INFO plots
```
