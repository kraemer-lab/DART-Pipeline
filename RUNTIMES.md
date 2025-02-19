RUNTIMES
========

Economic
--------

### Relative Wealth Index

```
time uv run dart-pipeline get economic/relative-wealth-index 3=VNM
6.309s

time uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=0
3m57.924s
time uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=1
7.687s
time uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=2
24.059s
time uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=3
7m18.860s
time uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=0 l=INFO plots
52.822s
time uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=1 l=INFO plots
3.640s
time uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=2 l=INFO plots
6.672s
time uv run dart-pipeline process economic/relative-wealth-index 3=VNM a=3 l=INFO plots
1m18.97s
```

Epidemiological
---------------

### Dengue - Peru

```
time uv run dart-pipeline get epidemiological/dengue/peru
21m31.33s
time uv run dart-pipeline process epidemiological/dengue/peru l=INFO
0.897s
time uv run dart-pipeline process epidemiological/dengue/peru l=INFO plots
1.282s
time uv run dart-pipeline process epidemiological/dengue/peru a=1 l=INFO
1.149s
time uv run dart-pipeline process epidemiological/dengue/peru a=1 l=INFO plots
3.109s
```

Geospatial
----------

### APHRODITE Daily Mean Temperature

```
time uv run dart-pipeline process geospatial/aphrodite-daily-mean-temp 3=VNM a=0 d=2015-05-11 l=INFO plots
5.835s
time uv run dart-pipeline process geospatial/aphrodite-daily-mean-temp 3=VNM a=1 d=2015-05-11 l=INFO plots
3.446s
time uv run dart-pipeline process geospatial/aphrodite-daily-mean-temp 3=VNM a=3 d=2015-05-11 l=INFO plots
5m35.019s
```

### APHRODITE Precipitation

```
time uv run dart-pipeline get meteorological/aphrodite-daily-precip -u
2.136s
time uv run dart-pipeline get geospatial/gadm 3=VNM
0.734s
time uv run dart-pipeline process geospatial/aphrodite-daily-precip 3=VNM a=0 d=2015-05-11 l=INFO plots
4.934s
time uv run dart-pipeline process geospatial/aphrodite-daily-precip 3=VNM a=1 d=2015-05-11 l=INFO plots
3.806s
time uv run dart-pipeline process geospatial/aphrodite-daily-precip 3=VNM a=3 d=2015-05-11 l=INFO plots
6m52.38s
```

### CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations

```
time uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023 a=0 l=INFO plots
1.503s
time uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05 a=0 l=INFO plots
1.500s
time uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05-11 a=0 l=INFO plots
1.474s
```

### ERA5 atmospheric reanalysis

```
time uv run dart-pipeline process geospatial/era5-reanalysis dataset=derived-era5-land-daily-statistics 3=VNM a=0 d=2024-10-01 l=INFO plots
10.791s
time uv run dart-pipeline process geospatial/era5-reanalysis dataset=derived-era5-land-daily-statistics 3=VNM a=1 d=2024-10-01 l=INFO plots
47.208s
```

### Global Administrative Areas (GADM)

```
time uv run dart-pipeline get geospatial/gadm 3=VNM
17.964s
time uv run dart-pipeline process geospatial/gadm 3=VNM a=0
```

### WorldPop Population Counts

```
time uv run dart-pipeline get sociodemographic/worldpop-count 3=VNM
8m4.44s
time uv run dart-pipeline get geospatial/gadm 3=VNM unpack
14.945s
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=0 d=2020
4.646s
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=1 d=2020
3.231s
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=2 d=2020
3.585s
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=3 d=2020
14.168s
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=0 d=2020 l=INFO plots
7.069s
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=1 d=2020 l=INFO plots
11.448s
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=2 d=2020 l=INFO plots
12.913s
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=3 d=2020 l=INFO plots
54.425s
```

### WorldPop Population Density

```
time uv run dart-pipeline get sociodemographic/worldpop-density 3=PER
time uv run dart-pipeline get geospatial/gadm 3=VNM unpack
time uv run dart-pipeline process geospatial/worldpop-density 3=VNM a=0 d=2020 l=INFO plots
1.513s
```

Meteorological
--------------

### APHRODITE Daily Mean Temperature

```
time uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp
16m6.556s
time uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp unpack

time uv run dart-pipeline process meteorological/aphrodite-daily-mean-temp
0.691s
time uv run dart-pipeline process meteorological/aphrodite-daily-mean-temp l=INFO
1.127s
time uv run dart-pipeline process meteorological/aphrodite-daily-mean-temp l=INFO plots
29.441s
```

### APHRODITE Precipitation

```
time uv run dart-pipeline get meteorological/aphrodite-daily-precip
1m23.727s
time uv run dart-pipeline get meteorological/aphrodite-daily-precip -u

time uv run dart-pipeline process meteorological/aphrodite-daily-precip
1.228s
time uv run dart-pipeline process meteorological/aphrodite-daily-precip plots
55.961s
```

### CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations

```
time uv run dart-pipeline get meteorological/chirps-rainfall d=2023
19.750s
time uv run dart-pipeline get meteorological/chirps-rainfall d=2023-05
1m32.80s

time uv run dart-pipeline process meteorological/chirps-rainfall d=2023 l=INFO
0.825s
time uv run dart-pipeline process meteorological/chirps-rainfall d=2023 l=INFO plots
1.367s
time uv run dart-pipeline process meteorological/chirps-rainfall d=2023-05 l=INFO
time uv run dart-pipeline process meteorological/chirps-rainfall d=2023-05-11 l=INFO
```

### TerraClimate

```
time uv run dart-pipeline get meteorological/terraclimate year=2023
9m30.29s
time uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=PER a=0 l=INFO
1m46.532s
time uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=PER a=1 l=INFO
2m52.306s
time uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=VNM a=0 l=INFO plots
2m22.568s
time uv run dart-pipeline process meteorological/terraclimate d=2023-01 3=VNM a=1 l=INFO plots
4m58.389s
```

### ERA5 atmospheric reanalysis

```
time uv run dart-pipeline get meteorological/era5-reanalysis d=2023 dataset=satellite-sea-ice-thickness
12.210s
time uv run dart-pipeline get meteorological/era5-reanalysis d=2024-10-01 dataset=derived-era5-land-daily-statistics
25.231s
time uv run dart-pipeline get meteorological/era5-reanalysis d=2024-10-01 dataset=reanalysis-era5-complete
13.051s
time uv run dart-pipeline get meteorological/era5-reanalysis d=2024-10-01 dataset=reanalysis-era5-single-levels
3m53.526s

time uv run dart-pipeline process meteorological/era5-reanalysis dataset=derived-era5-land-daily-statistics d=2024-10-01 l=INFO plots
16.422s
```

Population-Weighted
-------------------

### Relative Wealth Index

```
time uv run dart-pipeline get economic/relative-wealth-index 3=VNM
time uv run dart-pipeline get geospatial/gadm 3=VNM
time uv run dart-pipeline get sociodemographic/meta-pop-density 3=VNM --unpack
11m43.312s

time uv run dart-pipeline process population-weighted/relative-wealth-index 3=VNM d=2020 l=INFO plots
```

Socio-Demographic
-----------------

### Meta Population Density

```
time uv run dart-pipeline get sociodemographic/meta-pop-density 3=VNM
10m25.46s
```

### WorldPop Population Count

```
time uv run dart-pipeline get sociodemographic/worldpop-count 3=VNM
8m4.44s
time uv run dart-pipeline process sociodemographic/worldpop-count 3=VNM l=INFO
4.469s
```

### WorldPop Population Density

```
time uv run dart-pipeline get sociodemographic/worldpop-density 3=VNM
time uv run dart-pipeline get sociodemographic/worldpop-density 3=PER

time uv run dart-pipeline process sociodemographic/worldpop-density 3=VNM d=2020 l=INFO plots
0.933s
time uv run dart-pipeline process sociodemographic/worldpop-density 3=PER d=2020 l=INFO plots
0.900s
```
