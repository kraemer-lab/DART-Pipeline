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

### CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations

```
time uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023 a=0 l=INFO plots
1.503s
time uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05 a=0 l=INFO plots
1.500s
time uv run dart-pipeline process geospatial/chirps-rainfall 3=VNM d=2023-05-11 a=0 l=INFO plots
1.474s
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
5.429s
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=2 d=2020 l=INFO plots
5.842s
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=3 d=2020 l=INFO plots
16.962
```

Meteorological
--------------

### APHRODITE Daily Mean Temperature

```
time uv run dart-pipeline get meteorological/aphrodite-daily-mean-temp
16m6.556s
time uv run dart-pipeline process meteorological/aphrodite-daily-mean-temp
0.691s
```

### APHRODITE Precipitation

```
time uv run dart-pipeline get meteorological/aphrodite-daily-precip
1m23.727s
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
4.287s
```

### WorldPop Population Density

```
time uv run dart-pipeline get sociodemographic/worldpop-density 3=VNM
time uv run dart-pipeline process sociodemographic/worldpop-density iso3=VNM year=2023
```