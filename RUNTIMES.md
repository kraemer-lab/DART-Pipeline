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

### APHRODITE Precipitation

```
time uv run dart-pipeline get meteorological/aphrodite-daily-precip -u
time uv run dart-pipeline get geospatial/gadm 3=VNM
time uv run dart-pipeline process geospatial/aphrodite-daily-precip 3=VNM a=0 d=2015-05-11 l=INFO plots
3.97s
time uv run dart-pipeline process geospatial/aphrodite-daily-precip 3=VNM a=1 d=2015-05-11 l=INFO plots
3.19s
time uv run dart-pipeline process geospatial/aphrodite-daily-precip 3=VNM a=3 d=2015-05-11 l=INFO plots
6m48.48s
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

### Global Administrative Areas (GADM)

```
time uv run dart-pipeline get geospatial/gadm 3=VNM
17.964s
time uv run dart-pipeline process geospatial/gadm 3=VNM a=0
```

### WorldPop Population Counts

```
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=0 d=2023 l=INFO plots
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=1 d=2023 l=INFO plots
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=2 d=2023 l=INFO plots
time uv run dart-pipeline process geospatial/worldpop-count 3=VNM a=3 d=2023 l=INFO plots
```

Meteorological
--------------

### APHRODITE Precipitation

```
time uv run dart-pipeline get meteorological/aphrodite-daily-precip
14.39s
time uv run dart-pipeline get meteorological/aphrodite-daily-precip -u
14.87s
time uv run dart-pipeline process meteorological/aphrodite-daily-precip plots
52.55s
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
```

### WorldPop Population Density

```
time uv run dart-pipeline get sociodemographic/worldpop-density 3=VNM
```
