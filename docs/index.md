# DART Pipeline

Data analysis pipeline for the [Dengue Advanced Readiness Tools (DART)](https://dartdengue.org) project.

The aim of this project is to develop a scalable and reproducible pipeline for
the joint analysis of epidemiological, climate, and behavioural data to
anticipate and predict dengue outbreaks. Climate-sensitive infectious diseases
pose an important challenge for human, animal and environmental health and it
has been estimated that over half of known human pathogenic diseases can be
aggravated by climate change. While climatic and weather conditions are
important drivers of transmission of vector-borne diseases, socio-economic,
behavioural, and land-use factors as well as the interactions among them impact
transmission dynamics. Analysis of drivers of climate-sensitive diseases
require rapid integration of interdisciplinary data to be jointly analysed with
epidemiological (including genomic and clinical) data. Current tools for the
integration of multiple data sources are often limited to one data type or rely
on proprietary data and software. DART Pipeline has been developed to address
this gap and simplifies complex download, bias correction and aggregation steps.


```{figure} pipeline.png

Schematic diagram showing the data integration pipeline
```

```{toctree}
---
caption: Introduction
maxdepth: 1
---

overview
setup
```

```{toctree}
---
caption: Workflow
maxdepth: 1
---

workflow/configuration
workflow/fetching_data
workflow/bias_correction
workflow/processing_data
workflow/validation
workflow/relative_wealth_index
workflow/forecast
workflow/using_custom_shapefiles
```

```{toctree}
---
caption: Reference
maxdepth: 1
---

reference/metrics
reference/schema
reference/storage_conventions
reference/custom_metrics
```
