# DART Pipeline

Data analysis pipeline for the Dengue Advanced Readiness Tools (DART) project.

The aim of this project is to develop a scalable and reproducible pipeline for
the joint analysis of epidemiological, climate, and behavioural data to
anticipate and predict dengue outbreaks.

## Setup

We use [uv](https://docs.astral.sh/uv/getting-started/installation/) to setup
and manage Python versions and dependencies. uv can be installed using the
following method:

```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
```

If you do not have curl installed, then use `brew install curl` on macOS or
`sudo apt install curl` on Debian/Ubuntu or `sudo dnf install curl` on
Fedora/RHEL.

If you have an existing virtual environment for DART pipeline, it should be
removed as uv manages the .venv folder itself. Once installed, you can run
dart-pipeline as follows

```shell
git clone https://github.com/kraemer-lab/DART-Pipeline
uv sync
uv run dart-pipeline
```


```{toctree}
---
caption: Contents
maxdepth: 2
---

overview
schema
geospatial
metrics
```
