from pathlib import Path

import numpy as np
import pytest

from dart_pipeline.metrics.era5.collate import MetricCollection


@pytest.fixture(scope="module")
def metric_collection():
    return MetricCollection("HCM-2", data_path=Path("tests/data"), weekly=False)


def test_metric_collection(metric_collection):
    assert metric_collection.min_year == 2020
    assert metric_collection.max_year == 2021
    assert sorted(metric_collection.data.metric.unique()) == [
        "era5.core_daily",
    ]


@pytest.mark.parametrize(
    "name,metric,cell_methods",
    [
        ("tp", "era5.core_daily", "time: sum (interval: 1 day)"),
        (
            "mx2t24",
            "era5.core_daily",
            "time: max (interval: 1 day)",
        ),
        (
            "t2m",
            "era5.core_daily",
            "time: mean (interval: 1 day)",
        ),
    ],
)
def test_collate_metric(name, metric, cell_methods, metric_collection):
    da = metric_collection.collate_metric(metric)
    assert name in da.data_vars
    assert da.time.min() == np.datetime64("2020-01-01")
    assert da.time.max() == np.datetime64("2021-12-31")
    assert da[name].attrs["cell_methods"] == cell_methods


def test_collate_metric_errors(metric_collection):
    with pytest.raises(
        ValueError,
        match="No data found for metric='era5.2m_temperature.daily_median' from 2020-2021 in HCM-2",
    ):
        metric_collection.collate_metric("era5.2m_temperature.daily_median")
    with pytest.raises(
        ValueError,
        match="Contiguous years not present for metric='era5.core_daily' from 2019-2020 in HCM-2",
    ):
        metric_collection.collate_metric("era5.core_daily", (2019, 2020))


def test_collate(metric_collection):
    ds = metric_collection.collate()
    # fmt: off
    assert set(ds.data_vars).issubset({
        "mxr24", "t2m", "r", "mnr24", "q", "ssrd", "mx2t24", "mn2t24", "tp", "hb", "mxq24", "mnq24", "e"
    })
    # fmt: on
