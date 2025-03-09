import tomllib
from pathlib import Path
from functools import cache
from typing import TypedDict, Literal, Any

METRICS_PATH = Path(__file__).parent / "metrics.toml"


class MetricInfo(TypedDict, total=False):
    description: str
    depends: list[str]
    units: str
    statistics: list[str]
    resampling: Literal["remapdis", "remapbil"]


def get_resampling(metric: MetricInfo) -> Literal["remapdis", "remapbil"]:
    if res := metric.get("resampling"):
        return res
    statistics = metric.get("statistics", ["daily_mean"])
    if isinstance(statistics, str):
        statistics = [statistics]
    return "remapdis" if "daily_sum" in statistics else "remapbil"


def get_sources() -> dict[str, dict[str, Any]]:
    with open(METRICS_PATH, "rb") as fp:
        metrics = tomllib.load(fp)
        return metrics["sources"]


@cache
def get_metrics(key: str | None = None) -> dict[str, MetricInfo]:
    out = {}
    with open(METRICS_PATH, "rb") as fp:
        metrics = tomllib.load(fp)
        sources = list(metrics["sources"].keys())
        for source in sources:
            for metric in metrics.get(source, {}):
                out[f"{source}.{metric}"] = metrics[source][metric]
                out[f"{source}.{metric}"]["resampling"] = get_resampling(
                    metrics[source][metric]
                )
    return out if key is None else {k: v for k, v in out.items() if k.startswith(key)}
