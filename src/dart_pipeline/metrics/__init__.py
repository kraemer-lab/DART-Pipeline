from typing import TypedDict, Unpack

METRICS = {}
FETCHERS = {}
PROCESSORS = {}


class MetricInfo(TypedDict, total=False):
    description: str
    depends: list[str]
    units: str
    statistics: list[str]


class SourceInfo(TypedDict, total=False):
    description: str
    license_text: str
    requires_auth: bool
    auth_url: str
    metrics: dict[str, MetricInfo]


def register_metrics(source: str, **kwargs: Unpack[SourceInfo]):
    METRICS[source] = kwargs


def register_fetch(metric: str):
    if metric.split(".")[0] not in METRICS:
        raise ValueError(
            "Metric first part (before .) refers to a metric source that must be registered using register_metrics()"
        )
    if "." in metric:
        source, metric = metric.split(".")[:2]
        if metric in METRICS[source]["metrics"]:
            raise ValueError("Metric must be registered using register_metrics()")

    def decorator(func):
        FETCHERS[metric] = func

    return decorator


def register_process(metric: str):
    if metric.split(".")[0] not in METRICS:
        raise ValueError(
            "Metric first part (before .) refers to a metric source that must be registered using register_metrics()"
        )
    if "." in metric:
        source, metric = metric.split(".")[:2]
        if metric in METRICS[source]["metrics"]:
            raise ValueError("Metric must be registered using register_metrics()")

    def decorator(func):
        PROCESSORS[metric] = func

    return decorator
