import numpy as np
import numpy.testing as npt
from dart_pipeline.metrics.seasonal_forecast_monthly.util import collapse_step_to_month


def test_collapse_step_to_month(sample_monthly_forecast):
    dense = collapse_step_to_month(sample_monthly_forecast)
    assert set(dense.coords) == {"time", "latitude", "longitude", "month"}
    dense = dense.transpose("latitude", "longitude", "time", "month")
    expected = np.array(
        [
            [
                [
                    [0.5, 1.0, 1.5, 2.0, 2.5, 3.0],
                    [3.5, 4.0, 4.5, 5.0, 5.5, 6.0],
                    [6.5, 7.0, 7.5, 8.0, 8.5, 9.0],
                    [9.5, 10.0, 10.5, 11.0, 11.5, 12.0],
                ],
                [
                    [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                    [7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
                    [13.0, 14.0, 15.0, 16.0, 17.0, 18.0],
                    [19.0, 20.0, 21.0, 22.0, 23.0, 24.0],
                ],
            ],
            [
                [
                    [1.5, 3.0, 4.5, 6.0, 7.5, 9.0],
                    [10.5, 12.0, 13.5, 15.0, 16.5, 18.0],
                    [19.5, 21.0, 22.5, 24.0, 25.5, 27.0],
                    [28.5, 30.0, 31.5, 33.0, 34.5, 36.0],
                ],
                [
                    [3.0, 6.0, 9.0, 12.0, 15.0, 18.0],
                    [21.0, 24.0, 27.0, 30.0, 33.0, 36.0],
                    [39.0, 42.0, 45.0, 48.0, 51.0, 54.0],
                    [57.0, 60.0, 63.0, 66.0, 69.0, 72.0],
                ],
            ],
        ]
    )
    npt.assert_array_equal(dense.values, expected)
