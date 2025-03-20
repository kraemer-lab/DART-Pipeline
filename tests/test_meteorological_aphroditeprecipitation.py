"""Test the processing of APHRODITE precipitation (V1901) data."""

from unittest.mock import patch, mock_open

from freezegun import freeze_time
import numpy as np

from dart_pipeline.meteorological.aphroditeprecipitation import (
    process_aphroditeprecipitation,
)


def test_process_aphrodite_precipitation_data():
    # Minimal mocking for `np.fromfile` and file operations
    nx, ny = 360, 280
    nday = 365
    # Mock precipitation data (10 mm daily)
    mock_prcp = np.full((nday, ny, nx), 10.0, dtype="float32")
    # Mock station count data
    mock_rstn = np.ones((nday, ny, nx), dtype="float32")

    def mock_fromfile(file, dtype, count):
        if dtype == "float32" and count == nx * ny:
            # Toggle between prcp and rstn data based on the read order
            if mock_fromfile.toggle:
                data = mock_prcp[mock_fromfile.day]
            else:
                data = mock_rstn[mock_fromfile.day]
                # Increment day only after reading rstn
                mock_fromfile.day += 1
            # Flip the toggle
            mock_fromfile.toggle = not mock_fromfile.toggle
            return data.flatten()
        raise ValueError(
            f"Unexpected call to np.fromfile with {file}, {dtype}, {count}"
        )

    # Initialise day counter
    mock_fromfile.day = 0
    # Initialise toggle
    mock_fromfile.toggle = True

    # Mock file opening
    mocked_open = mock_open()
    with (
        freeze_time("2025-01-01"),
        patch("builtins.open", mocked_open),
        patch("numpy.fromfile", mock_fromfile),
    ):
        # Call the function
        year = 2023
        output, csv_name = process_aphroditeprecipitation(
            year=year, resolution=["025deg"], plots=False
        )

        # Expected CSV file name
        expected_csv_name = "aphrodite-daily-precip.csv"
        assert csv_name == expected_csv_name

        # Expected output structure
        assert not output.empty
        assert "year" in output.columns
        assert "month" in output.columns
        assert "day" in output.columns
        assert "value" in output.columns
        assert "resolution" in output.columns
        assert "metric" in output.columns
        assert "unit" in output.columns
        assert "creation_date" in output.columns

        # Check if precipitation values sum to the expected amount
        expected_total_precipitation = 10 * nx * ny * nday
        assert np.isclose(
            output["value"].sum(), expected_total_precipitation, atol=1e-5
        )
