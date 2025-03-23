"""Tests for process functions in process.py."""

from unittest.mock import patch, mock_open

import numpy as np
import pandas as pd

from dart_pipeline.meteorological.aphroditetemperature import (
    process_aphroditetemperature,
)


def test_process_aphroditetemperature():
    # Minimal mocking for `np.fromfile` and file operations
    nx, ny, _ = 360, 280, 365
    # Mock temperature data
    mock_temp = np.full((ny, nx), 25.0, dtype="float32")
    # Mock station count data
    mock_rstn = np.ones((ny, nx), dtype="float32")

    def mock_fromfile(file, dtype, count):
        if dtype == "float32" and count == nx * ny:
            return mock_temp.flatten() if "temp" in file.name else mock_rstn.flatten()
        raise ValueError(
            f"Unexpected call to np.fromfile with {file}, {dtype}, {count}"
        )

    # Mock file opening
    mocked_open = mock_open()
    with patch("builtins.open", mocked_open), patch("numpy.fromfile", mock_fromfile):
        # Call the function
        year = 2023
        output = process_aphroditetemperature(year=year)

        # Assert the output is a DataFrame
        assert isinstance(output, pd.DataFrame)
        assert len(output) > 0  # Ensure some data is processed
        assert "year" in output.columns
        assert "value" in output.columns

        # Check key output values
        assert (output["year"] == year).all()
        assert (output["unit"] == "°C").all()
