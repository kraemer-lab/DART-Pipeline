"""Tests for Peru dengue metrics"""

from unittest.mock import patch, MagicMock

import pytest
import pandas as pd
from bs4 import BeautifulSoup

from dart_pipeline.metrics.peru_dengue import (
    process_dengueperu,
    ministerio_de_salud_peru_data,
)

# Smallest single-precision floating-point number
MIN_FLOAT = -3.4028234663852886e38


@pytest.fixture
def mock_requests():
    with patch("requests.get") as mock_get:
        yield mock_get


@pytest.fixture
def mock_bs4():
    with patch("bs4.BeautifulSoup", wraps=BeautifulSoup) as mock_soup:
        yield mock_soup


@pytest.mark.parametrize(
    "mock_html, expect_error, expected_data",
    [
        # Test Case 1: Valid HTML with onclick attributes
        (
            """
            <a onclick="data:image/png;base64,SGVsbG8sIHdvcmxkISc=').then(something'); a.download = 'file1.xlsx'; a.click"></a>
            <button onclick="data:image/png;base64,SGVsbG8sIHdvcmxkISc=').then(something'); a.download = 'file2.xlsx'; a.click"></button>
            """,
            False,
            [
                ("file1.xlsx", b"Hello world"),
                ("file2.xlsx", b"Python Test"),
            ],
        ),
        # Test Case 2: HTML with no onclick attributes
        (
            "<html><body><p>No links here!</p></body></html>",
            True,
            [],
        ),
    ],
)
def test_ministerio_de_salud_peru_data(
    mock_requests, mock_bs4, mock_html, expect_error, expected_data
):
    # Mock PERU_REGIONS
    regions = ["Amazonas", "Lima"]
    with patch("dart_pipeline.metrics.peru_dengue.PERU_REGIONS", regions):
        # Mock response for requests.get
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = mock_html.encode("utf-8")
        mock_requests.return_value = mock_response

        # Mock BeautifulSoup parsing
        def soup_side_effect(content, parser):
            return BeautifulSoup(content, "html.parser")

        mock_bs4.side_effect = soup_side_effect

        if expect_error:
            # Expect an error if no links are present
            with pytest.raises(ValueError, match="No links found on the page"):
                ministerio_de_salud_peru_data()
        else:
            # Call the function and validate the results
            data_files = ministerio_de_salud_peru_data()

            assert data_files[0][0] == "file1.xlsx"
            assert data_files[0][1] == "."
            assert data_files[0][2] == b"Hello, world!'"
            assert data_files[1][0] == "file2.xlsx"
            assert data_files[1][1] == "."
            assert data_files[1][2] == b"Hello, world!'"


@pytest.fixture
def mock_get_path():
    with patch("dart_pipeline.paths.get_path") as mock_path:
        mock_path.return_value = "/mock/path"
        yield mock_path


@pytest.fixture
def mock_plot_timeseries():
    with patch("dart_pipeline.plots.plot_timeseries") as mock_plot:
        yield mock_plot


@pytest.fixture
def mock_read_excel():
    with patch("pandas.read_excel") as mock_read:
        mock_read.return_value = pd.DataFrame(
            {
                "ano": [2023, 2023],
                "semana": [1, 2],
                "tipo_dx": ["C", "P"],
                "n": [10, 20],
            }
        )
        yield mock_read


@pytest.fixture
def mock_os_walk():
    with patch("os.walk") as mock_walk:
        mock_walk.return_value = [
            (
                "/mock/source/path",
                ["subdir"],
                ["casos_dengue_nacional.xlsx", "casos_dengue_region1.xlsx"],
            )
        ]
        yield mock_walk


@pytest.mark.parametrize(
    "admin_level, expected_admin_level_1, expected_plot_calls, should_raise",
    [
        ("0", "", 1, False),  # Admin level 0
        ("1", "Region1", 1, False),  # Admin level 1
        ("2", None, 0, True),  # Invalid admin level
    ],
)
def test_process_dengueperu(
    admin_level,
    expected_admin_level_1,
    expected_plot_calls,
    should_raise,
    mock_get_path,
    mock_plot_timeseries,
    mock_read_excel,
    mock_os_walk,
):
    if should_raise:
        match = f"Invalid admin level: {admin_level}"
        with pytest.raises(ValueError, match=match):
            process_dengueperu(admin_level=admin_level)
    else:
        master = process_dengueperu(admin_level=admin_level, plots=True)

        # Validate the output DataFrame
        assert isinstance(master, pd.DataFrame)
        assert master["admin_level_0"].iloc[0] == "Peru"
        assert master["admin_level_1"].iloc[0] == expected_admin_level_1
        assert master["metric"].tolist() == [
            "Confirmed Dengue Cases",
            "Probable Dengue Cases",
        ]

        # Check the mock calls
        mock_read_excel.assert_called()
