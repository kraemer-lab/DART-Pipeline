"""
Run unit tests on process_data.py.

Past runs:

- 2024-01-17 macOS Sonoma, Python 3.12:
"""
import unittest
from process_data import get_base_directory, plot_pop_density, pixel_to_latlon
from pathlib import Path
import pandas as pd


class TestCases(unittest.TestCase):

    # def test_get_base_directory(self):
    #     expected = str(Path('~/DART-Pipeline').expanduser())
    #     actual = get_base_directory()
    #     self.assertEqual(expected, actual)

    def test_plot_pop_density(self):
        base_dir = get_base_directory()
        relative_path = Path(
            'B Process Data'
            'Geospatial and Socio-Demographic Data',
            'GADM administrative map and WorldPop population density',
            'Vietnam.csv'
        )
        path = Path(base_dir, relative_path)
        df = pd.read_csv(path)
        # Plot whole country
        plot_pop_density(df, path.parent, 'Vietnam.png')


if __name__ == '__main__':
    unittest.main()
