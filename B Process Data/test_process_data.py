"""
Run unit tests on process_data.py.

Past runs
---------
- 2024-02-08 on Ubuntu 22.04 using Python 3.12: Ran 2 tests in 0.824s
"""
import rasterio
import unittest
from process_data import plot_pop_density, pixel_to_latlon
from pathlib import Path
import pandas as pd
import utils


class TestCases(unittest.TestCase):

    def test_plot_pop_density(self):
        # Set up
        out_dir = Path('tests')
        out_dir.mkdir(parents=True, exist_ok=True)

        # Plot whole country
        base_dir = str(utils.get_base_directory())
        relative_path = Path(
            'B Process Data',
            'Geospatial and Socio-Demographic Data',
            'GADM administrative map and WorldPop population density',
            'Vietnam.csv'
        )
        path = Path(base_dir, relative_path)
        df = pd.read_csv(path)
        plot_pop_density(df, out_dir, 'Vietnam.png')

        # Check if the file has been created
        expected = True
        path = Path(out_dir, 'Vietnam.png')
        actual = path.exists()

        # Perform the test
        self.assertEqual(expected, actual)

        # Tear down
        path.unlink()
        Path('tests/').rmdir()

    def test_pixel_to_latlon(self):
        x = [0]
        y = [0]
        transform = [
            102.14, 0.00, 0.00,
            23.39, 0.00, -0.00,
        ]
        affine_transform = rasterio.Affine.from_gdal(*transform)
        lat, lon = pixel_to_latlon(x, y, affine_transform)

        # Perform the test
        expected = [[23.39]], [[102.14]]
        actual = lat, lon
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
