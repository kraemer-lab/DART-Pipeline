"""Plot data."""
from pathlib import Path
import logging
import re

from matplotlib import pyplot as plt
import numpy as np

from .util import output_path


def plot_heatmap(source, data, pdate, title, colourbar_label):
    """Create a heat map."""
    data[data == 0] = np.nan
    plt.imshow(data, cmap='coolwarm', origin='upper')
    plt.colorbar(label=colourbar_label)
    plt.title(title)
    # Make the plot title file-system safe
    title = re.sub(r'[<>:"/\\|?*]', '_', str(pdate))
    title = title.strip()
    # Export
    path = Path(
        output_path(source),
        str(pdate).replace('-', '/'), title + '.png'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info(f'Exporting:{path}')
    plt.savefig(path)
    plt.close()
