"""
Peru Dengue data
"""

import os
import re
import base64
import logging
from datetime import date
from pathlib import Path
from typing import Literal

import requests
import pandas as pd
from bs4 import BeautifulSoup

from ..constants import OUTPUT_COLUMNS
from ..plots import plot_timeseries
from ..paths import get_path
from ..types import DataFile

PERU_REGIONS = [
    "AMAZONAS",
    "ANCASH",
    "AREQUIPA",
    "AYACUCHO",
    "CAJAMARCA",
    "CALLAO",
    "CUSCO",
    "HUANUCO",
    "ICA",
    "JUNIN",
    "LA LIBERTAD",
    "LAMBAYEQUE",
    "LIMA",
    "LORETO",
    "MADRE DE DIOS",
    "MOQUEGUA",
    "PASCO",
    "PIURA",
    "PUNO",
    "SAN MARTIN",
    "TUMBES",
    "UCAYALI",
]


def ministerio_de_salud_peru_data() -> list[DataFile]:
    """
    Download data from the Ministerio de Salud (Peru).

    https://www.dge.gob.pe/sala-situacional-dengue
    """
    pages = ["Nacional_dengue"] + ["sala_dengue_" + region for region in PERU_REGIONS]
    # If the user specifies that only one dataset should be downloaded
    data: list[DataFile] = []
    for page in pages:
        url = "https://www.dge.gob.pe/sala-situacional-dengue/uploads/" + f"{page}.html"
        print(f"Accessing {url}")
        response = requests.get(url)
        # Raise an exception for bad response status
        response.raise_for_status()
        # Parse HTML content
        soup = BeautifulSoup(response.content, "html.parser")
        # Find links with the onclick attribute in both <a> and <button> tags
        onclick_elements = soup.find_all(
            lambda tag: tag.name in ["a", "button"] and tag.has_attr("onclick")
        )
        links = [element.get("onclick") for element in onclick_elements]
        if not links:
            raise ValueError("No links found on the page")

        for link in links:
            # Search the link for the data embedded in it
            matches = re.findall(r"base64,(.*?)(?='\).then)", link, re.DOTALL)
            if matches:
                base64_string = matches[0]
            else:
                raise ValueError("No data found embedded in the link")

            # Search the link for the filename
            matches = re.findall(r"a\.download = '(.*?)';\s*a\.click", link)
            if matches:
                # There is an actual filename for this data
                filename = matches[0]
            else:
                # Use the page name for the file
                filename = page + ".xlsx"

            file = DataFile(filename, ".", base64.b64decode(base64_string))
            data.append(file)
    return data


def process_dengueperu(
    admin_level: Literal["0", "1"] | None = None, plots=False
) -> pd.DataFrame:
    """Process data from the Ministerio de Salud - Peru."""
    if not admin_level:
        admin_level = "0"
        logging.info("admin_level:None (defaulting to %s)", admin_level)
    elif admin_level in ["0", "1"]:
        logging.info("admin_level:%s", admin_level)
    else:
        raise ValueError(f"Invalid admin level: {admin_level}")

    # Find the raw data
    path = get_path("sources", "PER", "dengue")
    if admin_level == "0":
        filepaths = [Path(path, "casos_dengue_nacional.xlsx")]
    else:
        filepaths = []
        for dirpath, _, filenames in os.walk(path):
            filenames.sort()
            for filename in filenames:
                # Skip hidden files
                if filename.startswith("."):
                    continue
                # Skip admin levels that have not been requested for analysis
                if filename == "casos_dengue_nacional.xlsx":
                    continue
                filepaths.append(Path(dirpath, filename))

    # Initialise an output data frame
    master = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Import the raw data
    for filepath in filepaths:
        logging.info("importing:%s", filepath)
        df = pd.read_excel(filepath)

        # Rename the headings
        columns = {"ano": "year", "semana": "week", "tipo_dx": "metric", "n": "value"}
        df = df.rename(columns=columns)

        # Define two metrics
        df.loc[df["metric"] == "C", "metric"] = "Confirmed Dengue Cases"
        df.loc[df["metric"] == "P", "metric"] = "Probable Dengue Cases"
        # Confirm no rows have been missed
        metrics = ["Confirmed Dengue Cases", "Probable Dengue Cases"]
        mask = ~df["metric"].isin(metrics)
        assert len(df[mask]) == 0

        # Get the name of the administrative divisions
        filename = filepath.name
        name = filename.removesuffix(".xlsx").split("_")[-1].capitalize()
        logging.info("processing:%s", name)
        # Add to the output data frame
        df["admin_level_0"] = "Peru"
        if admin_level == "1":
            df["admin_level_1"] = name
        else:
            df["admin_level_1"] = ""
        df["admin_level_2"] = ""
        df["admin_level_3"] = ""

        # Add to master data frame
        master = pd.concat([master, df], ignore_index=True)

        # Plot
        if plots:
            df["date"] = pd.to_datetime(
                df["year"].astype(str) + df["week"].astype(str) + "1", format="%Y%U%w"
            )
            start = df.loc[0, "year"]
            end = df.loc[len(df) - 1, "year"]
            if admin_level == "0":
                title = f"Dengue Cases\nPeru - {start} to {end}"
            else:
                title = f"Dengue Cases\n{name} - {start} to {end}"
            path = get_path("output", "PER", "dengue", name + ".png")
            plot_timeseries(df, title, path)

    # Fill in additional columns
    master["iso3"] = "PER"
    master["month"] = ""
    master["day"] = ""
    master["unit"] = "cases"
    master["creation_date"] = date.today()

    return master
