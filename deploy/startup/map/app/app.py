import logging
from datetime import datetime
from collate.collate_data import process as collate

logfile = f"logs/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logging.basicConfig(filename=logfile, level=logging.INFO)

if __name__ == "__main__":
    logging.info("Checking for presence of shape files")
    filename = (
        "data/"
        "A Collate Data/"
        "Geospatial Data/"
        "GADM administrative map/"
        "VNM/"
        "gadm41_VNM_shp/gadm41_VNM_2.shp"
    )

    try:
        with open(filename) as f:
            logging.info("Shape files already exist, skipping download...")
    except FileNotFoundError:
        logging.info("Collating data")
        collate("GADM", only_one=True)
        logging.info("Done")
