import os
import logging
from datetime import datetime
from schema import Database, Climate
from collate.collate_data import process as collate
from aggregate.process_data import process as aggregate

# Set up logging
logfile = datetime.now().strftime("%Y-%m-%d-%H-%M-%S.log")
logging.basicConfig(filename=f"logs/{logfile}", encoding="utf-8", level=logging.DEBUG)

# Database connection (environment variables)
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

if not all(
    [POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB]
):
    raise ValueError(
        "Please set the environment variables POSTGRES_HOST, "
        "POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB"
    )

connection = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


def populate(db, filename):
    """
    Populate the Climate table with some data

    Does not check for duplicates - use for testing purposes
    """

    # Read data from csv file
    with open(filename, "r") as f:
        data = f.readlines()
    header = list(map(lambda x: x.strip(), data.pop(0).split(",")))
    try:
        # identify and validate columns
        idx_gid2 = header.index("gid2")
        idx_rainfall = header.index("Rainfall")
    except ValueError:
        raise ValueError("Expected columns not found in the csv file, got: ", header)

    # push to database in chunks
    logging.info("Populating Climate table...")
    chunk_size = 1000
    data_size = len(data)
    logging.info(f"Data size: {data_size}")
    session = db.Session()
    for i in range(0, data_size, chunk_size):
        logging.info(f"Processing chunk {i}...")
        session.bulk_save_objects(
            map(
                lambda x: Climate(
                    gid2=x[idx_gid2],
                    rainfall=float(x[idx_rainfall]),
                ),
                [line.strip().split(",") for line in data[i : i + chunk_size]],
            )
        )
        session.commit()

    # close
    logging.info("Closing session...")
    session.close()
    logging.info("Done")


def query(db):
    session = db.Session()
    count = session.query(Climate).count()
    query = session.query(Climate).limit(5).all()
    logging.info(f"Climate data (n={count})")
    logging.info("Displaying the first 5 rows...")
    for row in query:
        logging.info(row.gid2, row.rainfall)
    session.close()


if __name__ == "__main__":
    metrics = ["GADM", "CHIRPS rainfall"]
    admin_level = "2"
    iso3 = "VNM"
    year = "2024"
    month = "01"
    rt = "daily"

    logging.info("Collating data")
    for metric in metrics[1:]:  # GADM is pre-loaded
        collate(metric, only_one=True)

    logging.info("Aggregating data")
    aggregate(
        data_name=metrics,
        admin_level=admin_level,
        iso3=iso3,
        year=year,
        month=month,
        rt=rt,
        debug=True,
    )

    # Write to database
    logging.info("Writing to database")
    db = Database(connection)
    db.init_tables()  # create tables
    filename = (
        "/app/data/B Process Data/Geospatial and Meteorological Data/"
        "GADM administrative map and CHIRPS rainfall data/VNM/Admin Level 2/"
        "Rainfall.csv"
    )
    populate(db, filename)

    # Query the data back
    logging.info("Querying data")
    query(db)

    logging.info("Done")
