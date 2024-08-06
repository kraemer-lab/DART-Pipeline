import os
import logging
from datetime import datetime
from schema import Database, Climate, Epi

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

logfile = f"{datetime.now().strftime('%Y-%m-%d')}.log"
logging.basicConfig(filename=f"logs/{logfile}", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())  # Log to console / docker logs

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


def query_table(db, Table):
    # Query the table
    session = db.Session()
    count = session.query(Table).count()
    query = session.query(Table).limit(5).all()
    logging.info(f"{Table.__name__} data (n={count})")
    logging.info(f"Columns: {query[0].__table__.columns.keys()}")
    logging.info("Showing first 5 rows...")
    for row in query:
        for key, value in row.__dict__.items():
            if not key.startswith("_"):
                logging.info(f"{key}: {value}")
    session.close()


def query_all(db):
    query_table(db, Climate)
    query_table(db, Epi)


if __name__ == "__main__":
    db = Database(connection)
    query_all(db)
