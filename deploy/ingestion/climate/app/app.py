import os
from schema import Database, Climate

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

connection = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def populate(db):
    """
    Populate the Climate table with some data
    """

    session = db.Session()

    line1 = Climate(37.7749, -122.4194, 15.0)  # lat, long, temperature
    line2 = Climate(15.1971, -7.5014, 16.0)
    line3 = Climate(69.7756, -152.8042, 17.0)

    session.add(line1)
    session.add(line2)
    session.add(line3)

    session.commit()
    session.close()


def query(db):
    session = db.Session()
    count = session.query(Climate).count()
    query = session.query(Climate).all()
    print(f"Climate data (n={count})")
    for row in query:
        print(row.latitude, row.longitude, row.temperature)
    session.close()


if __name__ == "__main__":
    db = Database(connection)
    db.init_tables()  # create tables
    populate(db)
    query(db)
