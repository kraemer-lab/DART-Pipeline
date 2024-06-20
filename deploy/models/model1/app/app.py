import os
from schema import Database, Climate, Epi

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


def query(db):
    session = db.Session()

    # Query the Climate table
    count = session.query(Climate).count()
    query = session.query(Climate).all()
    print(f"Climate data (n={count})")
    for row in query:
        print(row.latitude, row.longitude, row.temperature)
    session.close()

    # Query the Epi table
    count = session.query(Epi).count()
    query = session.query(Epi).all()
    print(f"Epi data (n={count})")
    for row in query:
        print(row.metric1)
    session.close()


if __name__ == "__main__":
    db = Database(connection)
    query(db)
