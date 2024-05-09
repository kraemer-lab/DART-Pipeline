import os
from schema import Database, Epi

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
    Populate the Epi table with some data
    """

    session = db.Session()

    line1 = Epi(1.0)  # metric1
    line2 = Epi(2.0)
    line3 = Epi(3.0)

    session.add(line1)
    session.add(line2)
    session.add(line3)

    session.commit()
    session.close()


def query(db):
    session = db.Session()
    count = session.query(Epi).count()
    query = session.query(Epi).all()
    print(f"Epi data (n={count})")
    for row in query:
        print(row.metric1)
    session.close()


if __name__ == "__main__":
    db = Database(connection)
    db.init_tables()  # create tables
    populate(db)
    query(db)
