import os
import psycopg2

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')

if not all([POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB]):
    raise ValueError('Please set the environment variables POSTGRES_HOST, '
                     'POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB')


def populate():
    config = {
        'host': POSTGRES_HOST,
        'user': POSTGRES_USER,
        'password': POSTGRES_PASSWORD,
        'dbname': POSTGRES_DB
    }
    queries = [
        ("Current time is: ", "SELECT NOW();"),
        ("Fetching data from climate table", "SELECT * FROM climate;"),
        ("Fetching data from epi table", "SELECT * FROM epi;"),
    ]
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for title, query in queries:
                    print(title)
                    cur.execute(query)
                    for row in cur.fetchall():
                        print(row)
                    print()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: ", error)


if __name__ == '__main__':
    populate()
