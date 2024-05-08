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
    commands = (

        """
        DROP TABLE IF EXISTS epi;
        """,

        """
        CREATE TABLE epi (
            id SERIAL PRIMARY KEY,
            datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            metric1 FLOAT
        );
        """,

        """
        INSERT INTO epi (
            id,
            datetime,
            metric1
        ) VALUES
            (1, '2021-01-01 01:00:00', 1.0),
            (2, '2021-02-02 02:00:00', 2.0),
            (3, '2021-03-03 03:00:00', 3.0),
            (4, '2021-04-04 04:00:00', 4.0),
            (5, '2021-05-05 05:00:00', 5.0),
            (6, '2021-06-06 06:00:00', 6.0),
            (7, '2021-07-07 07:00:00', 7.0),
            (8, '2021-08-08 08:00:00', 8.0),
            (9, '2021-09-09 09:00:00', 9.0),
            (10, '2021-10-10 10:00:00', 10.0)
        ;
        """,
    )
    config = {
        'host': POSTGRES_HOST,
        'user': POSTGRES_USER,
        'password': POSTGRES_PASSWORD,
        'dbname': POSTGRES_DB
    }
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for command in commands:
                    print(f"Executing command: {command.split()[0]}")
                    cur.execute(command)
                print("Fetching data from epi table")
                cur.execute("SELECT * FROM epi;")
                rows = cur.fetchall()
                for row in rows:
                    print(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: ", error)


if __name__ == '__main__':
    populate()
