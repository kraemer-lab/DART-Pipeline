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

    # Load data from csv
    with open('/app/data/climate.csv', 'r') as f:
        lines = f.readlines()
    lines = [line.strip() for line in lines]
    data = '\n'.join(['(' + line + '),' for line in lines[1:]])[:-1]

    commands = (

        """
        DROP TABLE IF EXISTS climate;
        """,

        """
        CREATE TABLE climate (
            id SERIAL PRIMARY KEY,
            datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            temperature FLOAT
        );
        """,

        """
        INSERT INTO climate (
            id,
            datetime,
            latitude,
            longitude,
            temperature
        ) VALUES
        """
        + data
        + """
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
                print("Fetching data from climate table")
                cur.execute("SELECT * FROM climate;")
                rows = cur.fetchall()
                for row in rows:
                    print(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: ", error)


if __name__ == '__main__':
    populate()
