
Secrets need to be placed into `service/.env`. This should include:
```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=dart
```
noting that you should substitute the actual username and password for something more
secure.

To start the service stack (specifically the database provider), run `./start.sh`.

To run individual services, run `./start.sh service_name`.
To run services at regular intervals, use cron by running `crontab -e` and
adding service entries. _Note: You will need to ensure that `PATH` is set correctly
within the `start.sh` script, for cron to work correctly.__

You can provide multiple entries here and customise the timings as needed.  The
service name should be one of the services provided in the `services/docker-compose.yml`
file. Timings are in CRON format (see e.g. [cronitor](https://crontab.guru/) for help
with the syntax). For example:
```
* * * * * /path/to/dart/start.sh climate
0 1 * * * /path/to/dart/start.sh epi
30 3 * * 3 /path/to/dart/start.sh flight
```
will run the `climate` service every minute, the `epi` services once a day at 1am (local
time), and the `flight` service every Wednesday at 3:30am.

> script for initial build (queries username, password and db name)
> procedure for adding new users
