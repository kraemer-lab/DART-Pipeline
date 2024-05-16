FROM python:3.11

WORKDIR /app

# Copy requirements files and install dependencies
COPY ingestion/climate/app/requirements.txt /app/requirements.txt
COPY ingestion/climate/app/collate/requirements.txt /app/collate/requirements.txt

# Install dependencies for all steps in the pipeline
RUN python -m pip install -r requirements.txt
RUN python -m pip install -r collate/requirements.txt
RUN python -m pip install distro

# Copy the rest of the application and database schema
COPY startup/map/app/ /app
