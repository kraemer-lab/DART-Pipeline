FROM python:3.11

# Install LaTeX for generating PDFs
RUN apt-get update && apt-get install -y texlive-latex-base texlive-fonts-recommended texlive-fonts-extra texlive-latex-extra dvipng cm-super
RUN apt-get install -y gdal-bin libgdal-dev

WORKDIR /app

# Copy requirements files and install dependencies
COPY ingestion/climate/app/requirements.txt /app/requirements.txt
COPY ingestion/climate/app/collate/requirements.txt /app/collate/requirements.txt
COPY ingestion/climate/app/aggregate/requirements.txt /app/aggregate/requirements.txt

# Install dependencies for all steps in the pipeline
RUN python -m pip install -r requirements.txt
RUN python -m pip install -r collate/requirements.txt
RUN python -m pip install -r aggregate/requirements.txt
RUN python -m pip install distro

# Copy the rest of the application and database schema
COPY ingestion/climate/app/ /app
COPY schema/ /app/schema
