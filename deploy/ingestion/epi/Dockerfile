FROM python:3.11

WORKDIR /app
COPY ingestion/epi/app/ /app
COPY schema/ /app/schema
RUN python -m pip install -r requirements.txt
