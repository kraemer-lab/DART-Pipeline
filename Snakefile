rule run:
    input:
        "A Collate Data/Geospatial Data/{data_name}/"
    output:
        "B Process Data/Geospatial Data/{data_name}/"
    resources:
        threads = 1
        # mem_mb = 1
    shell:
        "python3.12 'B Process Data/process_data.py' '{input}'"
