Overview
This project implements a Google Cloud Function that processes large Excel files containing Amazon Sponsored Products campaign data. It downloads zipped Excel files from Google Drive, processes them using a flexible schema, and uploads the results to Google Cloud Storage in Avro format. Finally, it loads the data into BigQuery for analysis.
Key Features

Multithreading: Utilizes concurrent processing to handle large Excel files efficiently.
Flexible Schema: Uses a configurable Avro schema and key mapping, allowing for easy adaptation to different Excel structures.
Streaming Excel Reader: Employs a memory-efficient streaming approach to read large Excel files.
Google Drive Integration: Downloads input files directly from Google Drive.
Google Cloud Storage: Uploads processed data as Avro files to GCS.
BigQuery Integration: Loads processed data into BigQuery for further analysis.
