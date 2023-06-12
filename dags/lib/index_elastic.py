import json
from elasticsearch import Elasticsearch

# Set your Elastic Cloud ID and API Key
CLOUD_ID = "My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ3N2E0YWRiZWRkOTY0OWFiYTQ5MWQwYzBjOWM5Mzc4MiQ3Mjc1MmY1ZmY5ZGY0MTg1OGZkOWI5MGM2YjQ3OTAxYQ=="
API_KEY = "d3ddfc90-6c4b-411a-a535-da165a3caa48"

# Create the Elasticsearch client instance
es = Elasticsearch(
    cloud_id=CLOUD_ID,
    api_key=API_KEY,
)


def index_to_elastic_bouches_du_rhone():
    # Indexing logic for Bouches-du-Rhône data
    index_name = 'bouches_du_rhone'
    file_path = '/Users/yuki/airflow/project/raw/data_bouches_du_rhone_13/election_pres_2022/20230611/bouches_du_rhone.json'

    # Read the JSON file
    with open(file_path) as file:
        json_data = json.load(file)

    # Index the JSON data
    result = es.index(index=index_name, body=json_data)


def index_to_elastic_nord():
    # Indexing logic for Nord data
    index_name = 'nord'
    file_path = '/Users/yuki/airflow/project/raw/data_nord_13/election_pres_2022/20230611/nord.json'

    # Read the JSON file
    with open(file_path) as file:
        json_data = json.load(file)

    # Index the JSON data
    result = es.index(index=index_name, body=json_data)


def index_to_elastic():
    # Call the functions to index data into Elasticsearch
    index_to_elastic_bouches_du_rhone()
    index_to_elastic_nord()


# Call the main indexing function
index_to_elastic()