from datetime import date
import requests
import json
import os
import logging

HOME = os.path.expanduser('~')
url = 'https://public.opendatasoft.com/api/records/1.0/search/?dataset=elections-france-presidentielles-2022-2nd-tour-par-bureau-de-vote&q=&facet=dep_name&facet=com_name&facet=nom&facet=prenom&facet=reg_name&facet=lib_du_b_vote&refine.dep_name=Bouches-du-Rh%C3%B4ne'
datalake_root_folder = HOME + "/airflow/"
path_to_store_file = "project/raw/data_bouches_du_rhone_13/election_pres_2022/"


def fetch_data_from_bouches_du_rhone():
    """
    Effectue une requête pour récupérer les données sur les élections présidentielles
    du département spécifié et les enregistre localement.
    """
    query_result = query_open_data_bouches_du_rhone()
    store_open_data_bouches_du_rhone(query_result.json())


def query_open_data_bouches_du_rhone():
    """
    Effectue une requête GET à l'API OpenDataSoft pour récupérer les données des élections présidentielles
    pour le département spécifié.
    """
    return requests.get(url)


def store_open_data_bouches_du_rhone(opendata):
    """
    Enregistre les données des élections présidentielles dans un fichier JSON avec la date actuelle.
    """
    current_day = date.today().strftime("%Y%m%d")
    target_path = os.path.join(datalake_root_folder, path_to_store_file, current_day)
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    logging.info("Écriture ici : %s", target_path)
    with open(os.path.join(target_path, "bouches_du_rhone.json"), "w+") as f:
        f.write(json.dumps(opendata, indent=4))



# Appel de la fonction d'ingestion
fetch_data_from_bouches_du_rhone()

# Code pour le traitement en temps réel via Kafka
# ------------------------------------------------
"""
# Importation des modules Kafka nécessaires
from kafka import KafkaProducer

# Configuration des paramètres Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'election_data_topic'

# Création d'une instance du producteur Kafka
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

# Envoi des données dans le topic Kafka
with open(os.path.join(target_path, "bouches_du_rhone.json"), "r") as f:
    data = f.read()
    producer.send(kafka_topic, value=data.encode())

# Fermeture du producteur Kafka
producer.close()
"""


# Code pour l'ingestion des données via Airbyte
# ---------------------------------------------
"""
# Importation des modules Airbyte nécessaires
from airbyte import AirbyteClient

# Configuration des paramètres Airbyte
airbyte_endpoint = 'http://localhost:8001'
airbyte_api_key = 'YOUR_API_KEY'

# Création d'une instance du client Airbyte
client = AirbyteClient(endpoint=airbyte_endpoint, api_key=airbyte_api_key)

# Ajout d'une nouvelle connexion pour l'ingestion des données
connection_name = 'bouches_du_rhone_data_connection'
source_id = 'open_data_source_id'
destination_id = 'your_destination_id'
sync_mode = 'incremental'

connection_config = {
    'source_id': source_id,
    'destination_id': destination_id,
    'sync_mode': sync_mode,
    'config': {
        'url': f'file://{target_path}/bouches_du_rhone.json'
    }
}

client.create_connection(connection_name, connection_config)

# Lancement de la synchronisation pour l'ingestion des données
client.sync_connection(connection_name)

# Fermeture du client Airbyte
client.close()
"""