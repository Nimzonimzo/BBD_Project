from datetime import date
import requests
import json
import os

HOME = os.path.expanduser('~')
URL = 'https://public.opendatasoft.com/api/records/1.0/search/?dataset=elections-france-presidentielles-2022-2nd-tour-par-bureau-de-vote&q=&facet=dep_name&facet=com_name&facet=nom&facet=prenom&facet=reg_name&facet=lib_du_b_vote&refine.dep_name=Nord'
DATALAKE_ROOT_FOLDER = HOME + "/airflow/"
PATH_TO_STORE_FILE = "project/raw/data_nord_59/election_pres_2022/"


def fetch_data_from_nord():
    """
    Effectue une requête pour récupérer les données sur les élections présidentielles
    du département spécifié et les enregistre localement.
    """
    query = query_open_data_nord()
    store_open_data_nord(query.json())


def query_open_data_nord():
    """
    Effectue une requête GET à l'API OpenDataSoft pour récupérer les données des élections présidentielles
    pour le département spécifié.
    """
    return requests.get(URL)


def store_open_data_nord(opendata):
    """
    Enregistre les données des élections présidentielles dans un fichier JSON avec la date actuelle.
    """
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + PATH_TO_STORE_FILE + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)
    print("Écriture ici : ", TARGET_PATH)
    with open(TARGET_PATH + "nord.json", "w+") as f:
        f.write(json.dumps(opendata, indent=4))

# Appel de la fonction d'ingestion
fetch_data_from_nord()
