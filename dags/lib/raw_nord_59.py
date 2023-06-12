import json
import os

import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/airflow/"
PATH_TO_STORE_FILE = "project/raw/data_nord_59/election_pres_2022/"
PATH_TO_FORMATTED = "project/formatted/data_nord_59/election_pres_2022/"


def convert_raw_to_formatted_nord(file_name, current_day, **kwargs):
    # Chemin du fichier JSON brut
    path_raw_json = DATALAKE_ROOT_FOLDER + PATH_TO_STORE_FILE + current_day + "/" + file_name

    # Dossier de stockage des fichiers formatés
    formatted_rating_folder = DATALAKE_ROOT_FOLDER + PATH_TO_FORMATTED + current_day + "/"
    if not os.path.exists(formatted_rating_folder):
        os.makedirs(formatted_rating_folder)

    with open(path_raw_json, 'r') as f:
        df = json.load(f)

    parquet_file_name = file_name.replace(".json", ".snappy.parquet")
    df = pd.DataFrame(data=df['records'])
    final_df = pd.DataFrame.from_dict(pd.json_normalize(df['fields']), orient='columns')

    # Enregistrement du DataFrame formaté au format Parquet
    final_df.to_parquet(formatted_rating_folder + parquet_file_name)

convert_raw_to_formatted_nord("nord.json", "20230611")

""""
import json
import os
import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/airflow/"
PATH_TO_STORE_FILE = "project/raw/data_nord_59/election_pres_2022/"
PATH_TO_FORMATTED = "project/formatted/data_nord_59/election_pres_2022/"


def convert_raw_to_formatted_nord(file_name, current_day, **kwargs):
    # Chemin du fichier JSON brut
    path_raw_json = DATALAKE_ROOT_FOLDER + PATH_TO_STORE_FILE + current_day + "/" + file_name

    # Dossier de stockage des fichiers formatés
    formatted_rating_folder = DATALAKE_ROOT_FOLDER + PATH_TO_FORMATTED + current_day + "/"
    if not os.path.exists(formatted_rating_folder):
        os.makedirs(formatted_rating_folder)

    try:
        with open(path_raw_json, 'r') as f:
            data = json.load(f)
    except IOError:
        print("Erreur lors de la lecture du fichier JSON brut.")
        return

    records = data.get('records', [])
    if not records:
        print("Aucun enregistrement trouvé dans le fichier JSON brut.")
        return

    formatted_data = []
    for record in records:
        fields = record.get('fields', {})
        formatted_record = {
            'champ1': fields.get('champ1'),
            'champ2': fields.get('champ2'),
            # Ajouter d'autres champs nécessaires avec des conditions appropriées pour gérer les valeurs manquantes
        }
        formatted_data.append(formatted_record)

    final_df = pd.DataFrame(formatted_data)

    try:
        # Enregistrement du DataFrame formaté au format Parquet
        parquet_file_name = file_name.replace(".json", ".snappy.parquet")
        final_df.to_parquet(formatted_rating_folder + parquet_file_name)
        print("Conversion réussie : ", parquet_file_name)
    except Exception as e:
        print("Erreur lors de l'enregistrement du DataFrame formaté :", str(e))

convert_raw_to_formatted_nord("nord.json", "20230611")
"""