
import subprocess
import pandas as pd
from pyspark.sql import SparkSession

# Installer les bibliothèques requises
subprocess.check_call(['pip', 'install', 'pandas'])
subprocess.check_call(['pip', 'install', 'pyspark'])

# Chemins des fichiers Parquet
file1 = '/Users/yuki/airflow/project/formatted/data_bouches_du_rhone_13/election_pres_2022/20230611/bouches_du_rhone.snappy.parquet'
file2 = '/Users/yuki/airflow/project/formatted/data_nord_59/election_pres_2022/20230611/nord.snappy.parquet'
output_file = '/Users/yuki/airflow/project/usage/combined_data_pandas.csv'

# Méthode 1 : Utilisation de Pandas
# Commentez cette partie si vous souhaitez utiliser la méthode 2 via Spark
"""""
def combine_data(file1, file2, output_file):
    # Lire les fichiers Parquet
    df1 = pd.read_parquet(file1)
    df2 = pd.read_parquet(file2)
    print(df1)
    print(df2)

    # Combinaison des DataFrames
    combined_df = pd.concat([df1, df2])

    # Écrire le DataFrame combiné dans un fichier CSV
    combined_df.to_csv(output_file, index=False)

    print("Les fichiers Parquet ont été combinés avec succès en un fichier CSV (méthode 1 : Pandas).")
"""

# Méthode 2 : Utilisation de Spark
# Commentez cette partie si vous souhaitez utiliser la méthode 1 avec Pandas
# Configuration de Spark

def combine_data(file1, file2, output_file):
    # Lire les fichiers Parquet
    df1 = pd.read_parquet(file1)
    df2 = pd.read_parquet(file2)
    print(df1)
    print(df2)

    # Combinaison des DataFrames
    combined_df = pd.concat([df1, df2])

    # Écrire le DataFrame combiné dans un fichier CSV
    combined_df.to_csv(output_file, index=False)

    print("Les fichiers Parquet ont été combinés avec succès en un fichier CSV (méthode 1 : Pandas).")

combine_data(file1, file2, output_file)