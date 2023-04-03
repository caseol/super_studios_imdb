import mysql.connector as mysql
import pandas as pd
import numpy as np
import sys

# define parametros de conexão ao banco de dados
db = mysql.connect(
    host="localhost",
    user="root",
    password="abc123",
    database="super_studios"
)
# abre ponteiro para escrever no banco
cursor = db.cursor()

print("Lendo source/base_imdb_final_thiago.csv")
df = pd.read_csv('source/base_imdb_final_thiago.csv', engine='python')
print("Lendo source/base_imdb_final_thiago.csv - ok!")

for index, row in df.iterrows():
    insert_sql = "INSERT INTO content_imdb_thiago (title_id, title_type, title_name, title_yearstart, title_yearend, " \
                 "runtime_unadjusted, title_genres, reviews_rating, reviews_count, episodes_count, runtime_adjusted, " \
                 "director_name, writer_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    insert_values = [
                     "" if pd.isnull(row['title_id']) else row['title_id'],
                     "" if pd.isnull(row['title_type']) else row['title_type'],
                     "" if pd.isnull(row['title_name']) else row['title_name'],
                     0 if np.isnan(row['title_yearstart']) else row['title_yearstart'],
                     0 if np.isnan(row['title_yearend']) else row['title_yearend'],
                     0 if np.isnan(row['runtime_unadjusted']) else row['runtime_unadjusted'],
                     "" if pd.isnull(row['title_genres']) else row['title_genres'],
                     0.0 if np.isnan(row['reviews_rating']) else row['reviews_rating'],
                     0.0 if np.isnan(row['reviews_count']) else row['reviews_count'],
                     0 if np.isnan(row['episodes_count']) else row['episodes_count'],
                     0 if np.isnan(row['runtime_adjusted']) else row['runtime_adjusted'],
                     "" if pd.isnull(row['director_name']) else row['director_name'],
                     "" if pd.isnull(row['writer_name']) else row['writer_name']]
    try:
        cursor.execute(insert_sql, insert_values)
        db.commit()
        print("Idx: " + str(index) + " - Gravando: " + row['title_id'] + " - ok")
    except Exception:
        print("Idx: " + str(index) + " - NOK! - ERRO motivo: " + str(sys.exc_info()))