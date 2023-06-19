import pandas as pd
import time
import warnings
import numpy as np
import concurrent.futures
import random
from imdb_api import IMDb
from queue import Queue
from workers.write_content_imdb import WriteContentDb
from workers.update_content_db import UpdateContentDb

# silencia warnings
warnings.filterwarnings("ignore")

# instancia fila para incluir dados do resultado da busca no IMDB
queue = Queue(maxsize=512)

# instancia thread para gravar os dados em separado
wcd = WriteContentDb(queue=queue).start()
#ucd = UpdateContentDb(queue=queue).start()

# lê da do arquivo CSV todos os IMDB IDs
#imdb_ids = pd.read_csv("source/ids_imdb.csv")
imdb_ids = pd.read_csv("source/ids_imdb_to_update.csv")

# divide a base de IDs do IMDB em 6 partes
imdb_main_parts = np.array_split(imdb_ids, 6)

# worker que será passada ao ThreadPoolExecutor
def process_imdb(imdb_ids):
    # instancia objeto para API IMDB
    imdb = IMDb()
    cont = 0
    # lê cada linha do CSV e busca na API do IMDB
    for index, row in imdb_ids.iterrows():
        if index > 0 and cont <= 40:
            print("Contador: " + str(cont) + " - Lendo linha do CSV - index: " + str(index) + " - row[0]: " + str(row[0]))
            result = imdb.getAllFeatures(row[0], seconds=False)
            print(result)
            queue.put(result)
            cont += 1
        else:
            cont = 0
            print("Colocando pra dormir")
            time_to_wait = random.randint(5, 20)
            time.sleep(time_to_wait)


# a busca por budget e world já foi incluída em getAllFeatures
def update_budget_and_gross(imdb_ids):
    print("update_budget_and_gross: ")
    # instancia objeto para API IMDB
    imdb = IMDb()
    budget = ""
    gross_world = ""

    cont = 0
    # lê cada linha do CSV e busca na API do IMDB
    for index, row in imdb_ids.iterrows():
        if index > 0 and cont <= 40:
            budget, gross_world = imdb.getBoxOffice(row[0])
            print("Contador: " + str(cont) + " - Lendo linha do CSV - index: " + str(index) + " - row[0]: " + str(
                row[0]) + " budget: " + str(budget) + " gross_world: " + gross_world)
            queue.put([row[0], budget, gross_world])
            cont += 1
        else:
            cont = 0
            print("Colocando pra dormir")
            time_to_wait = random.randint(5, 20)
            time.sleep(time_to_wait)

with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
    results = executor.map(process_imdb, imdb_main_parts)


# pool de threads que buscava apenas a informação de budget e world_gross e atualizava nos registros existentes
#with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
#    results = executor.map(update_budget_and_gross, imdb_parts)

wcd.stop()
#ucd.stop()
