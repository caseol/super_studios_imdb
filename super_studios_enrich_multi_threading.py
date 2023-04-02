import pandas as pd
import time
import warnings
import numpy as np
import concurrent.futures
from imdb_api import IMDb
from queue import Queue
from workers.write_content_imdb import WriteContentDb

# silencia warnings
warnings.filterwarnings("ignore")

# instancia fila para incluir dados do resultado da busca no IMDB
queue = Queue(maxsize=512)

# instancia thread para gravar os dados em separado
wcd = WriteContentDb(queue=queue).start()

# lê da do arquivo CSV todos os IMDB IDs
#imdb_ids = pd.read_csv("source/ids_imdb.csv")
imdb_ids = pd.read_csv("source/unique_to_id_imdb.csv")

# divide a base de IDs do IMDB em 6 partes
imdb_main_parts = np.array_split(imdb_ids, 3)
# cada código vai usar 1 parte principal que será dividida em 8 sub-partes, para que cada sub-parte
# possa ser processada por uma thread específica
imdb_parts = np.array_split(imdb_main_parts[0], 8)

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
            time.sleep(25)


with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    results = executor.map(process_imdb, imdb_parts)

wcd.stop()
