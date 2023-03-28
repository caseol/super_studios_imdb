import pandas as pd
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
imdb_ids = pd.read_csv("source/ids_imdb.csv")
imdb_main_parts = np.array_split(imdb_ids, 6)

imdb_parts = np.array_split(imdb_main_parts[1], 8)


def process_imdb(imdb_ids):
    # instancia objeto para API IMDB
    imdb = IMDb()

    # lê cada linha do CSV e busca na API do IMDB
    for index, row in imdb_ids.iterrows():
        if index > 0:
            print("Lendo linha do CSV - index: " + str(index) + " - row[0]: " + str(row[0]))
            result = imdb.getAllFeatures(row[0], seconds=False)
            print(result)
            queue.put(result)


with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    results = executor.map(process_imdb, imdb_parts)

wcd.stop()
