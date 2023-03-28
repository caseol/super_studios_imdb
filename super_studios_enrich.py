import pandas as pd
import warnings
import numpy as np
from imdb_api import IMDb
from queue import Queue
from workers.write_content_db import WriteContentDb

# silencia warnings
warnings.filterwarnings("ignore")

# instancia fila para incluir dados do resultado da busca no IMDB
queue = Queue(maxsize=256)

# instancia thread para gravar os dados em separado
wcd = WriteContentDb(queue=queue).start()

# lê da do arquivo CSV todos os IMDB IDs
imdb_ids = pd.read_csv("source/ids_imdb.csv")

# instancia objeto para API IMDB
imdb = IMDb()



# lê cada linha do CSV e busca na API do IMDB
for index, row in imdb_ids.iterrows():
    if index > 0:
        print("Lendo linha do CSV - index: " + str(index) + " - row[0]: " + str(row[0]))
        result = imdb.getAllFeatures(row[0], seconds=False)
        print(result)
        queue.put(result)

wcd.stop()