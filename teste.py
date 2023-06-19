import pandas as pd
import numpy as np
import warnings
from imdb_api import IMDb
from queue import Queue
from workers.write_content_db import WriteContentDb

# silencia warnings
warnings.filterwarnings("ignore")

# instancia fila para incluir dados do resultado da busca no IMDB
#queue = Queue(maxsize=256)

# instancia thread para gravar os dados em separado
#wcd = WriteContentDb(queue=queue).start()

# lê da do arquivo CSV todos os IMDB IDs
#imdb_ids = pd.read_csv("source/ids_imdb.csv")

# instancia objeto para API IMDB
imdb = IMDb()
result = imdb.getAllFeatures('tt3953236', seconds=False)
print (result)

#distributors = imdb.getDistributorsInfo('tt0059719')
#details = imdb.getDetails('tt0059719')

#budget, gross_world = imdb.getBoxOffice('tt0372784')
#print("Budget: " + budget)
#print("Gross World: " + gross_world)

#df_parts = np.array_split(imdb_ids, 4)

# Print the length of each part to verify that they are equal
#for i, part in enumerate(df_parts):
#    print(f"Length of part {i+1}: {len(part)}")

#print(details)
#print(distributors)
