import pandas as pd

# dataset aws
content_aws = pd.read_csv('source/content_imdb_aws.csv', engine='python')
#ids_content_aws = content_aws.iloc[:,1]
print("content_aws carregado")

# dataset oracle
content_oracle = pd.read_csv('source/content_imdb_oracle.csv', engine='python')
#ids_content_oracle = content_oracle.iloc[:,1]
print("ids_content_oracle carregado")

# dataset caseh
content_caseh = pd.read_csv('source/content_imdb_casehlaptop.csv', engine='python')
#ids_content_caseh = content_caseh.iloc[:,1]
print("ids_content_caseh carregado")

# dataset accenture
content_accenture = pd.read_csv('source/content_imdb_maquina_accenture.csv', engine='python')
#ids_content_accenture = content_accenture.iloc[:,1]
print("ids_content_accenture carregado")

# dataset thiago
content_thiago = pd.read_csv('source/content_imdb_maquina_thiago.csv', engine='python')
#ids_content_thiago = content_thiago.iloc[:,1]
print("ids_content_thiago carregado")

# concatena todos os datasets e remove os duplicados
content_partial = pd.concat([content_aws, content_oracle, content_caseh, content_thiago, content_accenture])
content_partial.drop_duplicates(keep=False)

#ids_content_partial = pd.concat([ids_content_aws, ids_content_oracle, ids_content_caseh, ids_content_thiago, ids_content_accenture])
#ids_content_partial.drop_duplicates(keep=False)

print("concatena todos os datasets e remove os registros duplicados")

# grava os datasets concatenados em um único aquivo
content_partial.to_csv('source/content_partial.csv', index=False)
print("grava os os registors já processados no arquivo source/content_partial.csv")

# pega apenas os ids do arquivo parcial
ids_content_partial = content_partial.iloc[:,1]
ids_content_partial.to_csv('source/ids_content_partial.csv', index=False)
print("grava os os registors já processados no arquivo source/ids_content_partial.csv")

ids_imdb = pd.read_csv('source/ids_imdb.csv', engine='python')
print("ids_imdb carregado")

# Get the rows that are unique to the first dataset
df = ids_imdb.merge(ids_content_partial, how='outer', indicator=True).loc[lambda x:x['_merge'] == 'left_only']
print("lista os ids faltantes")

df.drop('_merge', axis='columns', inplace=True)
print("removendo coluna _merge")

df.to_csv('source/unique_to_id_imdb.csv', index=False)
print("grava os ids faltantes em source/unique_to_id_imdb.csv")

print(df)
