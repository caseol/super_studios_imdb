from datetime import datetime
import mysql.connector as mysql
import sys

# cria conexão com o banco origem (super_studios = db_v1)
db_v1 = mysql.connect(
    host="localhost",
    user="root",
    password="abcd1234",
    database="super_studios"
)
cursor_v1 = db_v1.cursor(dictionary=True)

# cria conexão com o banco destino (super_studios_2 = db_v2)
db_v2 = mysql.connect(
    host="localhost",
    user="root",
    password="abcd1234",
    database="super_studios_2"
)
cursor_v2 = db_v2.cursor(dictionary=True, buffered=True)


def split_and_clean(string):
    list_result = []
    # verifica se a string não está vazia
    if string is not None and len(string) > 0:
        # faz o split e elimina espaços em branco
        list_result = [s.strip() for s in string.split(',')]

    return list_result


def create_or_update_model_and_relation(_list, _content_id, _table):
    for item in _list:
        cursor_v2.execute("select id, name from " + _table + " where name = %s", (item,))
        if cursor_v2.rowcount > 0:
            # item já existe pega ID para criar relacionamento
            result = cursor_v2.fetchone()
            item_id = int(result['id'])
        else:
            # item não existe na tabela cria um novo
            cursor_v2.execute("insert into " + _table + " (name) values (%s)", [item])
            item_id = cursor_v2.lastrowid
            db_v2.commit()
        # inclui o content_id e item na tabela de relacionamento propria
        cursor_v2.execute("insert into content_" + _table + " (content_id, " + _table + "_id) values (%s, %s)", [content_id, item_id])
        db_v2.commit()


# prepara para buscar todos os registros na tabela origem db_v1
sql_select_v1 = "select * from super_studios.content_imdb"
cursor_v1.execute(sql_select_v1)

# print("Total de registros em super_studios.content_imdb: " + str(cursor_v1.rowcount))
print("QUERY EXECUTADA!")

# percorre todos os registros do banco origem db_v1 e separa os registros no banco destino db_v2
resultset_v1 = cursor_v1.fetchall()
for row in resultset_v1:

    print("[INICIO][" + "{:%Y-%m-%d %H:%M:%S}".format(datetime.now()) + "][PROCESSANDO] ID: " + str(row['id']) + " - IMDB_ID: " + row['imdb_id'] + " - TITLE: " + row['title'])

    # pega o id do conteudo
    content_id = row['id']
    # pega infos do director e faz o split
    list_directors = split_and_clean(row['director'])
    create_or_update_model_and_relation(list_directors, content_id, 'director')

    # pega infos do genre (gênero) e faz o split
    list_genres = split_and_clean(row['genre'])
    create_or_update_model_and_relation(list_genres, content_id, 'genre')

    # pega infos do creator e faz o split
    list_creator = split_and_clean(row['creator'])
    create_or_update_model_and_relation(list_creator, content_id, 'creator')

    # pega infos de actor e faz o split
    list_actor = split_and_clean(row['main_actors'])
    create_or_update_model_and_relation(list_actor, content_id, 'actor')

    # pega infos de country e faz o split
    list_country = split_and_clean(row['countries'])
    create_or_update_model_and_relation(list_country, content_id, 'country')

    # pega infos de language e faz o split
    list_language = split_and_clean(row['languages'])
    create_or_update_model_and_relation(list_language, content_id, 'language')

    # pega infos de company e faz o split
    list_company = split_and_clean(row['companies'])
    create_or_update_model_and_relation(list_company, content_id, 'company')

    # pega infos de keyword e faz o split
    list_keyword = split_and_clean(row['keywords'])
    create_or_update_model_and_relation(list_keyword, content_id, 'keyword')

    # pega infos de film_location e faz o split
    list_film_location = split_and_clean(row['filming_location'])
    create_or_update_model_and_relation(list_film_location, content_id, 'film_location')

    # pega infos de distributor e faz o split
    list_distributor = split_and_clean(row['distributors'])
    create_or_update_model_and_relation(list_distributor, content_id, 'distributor')

    print("[FIM][" + "{:%Y-%m-%d %H:%M:%S}".format(datetime.now()) + "][PROCESSADO] ID: " + str(row['id']) + " - IMDB_ID: " + row['imdb_id'] + " - TITLE: " + row['title'])




