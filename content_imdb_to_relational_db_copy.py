import datetime
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
        cursor_v2.execute("insert into content_"+ _table +" (content_id, "+ _table +"_id) values (%s, %s)", [content_id, item_id])
        db_v2.commit()


# prepara para buscar todos os registros na tabela origem db_v1
sql_select_v1 = "select * from super_studios.content_imdb limit 10"
cursor_v1.execute(sql_select_v1)

print("Total de registros em super_studios.content_imdb: " + str(cursor_v1.rowcount))

# percorre todos os registros do banco origem db_v1 e separa os registros no banco destino db_v2
resultset_v1 = cursor_v1.fetchall()
for row in resultset_v1:
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

    # pega infos do creator e faz o split
    #list_creator = split_and_clean(row['creator'])
    #create_or_update_model_and_relation(list_creator, content_id, 'creator')

