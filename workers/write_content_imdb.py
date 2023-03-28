from threading import Thread
from queue import Queue
import datetime
import mysql.connector as mysql
import sys


class WriteContentDb(object):
    def __init__(self, queue=Queue(256)):

        # define parametros de conexão ao banco de dados
        self.db = mysql.connect(
            host="localhost",
            user="root",
            password="abcd1234",
            database="super_studios"
        )
        # abre ponteiro para escrever no banco
        self.cursor = self.db.cursor()

        # flag de controle
        self.stopped = False
        self.dt = datetime.datetime.now()
        self.Q = queue

        # Start the thread to read frames from the video stream
        self.thread = Thread(target=self.write_to_db, args=())
        self.thread.daemon = True

    def start(self):
        # start a thread to read frames from the file video stream
        self.thread.start()
        return self

    def stop(self):
        # indicate that the thread should be stopped
        self.db.close()
        self.stopped = True

    def write_to_db(self):
        _dtn = datetime.datetime.now()
        _count_empty = 0
        while True:
            # if the thread indicator variable is set, stop the
            # thread
            if self.stopped and self.Q.empty:
                break

            #  percorre a fila para gravar os dados
            if self.Q.not_empty:
                result = self.Q.get()
                if result is not None:
                    print("Gravando: " + result['imdb_id'] + " tamanho fila: " + str(self.Q.qsize()))
                    insert_sql = "INSERT INTO content_imdb (imdb_id, title, release_year, director, creator, main_actors, countries, languages, companies, genre, type, runtime, release_date, description, content_rating, rating, rating_count, reviews_count, keywords, filming_location, aka, poster_url, trailer_url, trailer_download_url, trailer_thumbnail_url, distributors) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    insert_values = [result['imdb_id'], result['title'], result['release_year'], result['director'],
                                     result['creator'], ','.join(result['main_actors']), ','.join(result['countries']),
                                     ','.join(result['languages']), ','.join(result['companies']),
                                     ','.join(result['genre']),
                                     result['type'], result['runtime'], result['release_date'], result['description'],
                                     result['content_rating'], result['rating'], result['rating_count'],
                                     result['reviews_count'], ','.join(result['keywords']), result['filming_location'],
                                     result['aka'], result['poster_url'], result['trailer_url'],
                                     result['trailer_download_url'], result['trailer_thumbnail_url'],
                                     ','.join(result['distributors'])]
                    try:
                        self.cursor.execute(insert_sql, insert_values)
                        self.db.commit()
                        print("Gravando: " + result['imdb_id'] + " - ok")
                    except Exception:
                        print("Gravando: " + result['imdb_id'] + " - NOK! - ERRO motivo: " + str(sys.exc_info()))
