from threading import Thread
from queue import Queue
import datetime
import mysql.connector as mysql
import sys


class UpdateContentDb(object):
    def __init__(self, queue=Queue(256)):

        # define parametros de conexão ao banco de dados
        self.db = mysql.connect(
            host="localhost",
            user="root",
            password="abc123",
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
                    if result[1] != "" or result[2] != "":
                        print("Update: " + result[0] + " tamanho fila: " + str(self.Q.qsize()))
                        insert_sql = "UPDATE content_imdb_thiago SET budget = %s, gross_world = %s WHERE title_id = %s"
                        insert_values = [result[1], result[2], result[0]]
                        try:
                            self.cursor.execute(insert_sql, insert_values)
                            self.db.commit()
                            print("Update: " + result[0] + " - ok")
                        except Exception:
                            print("Update: " + result[0] + " - NOK! - ERRO motivo: " + str(sys.exc_info()))
