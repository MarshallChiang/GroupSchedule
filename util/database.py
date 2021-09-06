import pymysql
import json
import os

CONFIG_PATH = os.path.dirname(os.path.abspath(__file__)) + '/config.json'

class sql_singleton(object) :
    '''
    init pymysql object when its not existed or broken connection, otherwise return the object with current connection.
    '''
    def __new__(self) :

        db_conn = lambda config : pymysql.connect(**config)

        if not hasattr(self, "instance") : 
            with open(CONFIG_PATH) as f :
                self.instance = db_conn(json.loads(f.read())['database'])
                print("Create a new connection to database.")

        elif not self.instance.open :
            with open(CONFIG_PATH) as f :
                self.instance = db_conn(json.loads(f.read())['database'])
                print("Database has been reconnected.")

        return self.instance

