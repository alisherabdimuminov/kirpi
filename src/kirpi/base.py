import psycopg
from psycopg import sql

from kirpi import DNS

class DataBase:
    def __init__(self):
        self.context = None
        try:
            self.context = psycopg.connect(DNS)
        except:
            print("configurations is invalid, please check .config file")
            exit()