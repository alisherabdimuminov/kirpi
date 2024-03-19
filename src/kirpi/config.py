from configparser import ConfigParser


config_file: str = ".config"

config: ConfigParser = ConfigParser()
config.read(filenames=config_file)
configs: dict = {}
try:
    configs = dict(config.items(section="DATABASE"))
except:
    pass

DB_HOST = configs.get("db_host")
DB_PORT = configs.get("db_port") 
DB_NAME = configs.get("db_name")
DB_USER = configs.get("db_user")
DB_PASSWORD = configs.get("db_password")

DNS = "host={} port={} dbname={} user={} password={}".format(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
