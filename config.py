import os
import ast

DATA_DIR = os.environ["DATA_DIR"]
db_tg = os.environ["DB_TG"]
db_users = os.environ["DB_USERS"]
db_schedules = os.environ["DB_SCHEDULES"]
folder = os.environ["FOLDER"]
foldercheck = os.environ["FOLDERCHECK"]
name_table = ast.literal_eval(os.environ["NAME_TABLE"])
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = ast.literal_eval(os.environ["ADMIN_ID"])
