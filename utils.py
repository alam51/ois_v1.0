from calendar import monthrange
import mysql.connector
import pandas as pd

CONNECTOR = mysql.connector.connect(
            host='127.0.0.1',
            user="root",
            password="pgcb1234",
            database="ois_2022_04_06",
)
