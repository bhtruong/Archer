#!/usr/bin/env python2.7

import mysql.connector
from mysql.connector import errorcode
from config import config


def connect():
    try:
        db_connection = mysql.connector.connect(**config)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_SPECIFIC_ACCESS_DENIED_ERROR:
            print("Access Denied")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database Error")
        else:
            print(err)

    db_connection.database = config['database']

    return db_connection

if __name__ == '__main__':
    connect()
