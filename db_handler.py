#!/usr/bin/env python2.7

from __future__ import print_function

import csv

from db_connection import connect

directory = "csv"
extension = ".csv"

csv_paths = {'heroes': "{}/heroes{}".format(directory, extension),
             'villains': "{}/villains{}".format(directory, extension),
             'masterminds': "{}/masterminds{}".format(directory, extension),
             'schemes': "{}/schemes{}".format(directory, extension)}

TABLES = {}

TABLES['Heroes'] = """
    CREATE TABLE `Heroes` (
     `Name` varchar(45) NOT NULL,
     `Team` varchar(45) NOT NULL,
     `CardSet` varchar(45) NOT NULL,
     PRIMARY KEY (`Name`, `CardSet`)
    ) ENGINE=InnoDB
    """

TABLES['Villains'] = """
    CREATE TABLE `Villains` (
     `Name` varchar(45) NOT NULL,
     `Mastermind` varchar(45),
     `IsHenchmanGroup` tinyint(2) NOT NULL,
     `CardSet` varchar(45) NOT NULL,
     PRIMARY KEY (`Name`, `CardSet`)
    ) ENGINE=InnoDB
    """

TABLES['Masterminds'] = """
    CREATE TABLE `Masterminds` (
     `Name` varchar(45) NOT NULL,
     `GroupLed` varchar(45) NOT NULL,
     `CardSet` varchar (45) NOT NULL,
     PRIMARY KEY (`Name`, `CardSet`)
    ) ENGINE=InnoDB
    """

TABLES['Schemes'] = """
    CREATE TABLE `Schemes` (
     `Name` varchar(45) NOT NULL,
     `RequiredVillainGroup` varchar(45),
     `ExtraVillainGroup` boolean NOT NULL,
     `ExtraHenchmanGroup` boolean NOT NULL,
     `CardSet` varchar(45) NOT NULL,
     PRIMARY KEY (`Name`, `CardSet`)
    ) ENGINE=InnoDB
    """


def create(table):
    if table == 'all':
        for _, path in csv_paths.iteritems():
            create_tables_file_opener(path)
    else:
        create_tables_file_opener(csv_paths[table])


def update(table):
    if table == 'all':
        for _, path in csv_paths.iteritems():
            update_tables_file_opener(path)
    else:
        update_tables_file_opener(csv_paths[table])


def drop(table):
    db_connection = connect()
    cursor = db_connection.cursor()

    sql = """
        DROP TABLE IF EXISTS {}
        """

    if table != 'all':

        print("Dropping table: {}".format(table.capitalize()))
        cursor.execute(sql.format(table.capitalize()))

    else:

        print("Dropping all tables")
        for key, value in TABLES.iteritems():
            cursor.execute(sql.format(key))

    db_connection.commit()
    db_connection.close()
    cursor.close()


def create_heroes(reader):
    db_connection = connect()
    cursor = db_connection.cursor()

    sql = """
        INSERT INTO Heroes
        (Name, Team, CardSet)
        VALUES (%s, %s, %s)
        """

    cursor.execute(TABLES['Heroes'])

    for hero in reader:
        parameters = (hero['Name'], hero['Team'], hero['CardSet'])

        print("Inserting {} into Heroes".format(hero['Name']))
        cursor.execute(sql, parameters)

    db_connection.commit()
    db_connection.close()
    cursor.close()


def update_heroes(reader):
    db_connection = connect()
    cursor = db_connection.cursor()

    sql = """
        UPDATE Heroes
        SET Name=%s, Team=%s, CardSet=%s
        WHERE Name=%s AND CardSet=%s
        """
    for hero in reader:
        parameters = (hero['Name'], hero['Team'], hero['CardSet'], hero['Name'], hero['CardSet'])

        print("Updating {} in Heroes".format(hero['Name']))
        cursor.execute(sql, parameters)

    db_connection.commit()
    db_connection.close()
    cursor.close()


def create_villains(reader):
    db_connection = connect()
    cursor = db_connection.cursor()

    sql = """
        INSERT INTO Villains
        (Name, Mastermind, IsHenchmanGroup, CardSet)
        VALUES (%s, %s, %s, %s)
        """

    cursor.execute(TABLES['Villains'])

    for villain_group in reader:
        parameters = (villain_group['Name'], villain_group['Mastermind'], villain_group['IsHenchmanGroup'],
                      villain_group['CardSet'])

        print("Inserting {} into Villains".format(villain_group['Name']))
        cursor.execute(sql, parameters)

    db_connection.commit()
    db_connection.close()
    cursor.close()


def update_villains(reader):
    db_connection = connect()
    cursor = db_connection.cursor()

    sql = """
        UPDATE Villains
        SET Name=%s, Mastermind=%s, IsHenchmanGroup=%s, CardSet=%s
        WHERE Name=%s AND CardSet=%s
        """

    for villain in reader:
        parameters = (villain['Name'], villain['Mastermind'], villain['IsHenchmanGroup'], villain['CardSet'],
                      villain['Name'], villain['CardSet'])

        print("Updating {} in Villains".format(villain['Name']))
        cursor.execute(sql, parameters)

    db_connection.commit()
    db_connection.close()
    cursor.close()


def create_masterminds(reader):
    db_connection = connect()
    cursor = db_connection.cursor()

    sql = """
        INSERT INTO Masterminds
        (Name, GroupLed, CardSet)
        VALUES (%s, %s, %s)
        """

    cursor.execute(TABLES['Masterminds'])

    for mastermind in reader:
        parameters = (mastermind['Name'], mastermind['GroupLed'], mastermind['CardSet'])

        print("Inserting {} into Masterminds".format(mastermind['Name']))
        cursor.execute(sql, parameters)

    db_connection.commit()
    db_connection.close()
    cursor.close()


def update_masterminds(reader):
    db_connection = connect()
    cursor = db_connection.cursor()

    sql = """
        UPDATE Masterminds
        SET Name=%s, GroupLed=%s, CardSet=%s
        WHERE Name=%s AND CardSet=%s
        """

    for mastermind in reader:
        parameters = (mastermind['Name'], mastermind['GroupLed'], mastermind['CardSet'], mastermind['Name'],
                      mastermind['CardSet'])

        print("Updating {} in Masterminds".format(mastermind['Name']))
        cursor.execute(sql, parameters)

    db_connection.commit()
    db_connection.close()
    cursor.close()


def create_schemes(reader):
    db_connection = connect()
    cursor = db_connection.cursor()

    sql = """
        INSERT INTO Schemes
        (Name, RequiredVillainGroup, ExtraVillainGroup, ExtraHenchmanGroup, CardSet)
        VALUES (%s, %s, %s, %s, %s)
        """

    cursor.execute(TABLES['Schemes'])

    for scheme in reader:
        parameters = (scheme['Name'], scheme['RequiredVillainGroup'], scheme['ExtraVillainGroup'],
                      scheme['ExtraHenchmanGroup'], scheme['CardSet'])

        print("Inserting {} into Schemes".format(scheme['Name']))
        cursor.execute(sql, parameters)

    db_connection.commit()
    db_connection.close()
    cursor.close()


def update_schemes(reader):
    db_connection = connect()
    cursor = db_connection.cursor()

    sql = """
        UPDATE Schemes
        SET Name=%s, RequiredVillainGroup=%s, ExtraVillainGroup=%s, ExtraHenchmanGroup=%s, CardSet=%s
        WHERE Name=%s AND CardSet=%s
        """

    for scheme in reader:
        parameters = (scheme['Name'], scheme['RequiredVillainGroup'], scheme['ExtraVillainGroup'],
                      scheme['ExtraHenchmanGroup'], scheme['CardSet'], scheme['Name'], scheme['CardSet'])

        print("Updating {} in Schemes".format(scheme['Name']))
        cursor.execute(sql, parameters)

    db_connection.commit()
    db_connection.close()
    cursor.close()


def create_tables_file_opener(path):
    with open(path) as csv_file:
        reader = csv.DictReader(csv_file)

        if path == csv_paths['heroes']:
            create_heroes(reader)
        elif path == csv_paths['villains']:
            create_villains(reader)
        elif path == csv_paths['masterminds']:
            create_masterminds(reader)
        elif path == csv_paths['schemes']:
            create_schemes(reader)

    csv_file.close()


def update_tables_file_opener(path):
    with open(path) as csv_file:
        reader = csv.DictReader(csv_file)

        if path == csv_paths['heroes']:
            update_heroes(reader)
        elif path == csv_paths['villains']:
            update_villains(reader)
        elif path == csv_paths['masterminds']:
            update_masterminds(reader)
        elif path == csv_paths['schemes']:
            update_schemes(reader)

    csv_file.close()
