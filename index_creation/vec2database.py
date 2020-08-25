#!/usr/bin/python3

import sys
import psycopg2
import time
import numpy as np

from config import *
from logger import *
import index_utils as utils

USE_BYTEA_TYPE = True

def init_tables(con, cur, table_name, logger):
    # drop old table
    query_clear = "DROP TABLE IF EXISTS " + table_name + ";"
    result = cur.execute(query_clear)
    con.commit()
    logger.log(Logger.INFO, 'Exexuted DROP TABLE on ' + table_name)

    # create table
    query_create_table = None
    if USE_BYTEA_TYPE:
        query_create_table = "CREATE TABLE " + table_name + " (id serial PRIMARY KEY, word varchar(100), vector bytea);"
    else:
        query_create_table = "CREATE TABLE " + table_name + " (id serial PRIMARY KEY, word varchar(100), vector float4[]);"
    result = cur.execute(cur.mogrify(query_create_table, (table_name,)))
    # commit changes
    con.commit()
    logger.log(Logger.INFO, 'Created new table ' + table_name)

    return

def serialize_array(array):
    output = '{'
    for elem in array:
        try:
            val = float(elem)
        except:
            return None
        output += str(elem) + ','
    return output[:-1] + '}'

def serialize_as_norm_array(array):
    vector = []
    for elem in array:
        try:
            vector.append(float(elem))
        except:
            return None
    output = '{'
    length = np.linalg.norm(vector)
    for elem in vector:
        output += str(elem / length) + ','
    return output[:-1] + '}'

def insert_vectors(filename, con, cur, table_name, batch_size, insert_limit, normalized, logger):
    f = open(filename, encoding='utf-8')
    (_, size) = f.readline().split()
    d = int(size)
    count = 1
    line = f.readline()
    values = []
    while line:
        splits = line.split()
        words = [x for x in splits if '0.33333' not in x]
        word = ' '.join(words)
        vector = [x for x in splits if x not in words]
        vec_len = len(vector)
        if normalized:
            vector = serialize_as_norm_array(vector)
        else:
            vector = serialize_array(vector)
        if (len(word.encode('utf-8')) < 100) and (vector != None) and (vec_len == d):
            values.append({"word": splits[0], "vector": vector})
            if count % batch_size == 0:
                if USE_BYTEA_TYPE:
                    cur.executemany("INSERT INTO "+ table_name + " (word,vector) VALUES (%(word)s, vec_to_bytea(%(vector)s::float4[]))", tuple(values))
                else:
                    cur.executemany("INSERT INTO "+ table_name + " (word,vector) VALUES (%(word)s, %(vector)s)", tuple(values))
                con.commit()
                logger.log(Logger.INFO, 'Inserted ' + str(count-1) + ' vectors')
                values = []
            count+= 1
        else:
            logger.log(Logger.WARNING, 'parsing problem with ' + line)
        if count-1 == insert_limit:
            break

        line = f.readline()

    if USE_BYTEA_TYPE:
        cur.executemany("INSERT INTO "+ table_name + " (word,vector) VALUES (%(word)s, vec_to_bytea(%(vector)s::float4[]))", tuple(values))
    else:
        cur.executemany("INSERT INTO "+ table_name + " (word,vector) VALUES (%(word)s, %(vector)s)", tuple(values))
    con.commit()
    logger.log(Logger.INFO, 'Inserted ' + str(count-1) + ' vectors')
    values = []

    return

def main(argc, argv):

    db_config = Configuration('config/db_config.json')
    logger = Logger(db_config.get_value('log'))

    if argc < 2:
        logger.log(Logger.ERROR, 'Configuration file for index creation required')
        return
    vec_config = Configuration(argv[1])

    user = db_config.get_value('username')
    password = db_config.get_value('password')
    host = db_config.get_value('host')
    db_name = db_config.get_value('db_name')
    port = db_config.get_value('port')

    args = "dbname='" + db_name + "' user='" + user + "' host='" + host + "' password='" + password + "'"
    if port != "":
        args = args + " port='" + port + "'"

    insert_limit = vec_config.get_value('insert_limit') if vec_config.has_key('insert_limit') else -1

    # init db connection
    try:        
        con = psycopg2.connect(args)
    except:
        logger.log(Logger.ERROR, 'Can not connect to database')
        return

    con.set_client_encoding('UTF8')
    cur = con.cursor()

    init_tables(con, cur, vec_config.get_value('table_name'), logger)

    insert_vectors(vec_config.get_value('vec_file_path'), con, cur, vec_config.get_value('table_name'), db_config.get_value('batch_size'), insert_limit, vec_config.get_value('normalized'), logger)

    # commit changes
    con.commit()

    # create index
    utils.create_index(vec_config.get_value('table_name'), vec_config.get_value('index_name'), 'word', con, cur, logger)

    # close connection
    con.close()

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
