#!/usr/bin/python3

import sys
import psycopg2
import time
import numpy as np

from config import *
from logger import *
import index_utils as utils

def init_tables(con, cur, table_name, logger):
    # drop old table
    query_clear = "DROP TABLE IF EXISTS " + table_name + ";"
    result = cur.execute(query_clear)
    con.commit()
    logger.log(Logger.INFO, 'Exexuted DROP TABLE on ' + table_name)

    # create table
    query_create_table = "CREATE TABLE " + table_name + " (id serial PRIMARY KEY, word varchar(100), vector bytea);"

    result = cur.execute(cur.mogrify(query_create_table, (table_name,)))
    # commit changes
    con.commit()
    logger.log(Logger.INFO, 'Created new table ' + table_name)

    return

def float_to_bitvec(vector):
    maxbits_per_num = 64
    nums = len(vector) // maxbits_per_num + 1
    bitvec = np.zeros(nums, dtype=np.uint64)
    for index in range(0, len(vector) - 1):
        num = int(index / maxbits_per_num)
        bitvec[num] *= np.uint64(2)
        bitvec[num] += np.uint64(1) if (float(vector[index]) > 0) else np.uint64(0)
    # print(bitvec)
    return psycopg2.Binary(bitvec)

def insert_vectors(filename, con, cur, table_name, batch_size, logger):
    f = open(filename)
    (_, size) = f.readline().split()
    d = int(size)
    count = 1
    line = f.readline()
    values = []
    while line:
        splits = line.split()
        # print(splits[:1])
        bitvec = float_to_bitvec(splits[1:])
        if (len(splits[0]) < 100) and (bitvec != None) and (len(splits) == (d + 1)):
            values.append({"word": splits[0], "bitvec": bitvec})
        else:
            logger.log(Logger.WARNING, 'parsing problem with ' + line)
            count -= 1
        if count % batch_size == 0:
            cur.executemany("INSERT INTO "+ table_name + " (word,vector) VALUES (%(word)s, %(bitvec)s)", tuple(values))
            con.commit()
            logger.log(Logger.INFO, 'Inserted ' + str(count-1) + ' vectors')
            values = []

        count+= 1
        line = f.readline()

    cur.executemany("INSERT INTO "+ table_name + " (word,vector) VALUES (%(word)s, %(bitvec)s)", tuple(values))
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

    # init db connection
    try:        
        con = psycopg2.connect(args)
    except:
        logger.log(Logger.ERROR, 'Can not connect to database')
        return

    cur = con.cursor()

    init_tables(con, cur, vec_config.get_value('table_name'), logger)

    insert_vectors(vec_config.get_value('vec_file_path'), con, cur, vec_config.get_value('table_name'), db_config.get_value('batch_size'), logger)

    # commit changes
    con.commit()

    # create index
    utils.create_index(vec_config.get_value('table_name'), vec_config.get_value('index_name'), 'word', con, cur, logger)

    # close connection
    con.close()

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
