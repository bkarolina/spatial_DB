import gc, os, zipfile
from glob import glob
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
import sqlite3
from sqlite3 import Error
import csv
from pyproj import Transformer

def preparing_csv(path, dataset):
    way = path + '/' + dataset
    os.chdir(way) # change directory from working dir to dir with files
    items = os.listdir(way)
    print(items)
    for item in items: # loop through items in dir
        file_name = os.path.abspath(item)
        print("Preparing file", item)
        name = item.lower().split('.')[0].split('_')[0]
        df = pd.read_csv(file_name)
        year = name[-4:]
        df['year'] = year
        if 'E_KOORD' not in df: 
            transformer = Transformer.from_crs('epsg:21781', 'epsg:2056', always_xy=True) 
            points = list(zip(df.X_KOORD, df.Y_KOORD))
            coordsWgs = np.array(list(transformer.itransform(points)))
            df.insert(loc=3, column='E_KOORD', value=coordsWgs[:,0])
            df.insert(loc=4, column='N_KOORD', value=coordsWgs[:,1])
        for col in ['ERHJAHR', 'PUBJAHR']:
            if col in df:
                df.drop(col, axis=1, inplace=True)
        if (dataset == 'statent' and int(year) < 2014):
            cols = df.columns
            df_tmp = df[['RELI','E_KOORD','N_KOORD','X_KOORD','Y_KOORD']]
            df.drop(['RELI','E_KOORD','N_KOORD','X_KOORD','Y_KOORD'], axis=1, inplace=True)
            df.insert(loc=0,column='E_KOORD',value=df_tmp['E_KOORD'].values)
            df.insert(loc=1,column='N_KOORD',value=df_tmp['N_KOORD'].values)
            df.insert(loc=2,column='X_KOORD',value=df_tmp['X_KOORD'].values)
            df.insert(loc=3,column='Y_KOORD',value=df_tmp['Y_KOORD'].values)
            df.insert(loc=4,column='RELI',value=df_tmp['RELI'].values)
            del df_tmp
        df.to_csv(way + '/%s.csv' %name, index=False)
        print(name,"is saved and ready to be written to database.")
        os.remove(file_name)
        del df
    print("All files processed.")    

def extracting(path, keep_pattern):
    extension = ".zip"
    os.chdir(path) # change directory from working dir to dir with files

    for item in os.listdir(path): # loop through items in dir
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            zip_ref = zipfile.ZipFile(file_name) # create zipfile object
            zip_ref.extractall(path) # extract file to dir
            zip_ref.close() # close file
            os.remove(file_name) # delete zipped file

    files = []
    files_keep = []

    for name in glob(path + "/*"): # list of directory
        files.append(name)

    for name in glob(path + keep_pattern): # remember files you want to keep
        files_keep.append(name)

    for item in list(set(files)-set(files_keep)): # remove unnecessary files
        file_name = os.path.abspath(item)
        os.remove(file_name)

def connect_to_db(db_file):
    """
    Connect to an SQlite database, if db file does not exist it will be created
    :param db_file: absolute or relative path of db file
    :return: sqlite3 connection
    """
    sqlite3_conn = None

    try:
        sqlite3_conn = sqlite3.connect(db_file)
        return sqlite3_conn

    except Error as err:
        print(err)

        if sqlite3_conn is not None:
            sqlite3_conn.close()

def insert_values_to_table(table_name, csv_file_path, creation, DB_FILE_PATH):
    """
    Open a csv file, store its content in a list excluding header and insert the data from the list to db table
    :param table_name: table name in the database to insert the data into
    :param csv_file_path: path of the csv file to process
    :return: None
    """

    conn = connect_to_db(DB_FILE_PATH)

    if conn is not None:
        c = conn.cursor()
        
        # Create table if it is not exist
        c.execute(creation)

        # Read CSV file content
        values_to_insert = open_csv_file(csv_file_path)

        # Insert to table
        if len(values_to_insert) > 0:
            column_names, column_numbers = get_column_names_from_db_table(c, table_name)

            values_str = '?,' * column_numbers
            values_str = values_str[:-1]

            sql_query = 'INSERT INTO ' + table_name + '(' + column_names + ') VALUES (' + values_str + ')'
            
            c.executemany(sql_query, values_to_insert)
            conn.commit()

            print('SQL insert process finished')
        else:
            print('Nothing to insert')

        conn.close()

    else:
        print('Connection to database failed')

def open_csv_file(csv_file_path):
    """
    Open and read data from a csv file without headers (skipping the first row)
    :param csv_file_path: path of the csv file to process
    :return: a list with the csv content
    """
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        reader = csv.reader(csv_file)
        next(reader)

        data = list()
        for row in reader:
            data.append(row)

        return data


def get_column_names_from_db_table(sql_cursor, table_name):
    """
    Scrape the column names from a database table to a list and convert to a comma separated string, count the number
    of columns in a database table
    :param sql_cursor: sqlite cursor
    :param table_name: table name to get the column names from
    :return: a comma separated string with column names, an integer with number of columns
    """

    table_column_names = 'PRAGMA table_info(' + table_name + ');'
    sql_cursor.execute(table_column_names)
    table_column_names = sql_cursor.fetchall()

    column_count = len(table_column_names)

    column_names = list()

    for name in table_column_names:
        column_names.append(name[1])

    return ', '.join(column_names), column_count


def create_table(table_name, DB_FILE_PATH):
    """ Derives tables from existing one with a geometry column in LV95 format.
    """
    conn = connect_to_db(DB_FILE_PATH)

    if conn is not None:
        c = conn.cursor()
        
        # Create table if it is not exist
        #c.execute(creation)

        print("Connected to database and creating tables with geometry column")
         
        new_table = 'g' + table_name
         
        column_names, column_numbers = get_column_names_from_db_table(c, table_name)

        values_str = '?,' * column_numbers
        values_str = values_str[:-1] 

        # Insert to table
        sql_enable_ex = "SELECT load_extension('mod_spatialite')"
        sql_geometry = "AddGeometryColumn(" + new_table + ", 'geometryLV95', 2056, 'POINT', 'XY')"
        sql_query = 'INSERT INTO ' + new_table + '(' + column_names + ' , geometryLV95) SELECT (' \
                     + column_names + 'MakePoint(E_KOORD, N_KOORD, 2056)) FROM ' + table_name 
        
        c.execute(sql_enable_ex)
        c.execute(sql_geometry)   
        c.execute(sql_query)
        conn.commit()
        print('SQL insert process with geometry column finished')
     
        conn.close()

    else:
        print('Connection to database failed')


def init_spatialite(DB_FILE_PATH):
    conn = connect_to_db(DB_FILE_PATH)

    if conn is not None:
        c = conn.cursor()
       # c.enable_load_extension(True)
       # try:
        #    c.load_extension('mod_spatialite.so')
       # except sqlite3.OperationalError:
        #    c.load_extension('libspatialite.so')
        sql_enable_ex = "SELECT load_extension('mod_spatialite')"
        sql_init_meta = "SELECT InitSpatialMetaData()"
        c.execute(sql_enable_ex)
        c.execute(sql_init_meta)
        conn.commit()

        print('Init Spatial MetaData process finished')

        conn.close()

    else:
        print('Connection to database failed')

