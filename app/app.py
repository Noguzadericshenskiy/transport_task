import os
import csv
import datetime
import sqlite3
import multiprocessing as mp
import pandas as pd

from threading import Lock
from typing import List, Tuple

from paths import path_DB, path_Matrix, path_Zone, name_dir_upload
from models import Matrix, Zones, session, engine, Base
from request_SQL import *


EXTENSION = {'csv'}


def drop_all_table():
    Base.metadata.drop_all(engine)


def creation_all_table():
    Base.metadata.create_all(engine)


def allowed_file(filename):
    """Проверка расширения файла"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in EXTENSION


def mkdir_check() -> None:
    """Проверка существования директории для сохранения файла"""
    if not os.path.isdir(name_dir_upload):
        os.mkdir(name_dir_upload)


def write_in_db(data, sql_str):
    with sqlite3.connect(path_DB) as conn:
        cur = conn.cursor()
        cur.executemany(sql_str, data)


def processing_chunk(chunk_data) -> None:
    list_data = []
    request_sql_add_data_matrix = """
    INSERT INTO 'matrix' 
    (ts, departure_zid, arrival_zid, customers_cnt, customers_cnt_metro) 
    VALUES (?, ?, ?, ?, ?);"""

    for row in chunk_data.itertuples(index=True):
        new_date = datetime.datetime.strptime(row.ts, '%Y.%m.%d %H:%M')
        record = (new_date, row.departure_zid, row.arrival_zid, row.customers_cnt, row.customers_cnt_metro)
        list_data.append(record)

    write_in_db(list_data, request_sql_add_data_matrix)


def processing_chunk_zone(chunk_data) -> None:
    request_sql_add_data_zone = """INSERT INTO 'zones' (zone, district) values(?, ?);"""
    list_data = []
    for row in chunk_data.itertuples(index=True):
        record = (row.Zone, row.District)
        list_data.append(record)

    write_in_db(list_data, request_sql_add_data_zone)


def read_file_Matrix() -> None:
    count = 0

    for chunk in pd.read_csv(path_Matrix, delimiter=";", encoding='utf8', chunksize=15000000):
        print(f"chunk{count}")
        processing_chunk(chunk)
        count += 1


def read_file_Zone() -> None:
    dataset = pd.read_csv(path_Zone, delimiter=";", encoding='utf8')
    processing_chunk_zone(dataset)


def get_data_fro_db(request: str) -> List[Tuple]:
    with sqlite3.connect(path_DB) as conn:
        cur = conn.cursor()
        cur.execute(request)
        records = cur.fetchall()
    return records


def get_max_load_sort_by_dz() -> None:
    list_max_dz = get_data_fro_db(GET_MAX_DZ)
    for line in list_max_dz:
        print(f"Зона {line[0]}  Время {line[1]}")


def get_max_load_city() -> None:
    list_max_city = get_data_fro_db(GET_MAX_CITY)
    for line in list_max_city:
        print(line)


if __name__ == "__main__":
    start = datetime.datetime.now()
    print('Start')
    mkdir_check()
    # drop_all_table()
    creation_all_table()
    read_file_Matrix()
    read_file_Zone()
    get_max_load_sort_by_dz()
    get_max_load_city()
    print('Время работы ', datetime.datetime.now() - start)
    print('ok')
