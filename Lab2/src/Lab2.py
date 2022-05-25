import csv
import psycopg2
from config import config
import time
import os

FILENAME_21 = "Odata2021File.csv"
FILENAME_19 = "Odata2019File.csv"
HEADERS_21 = ['OUTID', 'RegName', 'MathTest', 'MathTestStatus', 'MathBall100']
HEADERS_19 = ['OUTID', 'REGNAME', 'mathTest', 'mathTestStatus', 'mathBall100']
COLUMN_TYPES = ['varchar', 'varchar', 'varchar', 'varchar', 'varchar']
TABLE_NAME = 'ZNO_results'
ADDITIONAL_TABLE_NAME_21 = "indexes_21"
ADDITIONAL_TABLE_NAME_19 = "indexes_19"
BULK_SIZE = 20000


def create_tables(header, table_name, additional_table_name, conn, cur):
    """ create tables in the PostgreSQL database"""
    query_set = []
    main_query = ' CREATE TABLE IF NOT EXISTS ' + table_name
    columns_specification = ' ('
    n = len(header)
    for idx, item in enumerate(header):
        tmp = item.replace(' ', '_') + ' ' + COLUMN_TYPES[idx]
        if idx == 0:
            tmp += " primary key"
        if idx != n - 1:
            tmp += ', '
        columns_specification += tmp
    columns_specification += ', Year int);'
    main_query += columns_specification
    idx_query = 'CREATE TABLE IF NOT EXISTS ' + additional_table_name + ''' (
        index_id int ,
        list_length int
        )
    '''
    query_set.append(main_query)
    query_set.append(idx_query)
    try:
        for sql_query in query_set:
            cur.execute(sql_query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        raise Exception(error)


def insert_query_sql_for_year(columns_list, table_name, year):
    sql = "INSERT INTO " + table_name
    columns_statement = " ("
    vals_statement = " ("
    N = len(columns_list)
    for i in range(N):
        tmp = columns_list[i]
        if i != N - 1:
            tmp += ', '
        columns_statement += tmp
        tmp = '%s'
        if i != N - 1:
            tmp += ', '
        vals_statement += tmp
    columns_statement += ', Year'
    vals_statement += ', ' + str(year)
    columns_statement += ') VALUES' + vals_statement + ')'
    sql += columns_statement
    return sql


def info_insert_for_year(header, headers, bulks, index, list_length, table_name, additional_table_name, year, conn,
                         cur):
    start_time = time.time()
    last_index = list_length // BULK_SIZE + 1
    while index != last_index:
        data_to_insert = bulks[index]
        for item in data_to_insert:
            sql = insert_query_sql_for_year(headers, table_name, year)
            values = get_values_list(header, item, headers)
            try:
                cur.execute(sql, values)
            except (Exception, psycopg2.DatabaseError) as error:
                raise Exception(error)
        index += 1
        print("{}/{} has been written".format(index, last_index))
        cur.execute('UPDATE ' + additional_table_name + ' SET index_id = ' + str(index) + ' WHERE index_id is not NULL')
        conn.commit()
    return time.time() - start_time


def process_csv_and_save_in_db_for_year(headers, table_name, additional_table_name, filename, encoding, year):
    create_tables(headers, table_name, additional_table_name, conn, cur)
    header, rdr = read_csv(filename, encoding)
    index = get_index(cur, additional_table_name)
    list_length = get_list_length(cur, additional_table_name, filename, encoding)
    bulks = bulk_container(BULK_SIZE, list_length, rdr)
    estimated_time = info_insert_for_year(header, headers, bulks, index, list_length, table_name, additional_table_name, year,
                                 conn, cur)

    return estimated_time


def read_csv(filename, encoding):
    file = open(filename, encoding=encoding)
    csvreader = csv.reader(file, delimiter='\n')
    header = next(csvreader)
    return header[0].replace('"', '').split(';'), csvreader


def get_values_list(header, record, columns_list):
    values = []
    m = header.index(columns_list[-1])

    if m >= len(record):
        return None
    else:
        for column in columns_list:
            m = header.index(column)
            values.append(record[m])
    values = tuple(values)

    return values


def get_bulk(reader, pack):
    obj_ins = []
    for idx, obj in enumerate(reader):
        obj_ins.append(obj[0].replace('"', '').split(';'))
        if idx == pack - 1:
            break
    return obj_ins


def csv_lines_count(filename, encoding):
    file = open(filename, encoding=encoding)
    file_object = csv.reader(file)
    next(file_object)
    row_count = sum(1 for _ in file_object)
    return row_count - 1


def db_row_counts(tbl_name, conn, cur):
    query = 'SELECT COUNT(*) FROM ' + tbl_name
    try:
        cur.execute(query)
        rowcount = cur.fetchone()[0]
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        raise Exception(error)

    return rowcount


def bulk_container(pack, file_size, reader):
    bulk_set = []
    for idx in range((file_size - 1) // pack + 1):
        if idx == file_size // pack:
            bulk_set.append(get_bulk(reader, file_size - pack * (file_size // pack)))
        else:
            bulk_set.append(get_bulk(reader, pack))

    return bulk_set


def insert_query_sql(columns_list, table_name):
    sql = "INSERT INTO " + table_name
    columns_statement = " ("
    vals_statement = " ("
    N = len(columns_list)
    for i in range(N):
        tmp = columns_list[i]
        if i != N - 1:
            tmp += ', '
        columns_statement += tmp
        tmp = '%s'
        if i != N - 1:
            tmp += ', '
        vals_statement += tmp
    columns_statement += ') VALUES' + vals_statement + ')'
    sql += columns_statement
    return sql


def db_connection():
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    return conn, cur


def get_index(cur, additional_table_name):
    query = 'SELECT index_id FROM ' + additional_table_name
    try:
        cur.execute(query)
        val = cur.fetchall()[0][0]
    except (IndexError, psycopg2.DatabaseError):
        cur.execute('INSERT INTO ' + additional_table_name + ' (index_id) VALUES (%s)', (0,))
        conn.commit()
        val = 0
    except (Exception, psycopg2.DatabaseError) as error:
        raise Exception(error)

    return val


def get_list_length(cur, additional_table_name, filename, encoding):
    query = 'SELECT list_length FROM ' + additional_table_name
    try:
        cur.execute(query)
        list_len = cur.fetchall()[0][0]
        if list_len is None:
            list_len = csv_lines_count(filename, encoding)
            cur.execute('UPDATE ' + additional_table_name + ' SET list_length = ' + str(
                list_len) + ' WHERE index_id is not NULL')
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        raise Exception(error)

    return list_len


def info_insert(header, headers, bulks, index, list_length, table_name, additional_table_name, conn, cur):
    start_time = time.time()
    last_index = list_length // BULK_SIZE + 1
    while index != last_index:
        data_to_insert = bulks[index]
        for item in data_to_insert:
            sql = insert_query_sql(headers, table_name)
            values = get_values_list(header, item, headers)
            try:
                cur.execute(sql, values)
            except (Exception, psycopg2.DatabaseError) as error:
                raise Exception(error)
        index += 1
        print("{}/{} has been written".format(index, last_index))
        cur.execute('UPDATE ' + additional_table_name + ' SET index_id = ' + str(index) + ' WHERE index_id is not NULL')
        conn.commit()
    return time.time() - start_time


def process_csv_and_save_in_db(headers, table_name, additional_table_name, filename, encoding):
    create_tables(headers, table_name, additional_table_name, conn, cur)
    header, rdr = read_csv(filename, encoding)
    index = get_index(cur, additional_table_name)
    list_length = get_list_length(cur, additional_table_name, filename, encoding)
    bulks = bulk_container(BULK_SIZE, list_length, rdr)
    estimated_time = info_insert(header, headers, bulks, index, list_length, table_name, additional_table_name,
                                 conn, cur)

    return estimated_time


query = '''
SELECT regname2019 AS regname, math19, math21
FROM (SELECT regname AS regname2019, MAX(mathball100) AS math19 FROM zno_results
   WHERE mathteststatus = 'Зараховано' AND year = 2019
   GROUP BY regname) AS zno_19,
  (SELECT regname AS regname2021, MAX(mathball100) AS math21 FROM zno_results
   WHERE mathteststatus = 'Зараховано' AND year = 2021
   GROUP BY regname) AS zno_21
WHERE regname2019 = regname2021
'''


TABLES = ['result', 'student', 'year']
if __name__ == '__main__':
    start_time = time.time()
    print('Connecting database...')
    conn, cur = db_connection()
    print("Database connected.")

    # print('Student (21) table status:\n')
    # time_set_21 = process_csv_and_save_in_db(['OUTID', 'Birth', 'SexTypeName', 'RegName', 'TERNAME'], 'student',
    #                                          'student_additional_21', 'Odata2021File.csv', 'utf-8-sig')
    #
    # with open('time.txt', 'a+') as f:
    #     f.write('Students (21) set lead time - {}\n'.format(time_set_21))
    #
    # print('Student (19) table status:\n')
    # time_set_19 = process_csv_and_save_in_db(['OUTID', 'Birth', 'SEXTYPENAME', 'REGNAME', 'TERNAME'], 'student',
    #                                          'student_additional_19', 'Odata2019File.csv', 'cp1251')
    # with open('time.txt', 'a+') as f:
    #     f.write('Students (19) set lead time - {}\n'.format(time_set_19))
    #
    # print('Results (21) table status:\n')
    # time_set_21 = process_csv_and_save_in_db(['OUTID', 'MathTest', 'MathTestStatus', 'MathBall100'], 'result',
    #                                          'result_additional_21', 'Odata2021File.csv', 'utf-8-sig')
    # with open('time.txt', 'a+') as f:
    #     f.write('Result (21) set lead time - {}\n'.format(time_set_21))
    #
    # print('Results (19) table status:\n')
    # time_set_19 = process_csv_and_save_in_db(['OUTID', 'mathTest', 'mathTestStatus', 'mathBall100'], 'result',
    #                                          'result_additional_19', 'Odata2019File.csv', 'cp1251')
    # with open('time.txt', 'a+') as f:
    #     f.write('Result (19) set lead time - {}\n'.format(time_set_19))
    #
    # print('Year (21) table status:\n')
    # time_set_21 = process_csv_and_save_in_db_for_year(['OUTID'], 'year',
    #                                                   'year_additional_21', 'Odata2021File.csv', 'utf-8-sig', 2021)
    # with open('time.txt', 'a+') as f:
    #     f.write('Result (21) set lead time - {}\n'.format(time_set_21))
    #
    # print('Year (19) table status:\n')
    # time_set_19 = process_csv_and_save_in_db_for_year(['OUTID'], 'year',
    #                                                   'year_additional_19', 'Odata2019File.csv', 'cp1251', 2019)
    # with open('time.txt', 'a+') as f:
    #     f.write('Result (19) set lead time - {}\n'.format(time_set_19))

    for idx, tablename in enumerate(TABLES):
        os.system(f'pg_dump --column-inserts --data-only --table={tablename} '
                  f'-h localhost -p 5432 -U admin -F p db_postgres_lab2 > flyway/sql/V2.{idx + 1}__insert_into_{tablename}.sql')
    # cur.execute(query)
    # with open('zno_results.csv', 'a+', newline='', encoding='utf8') as file:
    #     writer = csv.writer(file, delimiter=';')
    #     writer.writerow([hdr[0] for hdr in cur.description])
    #
    #     for row in cur:
    #         writer.writerow([str(item) for item in row])
    cur.close()
    conn.close()
    print(time.time() - start_time)
