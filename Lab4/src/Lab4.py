from pymongo import MongoClient
from csv import reader, writer
import time
import os


HEADERS_21 = ['OUTID', 'RegName', 'MathTest', 'MathTestStatus', 'MathBall100']
HEADERS_19 = ['OUTID', 'REGNAME', 'mathTest', 'mathTestStatus', 'mathBall100']
MONGODB_URL = os.environ.get('MONGODB_URL')
# client = MongoClient('mongodb://user:password@mongodb')
# client = MongoClient("mongodb+srv://analytics:analytics-password@analytics.6onwml2.mongodb.net/?retryWrites=true&w=majority")
client = MongoClient(MONGODB_URL)

db = client.zno
collection = db.students


def string_parser(string_value):
    if string_value == "null":
        return None
    try:
        return float(string_value.replace(",", "."))
    except Exception as err:
        return string_value


def csv_lines_count(filename, encoding):
    file = open(filename, encoding=encoding)
    file_object = reader(file)
    next(file_object)
    row_count = sum(1 for _ in file_object)
    return row_count - 1


def initialize_db(len_19, len_21):
    start_time = time.time()
    file2019 = open("Odata2019File.csv", encoding="cp1251")
    csv_data = reader(file2019, delimiter=';')
    header = next(csv_data)
    values_array = []

    outid = header.index('OUTID')
    regname = header.index('REGNAME')
    mathTest = header.index('mathTest')
    mathTestStatus = header.index('mathTestStatus')
    mathBall100 = header.index('mathBall100')
    year = 2019

    state_19 = collection.count_documents({'year': 2019})
    for idx in range(state_19):
        next(csv_data)

    for idx, row in enumerate(csv_data):
        values = {'outID': row[outid],
                  'regName': row[regname],
                  'testName': string_parser(row[mathTest]),
                  'testBal': string_parser(row[mathBall100]),
                  'testStatus': row[mathTestStatus] != 'null',
                  'year': year}

        values_array.append(values)

        if (idx + 1) % 20000 == 0 and idx != 0:
            if state_19 == 0:
                state_19 = idx + 1
            print("{}/{} of 19th set recorded...".format(state_19, len_19))
            collection.insert_many(values_array)
            values_array = []
            state_19 += 20000

    collection.insert_many(values_array)
    print("{}/{} of 19th set recorded.\n".format(len_19, len_19))

    file2021 = open("Odata2021File.csv", encoding="utf-8-sig")
    csv_data = reader(file2021, delimiter=';')
    header = next(csv_data)
    values_array = []

    outid = header.index('OUTID')
    regname = header.index('RegName')
    mathTest = header.index('MathTest')
    mathTestStatus = header.index('MathTestStatus')
    mathBall100 = header.index('MathBall100')
    year = 2021

    state_21 = collection.count_documents({'year': 2021})
    for idx in range(state_21):
        next(csv_data)

    for idx, row in enumerate(csv_data):
        values = {'outID': row[outid],
                  'regName': row[regname],
                  'testName': row[mathTest],
                  'testBal': string_parser(row[mathBall100]),
                  'testStatus': row[mathTestStatus] != 'null',
                  'year': year
                  }

        values_array.append(values)

        if (idx + 1) % 20000 == 0 and idx != 0:
            if state_21 == 0:
                state_21 = idx + 1
            print("{}/{} of 21th set recorded...".format(state_21, len_21))
            collection.insert_many(values_array)
            values_array = []
            state_21 += 20000

    collection.insert_many(values_array)
    print("{}/{} of 21th set recorded.\n".format(len_21, len_21))

    return time.time() - start_time


def pipeline_agg():
    pipeline = [
        {'$match': {'testStatus': True}},
        {'$group': {
            '_id': {
                'regName': '$regName',
                'year': '$year'
            },
            'maxBal': {'$max': '$testBal'}
        }}
    ]

    return collection.aggregate(pipeline)


# len_19 = csv_lines_count("Odata2019File.csv", "cp1251")
# print("Length of 19th csv = {}".format(len_19))
# len_21 = csv_lines_count("Odata2021File.csv", "utf-8-sig")
# print("Length of 21th csv = {}".format(len_21))
len_19 = 353812
len_21 = 389324

rec_time = initialize_db(len_19, len_21)
agg_res = pipeline_agg()
#
# with open('rec_time.txt', 'a+') as file:
#     file.write('Recording time: {}\n'.format(rec_time))
#
# with open('zno_results.csv', 'a+', newline='', encoding='utf-8') as csvfile:
#     maxMarks2019 = []
#     maxMarks2021 = []
#     for elem in agg_res:
#         regName = elem['_id']['regName']
#         year = elem['_id']['year']
#         maxMark = elem['maxBal']
#
#         if year == 2019:
#             maxMarks2019.append((regName, maxMark))
#         elif year == 2021:
#             maxMarks2021.append((regName, maxMark))
#
#     writer = writer(csvfile, delimiter=';')
#     writer.writerow(['regName', 'maxMark2019', 'maxMark2021'])
#
#     for i in range(len(maxMarks2019)):
#         writer.writerow([maxMarks2019[i][0], maxMarks2019[i][1], maxMarks2021[i][1]])
