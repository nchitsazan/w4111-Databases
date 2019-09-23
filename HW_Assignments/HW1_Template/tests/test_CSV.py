import logging
import os
from src.CSVDataTable import CSVDataTable
import json

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("./Data/Baseball")


def test_init():
    
    connect_info = {
    "directory": data_dir,
    "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    return csv_tbl


#    print('Loaded Table = \n', csv_tbl)

def test_matches_template():
    
    row = {"cool": 'yes', 'db':'no '}
    t = {"cool":'yes'}
    result = CSVDataTable.matches_template(row, t)
    return result

def test_match_all(field_list):
    
    tmp= {"nameLast": 'Williams', 'birthCity':'San Diego'}
    
    connect_info = {
    "directory": data_dir,
    "file_name": "People.csv"
     }

    csv_tbl = CSVDataTable("people", connect_info, None)
    
    result = csv_tbl.find_by_template(tmp, field_list = field_list)
#    print(json.dumps(result, indent = 2))
    return result
    
def test_delete_by_template():
    tmp= {"nameLast": 'Williams', 'birthCity':'San Diego'}
    connect_info = {
    "directory": data_dir,
    "file_name": "People.csv"
     }

    csv_tbl = CSVDataTable("people", connect_info, None)
    print('number of data points before delete:', len(csv_tbl._rows))
    print('number of deleted data points:', csv_tbl.delete_by_template(tmp))
    print('number of data points after delete:',len(csv_tbl._rows))
    
def test_update_by_template():
    tmp= {"nameLast": 'Williams', 'birthCity':'San Diego'}
    nval = {"nameLast": 'TESTNAME', 'birthCity':'TESTCITY'}
    
    connect_info = {
    "directory": data_dir,
    "file_name": "People.csv"
     }

    csv_tbl = CSVDataTable("people", connect_info, None)
    
    print('Number of updated fields:',csv_tbl.update_by_template(tmp,nval))
    print('\n')
    result = csv_tbl.find_by_template(nval)
    print('Updated Fields: ', json.dumps(result, indent = 2))
    print('\n')
    
    result = csv_tbl.find_by_template(tmp)
    print('Checking if fields matching  template are still in data\nif the function works correctly this list should be empty')
    print (result)

def test_insert():
    connect_info = {
    "directory": data_dir,
    "file_name": "People.csv"
     }

    csv_tbl = CSVDataTable("people", connect_info, None)
    new_row = csv_tbl._rows[0]
    
    l1 = len(csv_tbl._rows)
    csv_tbl.insert(new_row)
    l2 = len(csv_tbl._rows)
    
    print('\nDid we add one row to the data? ')
    print(l1==l2-1)
    
    print('Is first row of data equal to last row?')
    print(csv_tbl._rows[0]==csv_tbl._rows[len(csv_tbl._rows)-1])
    
def test_find_by_primary_key(field_list):
   
    connect_info = {
    "directory": data_dir,
    "file_name": "People.csv"
     }

    csv_tbl = CSVDataTable("appearances", connect_info, key_columns=["playerID", "teamID", "yearID"])
    
    result = csv_tbl.find_by_primary_key(['willite01', 'BOS', '1960'] ,field_list = field_list)
#    print(json.dumps(result, indent = 2))
    return result

def test_delete_by_key():
    connect_info = {
    "directory": data_dir,
    "file_name": "People.csv"
     }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"]);
    
    print('\nTesting del by primary key method. One data point should be deleted')
    print('number of deleted data points: ', csv_tbl.delete_by_key(["willitr01"]))
    
def test_update_by_key():
    connect_info = {
    "directory": data_dir,
    "file_name": "People.csv"
     }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"]);
    
    print('\nTesting update by primary key method. One data point should be updated')
    print('number of updated data points: ', csv_tbl.update_by_key(["willitr01"],["TESTVAL"])) 

print("\nTesting Class CSVDataTable")
print("Testing Method test_init:")
print(test_matches_template())

field_list = ['nameFirst','nameLast', 'birthCity', 'deathDay']
res = test_match_all(field_list)
print('\n Testting Find_by_Template Method \n ', res)

print('\nTesting Delete By Template. If correct the number of data points should reduce by 2: ')
test_delete_by_template()

print('\nTesting update By Template. If correct the number of data points should reduce by 2: ')
test_update_by_template()

print('Test the insert method. The first row of data is inserted as a new data point')
test_insert()

print('Testing find by primary key method. The primary key is ["playerID", "teamID", "yearID"].')
field_list = ['playerID','nameFirst','nameLast', 'birthCity']
print(test_find_by_primary_key(field_list))

test_delete_by_key()
test_update_by_key()




