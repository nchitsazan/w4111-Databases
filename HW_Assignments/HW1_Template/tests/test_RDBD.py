
import os
from src.RDBDataTable import RDBDataTable
import json



def test_init():
    
    connect_info = {
    "directory": data_dir,
    "file_name": "People.csv"
    }
    
    connect_info = {}
    
    rdb = RDBDataTable("people", connect_info, None)
    return rdb

rdb = test_init()
#
print("Testing Method test_init: \n" )
print("Test the initial connection to data base- If correct the query should return 162 rows")
print("Deos the query return 162 rows?", rdb.cur.execute("select * from people where nameLast='Smith'")==162)
#
#
def test_find_by_template():
    
    tmp= {"nameLast": 'Williams', 'birthCity':'San Diego'}
    field_list = ['playerID','nameFirst','nameLast', 'birthCity']
      
    connect_info = {}
    
    rdb = RDBDataTable("people", connect_info, None)

    print(rdb.find_by_template(tmp, field_list = field_list))
    
print("\nTesting Find By Template Method:")
print("if True should return a list of two dictionaries with above field")
test_find_by_template()
#
#
def test_find_by_primary_key():
    
    connect_info = {}
    rdb = RDBDataTable("appearances", connect_info, key_columns=["playerID", "teamID", "yearID"])

    result = rdb.find_by_primary_key(['willite01', 'BOS', '1960'])
    print(json.dumps(result[0], indent = 2))


print("\nTesting Find By Primary Key Method The test is conducted on the appearances table to check the multiple key case")
print("If True should print a list of one dictionary with all the fields:")
test_find_by_primary_key()



def test_del_by_template():
    
    tmp = {"nameLast": 'Butler', 'birthCity':'Boston'}
      
    connect_info = {}
    
    rdb = RDBDataTable("people", connect_info, None)
    rdb.cur.execute('SELECT COUNT(1) FROM people')
    rows_before_del = rdb.cur.fetchall()
    print('Number of rows before delete', rows_before_del[0]['COUNT(1)'])
    
    print('Number of deleted rows', rdb.delete_by_template(tmp, commit=True))
    
    rdb = RDBDataTable("people", connect_info, None)
    rdb.cur.execute('SELECT COUNT(1) FROM people')
    rows_before_del = rdb.cur.fetchall()
    print('Number of rows after delete', rows_before_del[0]['COUNT(1)'])

print("\nTesting Del by Temp:")   
test_del_by_template()

def test_delete_by_key():
    connect_info = {}
    rdb = RDBDataTable("people", connect_info, key_columns=["playerID"])   
    result = rdb.delete_by_key(['caseyjo01'], commit=True)
    print('number of deleted data points: ', result)

print("\nTesting Del by Key--Numer of deleted data points:")
test_delete_by_key()

  
def test_update_by_template():
    
    tmp = {"nameLast": 'Allison', "birthCity":'New York'}
    new_val = {"nameLast": 'TESTNAME', 'birthCity':'TESTCITY'}
      
    connect_info = {}
    
    rdb = RDBDataTable("people", connect_info, None)
    
    matching_row = rdb.cur.execute('''select nameLast, birthCity from people 
                                  where nameLast="Allison" and birthCity="New York" ''')
    
    print('\n Testing update by template')
    print('Number of matching rows= ', matching_row )

    result = rdb.update_by_template(tmp, new_val)
    print('Number of updated rows should be equal to number of matching rows')
    print('Number of updated rows = ', result)
    
test_update_by_template()


def test_update_by_key():
         
    connect_info = {}
    rdb = RDBDataTable("people", connect_info, key_columns=["playerID"])
    
    matching_row = rdb.cur.execute('''select playerID from people 
                                  where playerID = "abbotfr01" ''')
        
    print('\nTesting update rows by key:')
    print('Number of matching rows= ', matching_row )
    
    result = rdb.update_by_key(["abbotfr01"],["TESTVAL"])
#    result = rdb.update_by_key(["TESTVAL"],["abbotfr01"])
    print('Number of updated field = ', result)
    
test_update_by_key()


def test_insert():
    connect_info = {}
    rdb = RDBDataTable("people", connect_info, None)
    
    ''' To create a new record , I extract one of the rows and change the primary ID(playerID)'''
    
    rdb.cur.execute('''select * from people where playerID = "brewech01" ''')
    data = rdb.cur.fetchall()
    data[0]['playerID']+='TEST'
    new_record = data[0]
    
    rdb.cur.execute('''select count(1) from people ''')
    nr = rdb.cur.fetchall()
    print('''Test insert method: One row with playerID = "brewech01" is selected. The player id is changes to avoid confusion in primary key.
          Then this row is added to the end''' )
    print('Number of Rows before insert = ',nr[0]['count(1)'])
        
    rdb.insert(new_record)
    
    rdb = RDBDataTable("people", connect_info, None)
    rdb.cur.execute('''select count(1) from people ''')
    nr = rdb.cur.fetchall()
    print('Number of Rows after insert = ',nr[0]['count(1)'])
    
test_insert()                               