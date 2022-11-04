import pyodbc 
import pandas as pd


index_l = 'Name'

Numb_File = r'Numbs.txt'

list_of_A_Numbers = []
with open(Numb_File) as f:                                                                                                                                                                                       
    list_of_A_Numbers = f.readlines()

list_of_A_Numbers = map(lambda s: s.strip(), list_of_A_Numbers)
# print(list_of_A_Numbers)

number_list = ''

for i in list_of_A_Numbers:
    # print(i)
    number_list = number_list + "'"+str(i)+"',"

# print(number_list)
query = """Select A_Party, 
B_Party,
Event_Start_Date,
Event_Type,
Service_Type,
Traffic_Type,
Event_Date,
Total_Duration
from Usage_Table
where Ref_Point = 'Onnet'
and Event_Date >= '2022%'
and A_Party in ({0})""".format(number_list[:-1])

try : 
    with pyodbc.connect("DSN=HIVE_CONN", autocommit=True) as conn:
                df = pd.read_sql(query, conn)
                df.to_csv ('Hive/file/CDR_{0}.csv'.format(index_l), index = False, header=True)
except Exception as e:
        print(e)
        print("Data Not present")
        