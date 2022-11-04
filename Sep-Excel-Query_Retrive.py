from datetime import date
import pyodbc 
import pandas as pd
from dotenv import load_dotenv
import os



load_dotenv()
START_DATE = os.getenv('START_DATE')
CONN = os.getenv('CONN')
CONTROL_POINT = os.getenv('CONTROL_POINT')
NUMBS_LIST = os.getenv('NUMS_LIST')
ERROR_LIST = os.getenv('ERROR_LIST')
STORE_PATH  = os.getenv('STORE_PATH')

os.chdir(STORE_PATH)

print(START_DATE)
print(CONN)

print(NUMBS_LIST)
print(ERROR_LIST)

Numb_File = NUMBS_LIST

Error_file = ERROR_LIST

f2 = open(Error_file, "a")

list_of_A_Numbers = []
with open(Numb_File) as f:                                                                                                                                                                                       
    list_of_A_Numbers = f.readlines()

list_of_A_Numbers = map(lambda s: s.strip(), list_of_A_Numbers)

# number_list = ''

for i in list_of_A_Numbers:
    try:
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
                    and A_Party in ({0});""".format(i , START_DATE, CONTROL_POINT)
        print(query)
       
        with pyodbc.connect("DSN={0}".format(CONN), autocommit=True) as conn:
                    df = pd.read_sql(query, conn)
                    # print(df)
                    df.to_csv ('{1}/CDRS/CDRs_{0}.csv'.format(i,STORE_PATH), index = False, header=True)
    except Exception as e:
        print(e)
        print(str(i) +" Not present")
        f2.write(i +"\n")
        continue

f2.close()

print("CDR(s) extracted")


