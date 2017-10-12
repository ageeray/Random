from difflib import SequenceMatcher
import pandas as pd
import numpy as np
import pyodbc
import difflib
import os
from functools import partial


os.chdir(r'C:\Users\angus.gray\Downloads')

def apply_sm(match, c1, c2):
    return difflib.SequenceMatcher(None, match[c1], match[c2]).ratio()

conn_str = (
            r'Driver={SQL Server};'
            r'Server=cstnismdb27.centerstone.lan;'
            r'Database=ndw3nfdb;'
            r'Trusted_Connection=yes;'
            )

cnxn = pyodbc.connect(conn_str)

#read in excel file
attr = pd.read_excel('AttrList.xlsx')

#make concatenated name field to compare joins on
attr['PATIENT NAME1'] = attr['LAST_NAME'] + ', ' + attr['FIRST_NAME']

#basic info from NDW to join to our excel data
sql = '''SELECT  ssn.Client_ID,
        ssn.SSN,
        n.LastName + ', ' + n.FirstName AS clientname
FROM    limiteddb.dbo.Client_SSN AS ssn
        INNER JOIN limiteddb.dbo.ClientName AS n ON n.Client_ID = ssn.Client_ID
                                             AND n.ORG_ID = 1;'''

data = pd.DataFrame(pd.read_sql(sql, cnxn))
#make attr['SSN'] a string to join on
attr['SSN'] = attr['SSN'].apply(str)
#merge the dataframes together
merged = pd.merge(attr, data, on='SSN', how='outer')
nomatch = merged[merged['Client_ID'].isnull() == True]
#can't iterate over NaN values in either column.  filter out the NaNs
match = merged[(merged['PATIENT NAME1'].isnull() == False) & (merged['Client_ID'].isnull() == False)]
#call our defined function on our data to calculate match ratio
match['NameMatchRatio'] = match.apply(partial(apply_sm, c1='PATIENT NAME1', c2='clientname'), axis=1)
#get matched clients with a match ratio of 0.7 or more
match = match[match['NameMatchRatio'] >= 0.7]

#change SSN to last 4 digits and get phone number/client_ID into string format
match['Last4SSN'] = match['SSN'].apply(lambda x: str(x)[-4:])
match['PHONE NUMBER2'] = match['PHONE NUMBER'].apply(lambda x: str(x)[:10])
match['Client_ID'] = match['Client_ID'].apply(lambda x: str(int(x)))

#drop useless columns
match.drop(['LAST_NAME', 'FIRST_NAME', 'ALTRUISTA_ID',
       'INSURANCE ID', 'SSN',  'LAST_CLAIM', 'LAST_VISIT_DATE',
       'NEXT_VISIT_DATE', 'ER_VISITS', 'APP_VISITS',
       'ADTDAYS_COUNT', 'DUE_DAYS', 'PATIENT NAME1',
       'NameMatchRatio', 'PHONE NUMBER'], axis = 1, inplace = True)

#get rid of scientific notation
pd.set_option('display.float_format', lambda x: '%.3f' % x)

#fill all null values with empty
match.fillna('')

match.to_csv('match.csv', sep = ',')

print('no match: ' + str(nomatch.shape))
print('match: ' + str(match.shape))
