from difflib import SequenceMatcher
import pandas as pd
import numpy as np
import pyodbc
import difflib
import os
from functools import partial
import datetime

pd.set_option('display.float_format', lambda x: '%.3f' % x) #stops phone numbers from appearing in scientific notation

os.chdir(r'C:\Users\angus.gray\Downloads') #change directory to where the attribution list is located

now = datetime.datetime.now() #used later to make a 'date data pulled' column and to name csv output

#gets the similarity ratio between the two clientname columns
def apply_sm(merged, c1, c2):
    return difflib.SequenceMatcher(None, merged[c1], merged[c2]).ratio()

#connect to NDW using windows authentication
conn_str = (
            r'Driver={SQL Server};'
            r'Server=cstnismdb27.centerstone.lan;'
            r'Database=ndw3nfdb;'
            r'Trusted_Connection=yes;'
            )

cnxn = pyodbc.connect(conn_str)

attr = pd.read_excel('AttrList.xlsx') #read in the attribution file

attr['CLIENT NAME'] = attr['LAST_NAME'] + ', ' + attr['FIRST_NAME'] #make combined name field for the attr list

#get the client id and ssn info in NDW and store it in a dataframe
client_id_ssn = '''SELECT DISTINCT
        X.Client_ID
,       X.SSN
,       X.ClientName
FROM    ( SELECT    n.SourceClient_ID AS Client_ID
          ,         ssn.SSN
          ,         n.LastName + ', ' + n.FirstName AS ClientName
          ,         MAX(s.ServiceDate) AS maxservicedate
          ,         ROW_NUMBER() OVER ( PARTITION BY ssn.SSN ORDER BY s.ServiceDate DESC ) AS rnum3
          FROM      limiteddb.dbo.Client_SSN AS ssn
                    INNER JOIN limiteddb.dbo.ClientName AS n ON n.Client_ID = ssn.Client_ID
                                                              AND n.ORG_ID = 1
                    LEFT JOIN ndw3nfdb.dbo.Service AS s ON s.Client_ID = ssn.Client_ID
          WHERE     s.ServiceDate <= GETDATE()
          GROUP BY  n.SourceClient_ID
          ,         ssn.SSN
          ,         s.ServiceDate
          ,         n.LastName + ', ' + n.FirstName
        ) X
WHERE   rnum3 = 1;'''

client_id_ssn = pd.DataFrame(pd.read_sql(client_id_ssn, cnxn))

attr['SSN'] = attr['SSN'].apply(str) #make SSN a string so it is able to be merged on

merged = pd.merge(attr, client_id_ssn, on='SSN', how='left') #join/merge our attr list and SSN data from NDW

merged = merged.fillna('') #fill all NaN values with a blank string value so we can iterate over them without error

#create column that shows the similarity ratio between the two name columns.  (calling defined apply_sm function from above)
merged['NameMatchRatio'] = merged.apply(partial(apply_sm, c1='CLIENT NAME', c2='ClientName'), axis=1)

merged['Last4SSN'] = merged['SSN'].apply(lambda x: str(x)[-4:]) #get a last 4 SSN field instead of keeping full length SSN
merged['PHONE NUMBER'] = merged['PHONE NUMBER'].apply(lambda x: str(x)[:10]) #make phone number a string and get the first 10 digits
merged['PHONE NUMBER'].replace(to_replace = 'nan', value = '', inplace = True) #replace values 'nan' with a blank ('') string value

#drop useless columns
merged.drop(['LAST_NAME', 'FIRST_NAME', 'ALTRUISTA_ID',
       'INSURANCE ID', 'SSN',  'LAST_CLAIM', 'LAST_VISIT_DATE',
       'NEXT_VISIT_DATE', 'ER_VISITS', 'APP_VISITS',
       'ADTDAYS_COUNT', 'DUE_DAYS', 'ClientName',
       'ASSIGNED DATE/ATTRIBUTED DATE', 'PROGRAM_NAMES', 'RISK_CATEGORY_NAME', 'RISK_SCORE'], axis = 1, inplace = True)


#get the payor_id info in NDW and store it in a dataframe
payor_id = '''SELECT  X.*
                    FROM    ( SELECT DISTINCT
                                        c.sourceClient_ID as Client_ID
                                       ,cp.Payor_ID_Number
                                       ,MAX(cp.BeginDate) AS maxbegindate
                                       ,ROW_NUMBER() OVER ( PARTITION BY c.sourceClient_ID ORDER BY pc.PayorName ASC ) AS rnum
                              FROM      ndw3nfdb.dbo.ClientPayor AS cp
                                        INNER JOIN ndw3nfdb.dbo.PayorCode AS pc WITH (NOLOCK) ON pc.PayorCode_ID = cp.PayorCode_ID
                                        LEFT JOIN ndw3nfdb.dbo.PayorGroup AS pg WITH (NOLOCK) ON pg.PayorGroup_ID = pc.Payor_GRP_ID
                                        INNER JOIN ndw3nfdb.dbo.client as c with (NOLOCK) on c.client_id = cp.client_id
                              WHERE     cp.EndDate IS NULL
                                        AND pg.Tenncare = 1
                                        AND cp.ORG_ID = 1
                              GROUP BY  c.sourceClient_ID
                                       ,cp.Payor_ID_Number
                                       ,pc.PayorName
                            ) X
                    WHERE   X.rnum = 1
                            AND X.Payor_ID_Number IS NOT NULL'''

payor_id_data = pd.DataFrame(pd.read_sql(payor_id, cnxn))
main_data = pd.merge(merged, payor_id_data, on='Client_ID', how='left') #merge our main dataframe and the payor_id data


#get the care coordinator info in NDW and store it in a dataframe
cc_info = '''        SELECT  X1.sourceClient_ID as Client_ID
               ,X1.CC_Name
               ,X1.LOC_Name as CCLocation
        FROM    ( -- X1
                  SELECT    C.sourceClient_ID
                           ,DATEPART(YYYY , CS.BeginDate) Year
                           ,DATEPART(MM , CS.BeginDate) Month
                           ,CS.LOC_ID
                           ,CS.Staff_ID
                           ,QST.SourceStaff_ID
                           ,QST.EMP_ID
                           ,QST.FirstName + ' ' + QST.LastName AS CC_Name
                           ,l.LOC_Name
                           ,CC_RANK_DESC = ROW_NUMBER() OVER ( PARTITION BY C.Client_ID ORDER BY CS.PrimaryRecord DESC, CS.ClientStaff_ID DESC, CS.BeginDate DESC, CS.EndDate DESC )
                  FROM      ndw3nfdb.dbo.Client C
                            INNER JOIN ndw3nfdb.dbo.ClientStaff CS WITH ( NOLOCK ) ON C.Client_ID = CS.Client_ID
                            INNER JOIN ndw3nfdb.dbo.QV_Staff QST WITH ( NOLOCK ) ON QST.Staff_ID = CS.Staff_ID
                            left JOIN ndw3nfdb.dbo.QV_StaffHistory QSH WITH ( NOLOCK ) ON QST.Staff_ID = QSH.Staff_ID
                                                              AND CAST(QSH.BeginDate AS DATE) <= CAST(GETDATE() AS DATE)
                                                              AND ISNULL(CAST(QSH.EndDate AS DATE) ,
                                                              CAST(GETDATE() AS DATE)) >= CAST(GETDATE() AS DATE)
                            INNER JOIN ndw3nfdb.dbo.JobTitle JT WITH ( NOLOCK ) ON QST.JobTitle_ID = JT.JobTitle_ID
                            INNER JOIN ndw3nfdb.dbo.Location AS l ON l.LOC_ID = QST.LOC_ID
                  WHERE     CAST(CS.BeginDate AS DATE) <= CAST(GETDATE() AS DATE)
                            AND ISNULL(CAST(CS.EndDate AS DATE) ,
                                       CAST(GETDATE() AS DATE)) >= CAST(GETDATE() AS DATE)
                            AND CS.PrimaryRecord = 1
                            AND C.ORG_ID = 1
                ) X1
        WHERE   CC_RANK_DESC = 1;

'''

cc_info_data = pd.DataFrame(pd.read_sql(cc_info, cnxn))
main_data = pd.merge(main_data, cc_info_data, on='Client_ID', how='left') #merge our main dataframe and the cc_info data


#get the previous service info in NDW and store it in a dataframe
previous_service_info = '''SELECT  X.*
FROM    ( SELECT    c.SourceClient_ID AS Client_ID
          ,         MAX(s.ServiceDate) AS LastServiceDate
          ,         l.LOC_Name AS LastServiceLocation
          ,         a.ActivityCode AS LastServiceActivityCode
          ,         a.Activity AS LastServiceActivity
          ,         ROW_NUMBER() OVER ( PARTITION BY c.SourceClient_ID ORDER BY c.SourceClient_ID ) AS rnum1
          FROM      Service AS s
                    LEFT JOIN Location AS l WITH ( NOLOCK ) ON l.LOC_ID = s.LOC_ID
                    LEFT JOIN dbo.Activity AS a WITH ( NOLOCK ) ON a.Activity_ID = s.Activity_ID
                    INNER JOIN Client AS c WITH ( NOLOCK ) ON c.Client_ID = s.Client_ID
          WHERE     s.ServiceDate BETWEEN DATEADD(YEAR, -2, GETDATE())
                                  AND     GETDATE()
                    AND s.ORG_ID = 1
                    AND s.ServiceStatus_ID = 14
                    AND s.DeletedFlag = 0
                    AND a.ActivityCode != 'MEMO'
          GROUP BY  c.SourceClient_ID
          ,         l.LOC_Name
          ,         a.ActivityCode
          ,         a.Activity
        ) X
WHERE   X.rnum1 = 1;'''

previous_service_data = pd.DataFrame(pd.read_sql(previous_service_info, cnxn))
main_data = pd.merge(main_data, previous_service_data, on='Client_ID', how='left') #merge our main dataframe and the previous service data


#get the next service info in NDW and store it in a dataframe
next_service_info = '''SELECT  X.*
FROM    ( SELECT    c.SourceClient_ID AS Client_ID
          ,         MIN(s.ServiceDate) AS NextServiceDate
          ,         l.LOC_Name AS NextServiceLocation
          ,         a.ActivityCode AS NextServiceActivityCode
          ,         a.Activity AS NextServiceActivity
          ,         ROW_NUMBER() OVER ( PARTITION BY c.SourceClient_ID ORDER BY c.SourceClient_ID ) AS rnum2
          FROM      Service AS s
                    LEFT JOIN Location AS l WITH ( NOLOCK ) ON l.LOC_ID = s.LOC_ID
                    LEFT JOIN dbo.Activity AS a WITH ( NOLOCK ) ON a.Activity_ID = s.Activity_ID
                    INNER JOIN Client AS c WITH ( NOLOCK ) ON c.Client_ID = s.Client_ID
          WHERE     s.ServiceDate > GETDATE()
                    AND s.ORG_ID = 1
                    AND s.DeletedFlag = 0
                    AND a.ActivityCode != 'MEMO'
          GROUP BY  c.SourceClient_ID
          ,         l.LOC_Name
          ,         a.ActivityCode
          ,         a.Activity
        ) X
WHERE   X.rnum2 = 1;'''

next_service_data = pd.DataFrame(pd.read_sql(next_service_info, cnxn))
main_data = pd.merge(main_data, next_service_data, on='Client_ID', how='left') #merge our main dataframe and the next service data


#get the level of care  info in NDW and store it in a dataframe
loc_info = '''SELECT  X.SourceClient_ID as Client_ID
,       X.HLink_LOC
FROM    ( SELECT    c.SourceClient_ID
          ,         cp.HLink_LOC
          ,         ROW_NUMBER() OVER ( PARTITION BY c.SourceClient_ID ORDER BY cp.BeginDate DESC ) AS rnum4
          FROM      dbo.ClientProgram AS cp
                    LEFT JOIN Client AS c ON c.Client_ID = cp.Client_ID
          WHERE     cp.PROG_ID = 6101
                    AND cp.ORG_ID = 1
                    AND cp.EndDate IS NULL
        ) X
WHERE   X.rnum4 = 1;'''

loc_data = pd.DataFrame(pd.read_sql(loc_info, cnxn))
main_data = pd.merge(main_data, loc_data, on='Client_ID', how='left') #merge our main dataframe and the level of care data


#get the client status info in NDW and store it in a dataframe
status_info = '''SELECT  c.SourceClient_ID as Client_ID
,       c.ClientStatus AS MemberStatus
FROM    Client AS c
WHERE   c.ORG_ID = 1;'''

status_data = pd.DataFrame(pd.read_sql(status_info, cnxn))
main_data = pd.merge(main_data, status_data, on='Client_ID', how='left') #merge our main dataframe and the client status data

main_data.drop(['rnum', 'rnum1', 'rnum2', 'maxbegindate'], axis = 1, inplace = True) #drop more useless columns that were joined

main_data = main_data.fillna('') #fill all NaN values a blank string so data looks nicer and so we can iterate over them without error

main_data['Payor_ID_Number'] = main_data['Payor_ID_Number'].str.strip('ZEC|D') #Strip ZEC or ZED from beginning of payor_id.  ( at the request of Mandi)

main_data['RunDate'] = now.strftime("%Y-%m-%d") #add a column that displays what date this data was ran out on

#reorganize our columns into a nicer display order
main_data = main_data[['RunDate', 'HEALTH PLAN', 'Payor_ID_Number', 'Client_ID', 'Last4SSN',
       'PATIENT_DOB', 'CLIENT NAME', 'ADDRESS', 'PHONE NUMBER', 'PCP_NAME', 'MemberStatus',
       'THL_STATUS', 'CC_Name', 'CCLocation','LastServiceDate', 'LastServiceLocation',
       'LastServiceActivityCode','LastServiceActivity', 'NextServiceDate', 'NextServiceLocation',
       'NextServiceActivityCode', 'NextServiceActivity', 'NameMatchRatio', 'HLink_LOC']]

#creates a dataframe of only matched clients.  this has no use except to calculate the number of matched vs unmatched attributed clients
match = main_data[(main_data['Payor_ID_Number'] != '') & (main_data['NameMatchRatio'] >= 0.70)]

print('no match: ' + str(int(main_data.shape[0]) - int(match.shape[0]))) #prints the number of clients that did not get associated with a Payor_ID_Number
print('match: ' + str(match.shape[0])) #prints the number of clients that DID get associated with a Payor_ID_Number

#drop the last useless column
main_data.drop(['NameMatchRatio'], axis = 1, inplace = True)

#send our mapped client info to a csv file in the current working directory.  (CWD is located at top of document)
main_data.to_csv('HLAltruistaAttrClientsMapped' + ' ' + now.strftime("%Y-%m-%d") + '.csv', index = False)
