import re
import os
import pandas as pd


#change active directory
os.chdir(r'\\Centerstone.lan\Files\HomeDrive\angus.gray\My Documents\claimstest')
base_dir = (r'\\Centerstone.lan\Files\HomeDrive\angus.gray\My Documents\claimstest')

#set up Regular Expression objects to parse X12
claimidRegex = re.compile(r'(CLM\*)(\d+)')
dxRegex = re.compile(r'(ABK:)(\w\d+)(\*|~)(ABF:)?(\w\d+)?(\*|~)?(ABF:)?(\w\d+)?(\*|~)?(ABF:)?(\w\d+)?(\*|~)?(ABF:)?(\w\d+)?(\*|~)?(ABF:)?(\w\d+)?(\*|~)?(ABF:)?(\w\d+)?(\*|~)?(ABF:)?(\w\d+)?(\*|~)?')

claimids = []
dxinfo = []

for dirpath, dirnames, filename in os.walk(base_dir):
    for filename in filename:
        txtfile_full_path = os.path.join(dirpath, filename)
        x12 = open(txtfile_full_path, 'r')
        for i in x12:
            match = claimidRegex.findall(i)
            for word in match:
                claimids.append(word[1])
        x12.seek(0)
        for i in x12:
            match = dxRegex.findall(i)
            for word in match:
                dxinfo.append(word)
        x12.close()

datadic = dict(zip(claimids, dxinfo))

#Read and clean data in Pandas.  Assign column names and drop non diag columns
df = pd.DataFrame.from_dict(datadic, orient = 'index').reset_index()
df.columns = ['ClaimID', 'Toss1', 'PrimaryDx', 'Toss2', 'Toss3',
              'SecDiag2', 'Toss4', 'Toss5', 'SecDiag3', 'Toss6',
              'Toss7', 'SecDiag4', 'Toss8', 'Toss9', 'SecDiag5',
             'Toss10', 'Toss11', 'SecDiag6', 'Toss12', 'Toss13',
             'SecDiag7', 'Toss14', 'Toss15', 'SecDiag8', 'Toss16']

idf = df.set_index(['ClaimID'])
idf.drop(['Toss1', 'Toss2', 'Toss3', 'Toss4', 'Toss5', 'Toss6',
          'Toss7', 'Toss8', 'Toss9', 'Toss10', 'Toss11', 'Toss12',
          'Toss13', 'Toss14', 'Toss15', 'Toss16'], axis = 1, inplace = True)

#send dataframe to a csv file
os.chdir(r'\\Centerstone.lan\Files\HomeDrive\angus.gray\My Documents')
idf.to_csv('Claims.csv', sep = ',')
