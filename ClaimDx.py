import re
import os
import pandas as pd

#change active directory
os.chdir(r'H:\aNGUS')
base_dir = (r'H:\aNGUS')

#set up Regular Expression objects to parse X12
claimidRegex = re.compile(r'(CLM\*)(\d+)')
dxRegex = re.compile(r'(A?BK:)(\w\d+)?(\*|~)?(A?BF:)?(\w\d+)?(\*|~)?(A?BF:)?(\w\d+)?(\*|~)?(A?BF:)?(\w\d+)?(\*|~)?(A?BF:)?(\w\d+)?(\*|~)?(A?BF:)?(\w\d+)?(\*|~)?(A?BF:)?(\w\d+)?(\*|~)?(A?BF:)?(\w\d+)?(\*|~)?')

#the two main lists that will be zipped together to create master dictionary
appended_data = []
docname = []

for dirpath, dirnames, filename in os.walk(base_dir):
    for filename in filename:
        claimids = []
        dxinfo = []
        txtfile_full_path = os.path.join(dirpath, filename)
        x12 = open(txtfile_full_path, 'r')
        #get claimids into a list
        for i in x12:
            match = claimidRegex.findall(i)
            for word in match:
                claimids.append(word[1])
        #reset the read cursor for the next for loop
        x12.seek(0)
        # get list of tuples with diagnosis information
        for i in x12:
            match = dxRegex.findall(i)
            for word in match:
                dxinfo.append(word)
        #for every row being read from current text file, append name of document to a list
        for i in range(len(claimids)):
            docname.append(filename)
        #create dictionary that relates claimids to diagnosis tuples
        datadic = dict(zip(claimids, dxinfo))
        dfa = pd.DataFrame.from_dict(datadic, orient = 'index').reset_index()
        #append dataframe to list
        appended_data.append(dfa)
        x12.close()

#concat all dataframes together to create big one
appended_data = pd.concat(appended_data, axis = 0)

#name columns
appended_data.columns = ['Claim_Bill_ID', 'Toss1', 'PrimaryDx', 'Toss2', 'Toss3',
           'SecDiag2', 'Toss4', 'Toss5', 'SecDiag3', 'Toss6',
           'Toss7', 'SecDiag4', 'Toss8', 'Toss9', 'SecDiag5',
          'Toss10', 'Toss11', 'SecDiag6', 'Toss12', 'Toss13',
          'SecDiag7', 'Toss14', 'Toss15', 'SecDiag8', 'Toss16']
appended_data.insert(8, 'New_ID', range(len(appended_data)))
#assign index and drop useless columns
idf = appended_data.set_index(['New_ID'])
idf.drop(['Toss1', 'Toss2', 'Toss3', 'Toss4', 'Toss5', 'Toss6',
          'Toss7', 'Toss8', 'Toss9', 'Toss10', 'Toss11', 'Toss12',
          'Toss13', 'Toss14', 'Toss15', 'Toss16'], axis = 1, inplace = True)

#create dataframe of the list of document names
docname = pd.DataFrame(docname)
docname.columns = ['DocName']

#create a common column for docname and idf dataframes to join on; basically create an index from 1 to length of document
docname.insert(0, 'New_ID', range(len(docname)))

#create the index column for idf
idf.insert(8, 'New_ID', range(len(idf)))

#merge the two dataframes together on the new index
idf = pd.merge(idf, docname, on = 'New_ID', how = 'inner')

#drop the indexing column and assign new index
idf.drop('New_ID', axis = 1, inplace = True)
idf.set_index('Claim_Bill_ID', inplace = True)

#export dataframe to a csv file
os.chdir(r'\\Centerstone.lan\Files\HomeDrive\angus.gray\My Documents')
idf.to_csv('Claims.csv', sep = ',')
