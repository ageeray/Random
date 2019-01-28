import itertools
import pandas as pd
import re
from itertools import product
import csv
import os
import time
import random
import datetime

import requests
from selenium import webdriver
from bs4 import BeautifulSoup

from database import Database
from database import CursorFromConnectionFromPool


Database.initialise(user='postgres', password='samfurdissamrea', host='localhost', database='ODS')
datestamp = datetime.datetime.today().strftime('%Y-%m-%d')
os.chdir(r'C:\Users\ageeray\Documents\Python Scripts\PokemonTrainerScrape\Project\Output')

#gets row level html info from relevant tables
def getTableRowExtract(web_url):
    try:
        page=requests.get(web_url, headers={'User-Agent': 'Mozilla/5.0'})
    except Exception as e:
        pass
    print(web_url)
    rowextract = []
    soup = BeautifulSoup(page.text, 'lxml')
    # get all the html tables into a list and filter it
    tableTag = soup.find_all("table")
    trainerTables = filterTrainerTables(tableTag)
    # parse out the rows of the tables that contain trainer data
    rows = []
    for i in trainerTables:
        trows = i.find_all("tr")
        trows = trows[1:-1]
        rows.append(trows)
    merged = list(itertools.chain(*rows))
    rowextract.append(merged)
    # this is out list of table rows from all the pages
    rowextract = list(itertools.chain(*rowextract))
    return rowextract

# filters the list of scraped tables to onlt ones with trainer data
def filterTrainerTables(tlist):
    trainerTables = []
    for table in tlist:
        try:
            ths = table.find_all('th')
            headings = [th.text.strip() for th in ths]
            if headings[0] == 'Trainer name':
                trainerTables.append(table)
        except Exception as e:
            pass
    return trainerTables

#returns dataframe of trainer data (name, battle, winnings)
def getTrainerData(rowextract):
     # get the non pokemon trainer data into dataframe format
    rowspans = []  # track pending rowspans
    rows = rowextract

    # first scan, see how many columns we need
    colcount = 0
    for r, row in enumerate(rows):
        
        cells = row.find_all(['td', 'th'], recursive=False)[:4]
        # count columns (including spanned).
        # add active rowspans from preceding rows
        # we *ignore* the colspan value on the last cell, to prevent
        # creating 'phantom' columns with no actual cells, only extended
        # colspans. This is achieved by hardcoding the last cell width as 1. 
        # a colspan of 0 means “fill until the end” but can really only apply
        # to the last cell; ignore it elsewhere. 
        colcount = max(
            colcount,
            sum(int(c.get('colspan', 1)) or 1 for c in cells[:-1]) + len(cells[-1:]) + len(rowspans))
        # update rowspan bookkeeping; 0 is a span to the bottom. 
        rowspans += [int(c.get('rowspan', 1)) or len(rows) - r for c in cells]
        rowspans = [s - 1 for s in rowspans if s > 1]
    
    # it doesn't matter if there are still rowspan numbers 'active'; no extra
    # rows to show in the table means the larger than 1 rowspan numbers in the
    # last table row are ignored.
    
    # build an empty matrix for all possible cells
    trainerdata = [[None] * colcount for row in rows]
    
    # fill matrix from row data
    rowspans = {}  # track pending rowspans, column number mapping to count
    for row, row_elem in enumerate(rows):
        span_offset = 0  # how many columns are skipped due to row and colspans 
        for col, cell in enumerate(row_elem.find_all(['td', 'th'], recursive=False)):
            # adjust for preceding row and colspans
            col += span_offset
            while rowspans.get(col, 0):
                span_offset += 1
                col += 1
    
            # fill table data
            rowspan = rowspans[col] = int(cell.get('rowspan', 1)) or len(rows) - row
            colspan = int(cell.get('colspan', 1)) or colcount - col
            # next column is offset by the colspan
            span_offset += colspan - 1
            value = cell.get_text()
            for drow, dcol in product(range(rowspan), range(colspan)):
                try:
                    trainerdata[row + drow][col + dcol] = value
                except IndexError:
                    # rowspan or colspan outside the confines of the table
                    pass
    
        # update rowspan bookkeeping
        rowspans = {c: s - 1 for c, s in rowspans.items() if s > 1}
    trainerdata = pd.DataFrame(trainerdata)
    if trainerdata.empty:
        pass
    else:
        trainerdata = trainerdata[trainerdata.columns[:3]]
    return trainerdata

driver = webdriver.Firefox()
driver.get(r'https://bulbapedia.bulbagarden.net/wiki/Pok%C3%A9mon_Trainer#List_of_Trainer_classes')
continue_link = driver.find_element_by_tag_name('a')
elem = driver.find_elements_by_xpath("//*[@href]")

sites =[]

for i in elem:
    sites.append(str(i.get_attribute('href')))

matching = [x for x in sites if r'_(Trainer_class)' in x and '.net/wiki/' in x]



# loop through the trainer class websites and extract the relevant table data
for i in set(matching):
    rowextract = getTableRowExtract(i)
    trainerdata = getTrainerData(rowextract)
    
    # get the pokemon level into dataframe format
    rowspans = []  # track pending rowspans
    rows = rowextract
    
    # first scan, see how many columns we need
    colcount = 0
    for r, row in enumerate(rows):
        cells = row.find_all(['td', 'th'], recursive=False)[3:]
        colcount = max(
            colcount,
            sum(int(c.get('colspan', 1)) or 1 for c in cells[:-1]) + len(cells[-1:]) + len(rowspans))
        # update rowspan bookkeeping; 0 is a span to the bottom. 
        rowspans += [int(c.get('rowspan', 1)) or len(rows) - r for c in cells]
        rowspans = [s - 1 for s in rowspans if s > 1]
    
    # build an empty matrix for all possible cells
    pokemonlevels = [[None] * colcount for row in rows]
    
    # fill matrix from row data
    rowspans = {}  # track pending rowspans, column number mapping to count
    for row, row_elem in enumerate(rows):
        span_offset = 0  # how many columns are skipped due to row and colspans 
        for col, cell in enumerate(row_elem.find_all(['td', 'th'], recursive=False)[3:]):
            # adjust for preceding row and colspans
            col += span_offset
            while rowspans.get(col, 0):
                span_offset += 1
                col += 1
    
            # fill table data
            rowspan = rowspans[col] = int(cell.get('rowspan', 1)) or len(rows) - row
            colspan = int(cell.get('colspan', 1)) or colcount - col
            # next column is offset by the colspan
            span_offset += colspan - 1
            value = cell.get_text()
            for drow, dcol in product(range(rowspan), range(colspan)):
                try:
                    pokemonlevels[row + drow][col + dcol] = value
                except IndexError:
                    # rowspan or colspan outside the confines of the table
                    pass
    
        # update rowspan bookkeeping
        rowspans = {c: s - 1 for c, s in rowspans.items() if s > 1}
    
    # get the pokemon name info from the href data to parse
    rowspans = []  # track pending rowspans
    rows = rowextract
    
    # first scan, see how many columns we need
    colcount = 0
    for r, row in enumerate(rows):
        cells = row.find_all(['td', 'th'], recursive=False)[3:]
        colcount = max(
            colcount,
            sum(int(c.get('colspan', 1)) or 1 for c in cells[:-1]) + len(cells[-1:]) + len(rowspans))
        # update rowspan bookkeeping; 0 is a span to the bottom. 
        rowspans += [int(c.get('rowspan', 1)) or len(rows) - r for c in cells]
        rowspans = [s - 1 for s in rowspans if s > 1]
    
    # build an empty matrix for all possible cells
    pokemonnames = [[None] * colcount for row in rows]
    
    # fill matrix from row data
    rowspans = {}  # track pending rowspans, column number mapping to count
    for row, row_elem in enumerate(rows):
        span_offset = 0  # how many columns are skipped due to row and colspans 
        for col, cell in enumerate(row_elem.find_all(['td', 'th'], recursive=False)):
            # adjust for preceding row and colspans
            col += span_offset
            while rowspans.get(col, 0):
                span_offset += 1
                col += 1
    
            # fill table data
            rowspan = rowspans[col] = int(cell.get('rowspan', 1)) or len(rows) - row
            colspan = int(cell.get('colspan', 1)) or colcount - col
            # next column is offset by the colspan
            span_offset += colspan - 1
            try:
                 value = cell.find_all('a', href=True)[0]
            except:
                value = '-------\\n'
            for drow, dcol in product(range(rowspan), range(colspan)):
                try:
                    pokemonnames[row + drow][col + dcol] = value
                except IndexError:
                    # rowspan or colspan outside the confines of the table
                    pass
    
        # update rowspan bookkeeping
        rowspans = {c: s - 1 for c, s in rowspans.items() if s > 1}
    
    

    pokemonlevels = pd.DataFrame(pokemonlevels)
    pokemonnames = pd.DataFrame(pokemonnames)
    
    flatdata = pd.concat([trainerdata, pokemonlevels, pokemonnames], axis=1)
    flatdata['ImportDate'] = datetime.datetime.today()
    flatdata.columns = ['TrainerName', 'Battle', 'Winnings','PokeLevel1', 'PokeLevel2', 'PokeLevel3', 
                        'PokeLevel4', 'PokeLevel5', 'PokeLevel6', 'Pokemon1', 'Pokemon2', 'Pokemon3', 
                        'Pokemon4', 'Pokemon5', 'Pokemon6', 'ImportDate']
    
    regex = re.compile('[^a-zA-Z ]')
    
    flatdata['TrainerName'] = flatdata['TrainerName'].map(lambda x: regex.sub('', x))
    flatdata.to_csv('TrainerData.csv', sep=',',index=False)
    
    with CursorFromConnectionFromPool() as cursor:
        #cursor.execute('truncate table public."trainer_data"')
        with open('TrainerData.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row.
            for row in reader:
                cursor.execute(
                    'INSERT INTO public."trainer_data" (trainer_name, battle, winnings, poke_level1, poke_level2, poke_level3, poke_level4, poke_level5, poke_level6, pokemon1, pokemon2, pokemon3, pokemon4, pokemon5, pokemon6, import_date) VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)',
                    row
                )
time.sleep(random.randint(3,7))

