import csv
import os
import datetime

from database import Database
from database import CursorFromConnectionFromPool


Database.initialise(user='postgres', password='samfurdissamrea', host='localhost', database='ODS')
datestamp = datetime.datetime.today().strftime('%Y-%m-%d')
os.chdir(r'C:\Users\ageeray\Documents\Python Scripts\PokemonTrainerScrape\Project')

with CursorFromConnectionFromPool() as cursor:
    #cursor.execute('truncate table public."trainer_data"')
    with open('Pokemon.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row.
        for row in reader:
#                    print(row)
#                    print(str(len(row)))
            cursor.execute(
                'INSERT INTO public.pokemon_data (poke_id, poke_name, type1, type2, total_stat, hp, attack, defense, spatk, spdef, spd, generation, legendary_ind) VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s)',
                row
            )