import os
from dotenv import load_dotenv
from pymongo import MongoClient

from insert import insert_all_pokemons
from aggregations import color_by_type, damage_taken_avg, imc_avg, type_change_filter, weight_double_filter

from pprint import pprint

def print_result (result):
  for item in result:
    pprint(item, width=2)
    print('')

if __name__ == '__main__':
  load_dotenv()
  mongo_url = os.environ['MONGO_URL']
  client = MongoClient(mongo_url)

  db = client[os.environ['DB_NAME']]
  pokemons_col = db[os.environ['COLLECTION']]
  
  # insert_all_pokemons(pokemons_col)

  # IMC
  # print_result(imc_avg(pokemons_col))

  # Cor por tipo
  # print_result(color_by_type(pokemons_col))

  # Dano médio de tipo x tipo
  # print_result(damage_taken_avg(pokemons_col))

  # Dobram o peso na evolução
  # print_result(weight_double_filter(pokemons_col))

  # Trocam de tipo na evolução
  # print_result(type_change_filter(pokemons_col))
