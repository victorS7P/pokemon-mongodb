import pandas as pd

def get_float_value (value):
  return float(value) if str(value) != 'nan' else 0.0

def get_pokemon_document (pokemon, pokemons_df):
  evolutions = []
  next_evolves = str(pokemon['next_evolves'])
  if len(next_evolves):
    for number in tuple(eval(pokemon['next_evolves'])):
      evolution_pokemon = pokemons_df[pokemons_df['number'] == number]
      if (len(evolution_pokemon.index) == 1):
        evolution_pokemon = evolution_pokemon.iloc[0]
        evolutions.append(get_pokemon_document(evolution_pokemon, pokemons_df))

  types = str(pokemon['types'])
  types = list(eval(types.replace('fighting', 'fight'))) if types != 'nan' else []

  damage_taken = str(pokemon['damage_taken'])
  damage_taken = dict(eval(damage_taken.replace('""', '"'))) if damage_taken != 'nan' else {}

  color = str(pokemon['color'])
  color = color if color != 'nan' else None

  return {
    "_id": str(pokemon['number']),
    "name": pokemon['name'],
    "evolutions": evolutions,
    "size": get_float_value(pokemon['size']),
    "weight": get_float_value(pokemon['weight']),
    "types": types,
    "color": color,
    "damage_taken": damage_taken
  }

def insert_all_pokemons (collection):
  print('dropping collection...')
  collection.drop()
  print('collection dropped successfully!\n')

  pokemons_df = pd.read_csv('data/pokemons.csv', header=0, converters={'number': lambda x: str(x)})

  print('inserting pokemons data...')
  for _, pokemon in pokemons_df.iterrows():
    collection.insert_one(get_pokemon_document(pokemon, pokemons_df))
    print(pokemon['number'], ' OK')
