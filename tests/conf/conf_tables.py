import uuid


def get_test_tables():
    return {
        'simple': get_simple_table(),
        'pokemon': get_pokemon_table(),
    }


def get_simple_table(random_name=False):
    name = 'simple'

    if random_name:
        name = name + str(uuid.uuid4()).replace('-', '')

    return {
        'schema': {
            'name': name,
            'columns': {
                'id': {'type': int, 'primary': True},
                'name': {'type': str, 'nullable': False},
                'rating': {'type': str, 'default': 'alright'},
                'raw_data': {'type': bytes},
                'completed': {'type': bool},
            },
        },
        'rows': [
            (5, 'this', 'alright', b'\xde\xad\xbe\xef', True),
            (6, 'is', 'incredible', b'', True),
            (7, 'a', 'awful', b'\xbe\xef', False),
            (8, 'test', 'alright', b'\xbe\xef', False),
        ],
        'indices': [],
        'constraints': [],
    }


def get_pokemon_table(random_name=False):
    # from
    # - https://pokemondb.net/pokedex/all
    # - https://pokemondb.net/pokedex/stats/height-weight

    name = 'pokemon'

    if random_name:
        name = name + str(uuid.uuid4()).replace('-', '')

    return {
        'schema': {
            'name': name,
            'columns': {
                'id': {'primary': True, 'type': int},
                'name': {'type': str, 'unique': True},
                'primary_type': {'type': str, 'index': True},
                'total_stats': int,
                'hp': int,
                'attack': int,
                'defense': int,
                'special_attack': int,
                'special_defense': int,
                'speed': int,
                'all_types': dict,
                'height': float,
                'weight_lbs': float,
                'weight_kgs': float,
                'bmi': float,
            },
            'indices': [],
            'constraints': [],
        },
        'rows': [
            (1, 'Bulbasaur', 'GRASS', 318, 45, 49, 49, 65, 65, 45, ['GRASS', 'POISON'], 0.7, 15.2, 6.9, 14.1),
            (2, 'Ivysaur', 'GRASS', 405, 60, 62, 63, 80, 80, 60, ['GRASS', 'POISON'], 1.0, 28.7, 13.0, 13.0),
            (3, 'Venusaur', 'GRASS', 525, 80, 82, 83, 100, 100, 80, ['GRASS', 'POISON'], 2.0, 220.5, 100.0, 25.0),
            (4, 'Charmander', 'FIRE', 309, 39, 52, 43, 60, 50, 65, ['FIRE'], 0.6, 18.7, 8.5, 23.6),
            (5, 'Charmeleon', 'FIRE', 405, 58, 64, 58, 80, 65, 80, ['FIRE'], 1.1, 41.9, 19.0, 15.7),
            (6, 'Charizard', 'FIRE', 534, 78, 84, 78, 109, 85, 100, ['FIRE', 'FLYING'], 1.7, 199.5, 90.5, 31.3),
            (7, 'Squirtle', 'WATER', 314, 44, 48, 65, 50, 64, 43, ['WATER'], 0.5, 19.8, 9.0, 36.0),
            (8, 'Wartortle', 'WATER', 405, 59, 63, 80, 65, 80, 58, ['WATER'], 1.0, 49.6, 22.5, 22.5),
            (9, 'Blastoise', 'WATER', 530, 79, 83, 100, 85, 105, 78, ['WATER'], 1.6, 188.5, 85.5, 33.4),
            (10, 'Caterpie', 'BUG', 195, 45, 30, 35, 20, 20, 45, ['BUG'], 0.3, 6.4, 2.9, 32.2),
            (11, 'Metapod', 'BUG', 205, 50, 20, 55, 25, 25, 30, ['BUG'], 0.7, 21.8, 9.9, 20.2),
            (12, 'Butterfree', 'BUG', 395, 60, 45, 50, 90, 80, 70, ['BUG', 'FLYING'], 1.1, 70.5, 32.0, 26.4),
            (13, 'Weedle', 'BUG', 195, 40, 35, 30, 20, 20, 50, ['BUG', 'POISON'], 0.3, 7.1, 3.2, 35.6),
            (14, 'Kakuna', 'BUG', 205, 45, 25, 50, 25, 25, 35, ['BUG', 'POISON'], 0.6, 22.0, 10.0, 27.8),
            (15, 'Beedrill', 'BUG', 395, 65, 90, 40, 45, 80, 75, ['BUG', 'POISON'], 1.0, 65.0, 29.5, 29.5),
            (16, 'Pidgey', 'NORMAL', 251, 40, 45, 40, 35, 35, 56, ['NORMAL', 'FLYING'], 0.3, 4.0, 1.8, 20.0),
            (17, 'Pidgeotto', 'NORMAL', 349, 63, 60, 55, 50, 50, 71, ['NORMAL', 'FLYING'], 1.1, 66.1, 30.0, 24.8),
            (18, 'Pidgeot', 'NORMAL', 479, 83, 80, 75, 70, 70, 101, ['NORMAL', 'FLYING'], 1.5, 87.1, 39.5, 17.6),
            (19, 'Rattata', 'NORMAL', 253, 30, 56, 35, 25, 35, 72, ['NORMAL'], 0.3, 7.7, 3.5, 38.9),
            (20, 'Raticate', 'NORMAL', 413, 55, 81, 60, 50, 70, 97, ['NORMAL'], 0.7, 40.8, 18.5, 37.8),
            (21, 'Spearow', 'NORMAL', 262, 40, 60, 30, 31, 31, 70, ['NORMAL', 'FLYING'], 0.3, 4.4, 2.0, 22.2),
            (22, 'Fearow', 'NORMAL', 442, 65, 90, 65, 61, 61, 100, ['NORMAL', 'FLYING'], 1.2, 83.8, 38.0, 26.4),
            (23, 'Ekans', 'POISON', 288, 35, 60, 44, 40, 54, 55, ['POISON'], 2.0, 15.2, 6.9, 1.7),
            (24, 'Arbok', 'POISON', 448, 60, 95, 69, 65, 79, 80, ['POISON'], 3.5, 143.3, 65.0, 5.3),
            (25, 'Pikachu', 'ELECTRIC', 320, 35, 55, 40, 50, 50, 90, ['ELECTRIC'], 0.4, 13.2, 6.0, 37.5),
            (26, 'Raichu', 'ELECTRIC', 485, 60, 90, 55, 90, 80, 110, ['ELECTRIC'], 0.8, 66.1, 30.0, 46.9),
            (27, 'Sandshrew', 'GROUND', 300, 50, 75, 85, 20, 30, 40, ['GROUND'], 0.6, 26.5, 12.0, 33.3),
            (28, 'Sandslash', 'GROUND', 450, 75, 100, 110, 45, 55, 65, ['GROUND'], 1.0, 65.0, 29.5, 29.5),
            (29, 'Nidoran♀', 'POISON', 275, 55, 47, 52, 40, 40, 41, ['POISON'], 0.4, 15.4, 7.0, 43.8),
            (30, 'Nidorina', 'POISON', 365, 70, 62, 67, 55, 55, 56, ['POISON'], 0.8, 44.1, 20.0, 31.3),
            (31, 'Nidoqueen', 'POISON', 505, 90, 92, 87, 75, 85, 76, ['POISON', 'GROUND'], 1.3, 132.3, 60.0, 35.5),
            (32, 'Nidoran♂', 'POISON', 273, 46, 57, 40, 40, 40, 50, ['POISON'], 0.5, 19.8, 9.0, 36.0),
            (33, 'Nidorino', 'POISON', 365, 61, 72, 57, 55, 55, 65, ['POISON'], 0.9, 43.0, 19.5, 24.1),
            (34, 'Nidoking', 'POISON', 505, 81, 102, 77, 85, 75, 85, ['POISON', 'GROUND'], 1.4, 136.7, 62.0, 31.6),
            (35, 'Clefairy', 'FAIRY', 323, 70, 45, 48, 60, 65, 35, ['FAIRY'], 0.6, 16.5, 7.5, 20.8),
            (36, 'Clefable', 'FAIRY', 483, 95, 70, 73, 95, 90, 60, ['FAIRY'], 1.3, 88.2, 40.0, 23.7),
            (37, 'Vulpix', 'FIRE', 299, 38, 41, 40, 50, 65, 65, ['FIRE'], 0.6, 21.8, 9.9, 27.5),
            (38, 'Ninetales', 'FIRE', 505, 73, 76, 75, 81, 100, 100, ['FIRE'], 1.1, 43.9, 19.9, 16.4),
            (39, 'Jigglypuff', 'NORMAL', 270, 115, 45, 20, 45, 25, 20, ['NORMAL', 'FAIRY'], 0.5, 12.1, 5.5, 22.0),
            (40, 'Wigglytuff', 'NORMAL', 435, 140, 70, 45, 85, 50, 45, ['NORMAL', 'FAIRY'], 1.0, 26.5, 12.0, 12.0),
        ],
    }


def get_weather_table(random_name=False):
    name = 'weather'

    if random_name:
        name = name + str(uuid.uuid4()).replace('-', '')

    return {
        'schema': {
            'name': name,
            'columns': {
                'year': {'type': 'Integer', 'primary': True},
                'month': {'type': 'Integer', 'primary': True},
                'rainfall': {'type': 'Float'},
                'temperature': {'type': 'Float'},
            },
        },
        'rows': [
            (2020, 1, 8.2, 90),
            (2020, 5, 1.1, 50),
            (2020, 9, 7.4, 60),
            (2021, 1, 5.2, 72),
            (2021, 5, 2.1, 56),
            (2021, 9, 6.4, 68),
            (2022, 1, 1.2, 70),
            (2022, 5, 4.1, 56),
            (2022, 9, 9.4, 60),
        ],
    }


def get_history_table(random_name=False):
    name = 'history'

    if random_name:
        name = name + str(uuid.uuid4()).replace('-', '')

    return {
        'schema': {
            'name': name,
            'columns': {
                'id': {'type': int, 'primary': True},
                'year': int,
                'month': int,
                'country': str,
                'condition': str,
            },
            'indices': [
                {
                    'columns': ['year', 'month', 'country'],
                    'unique': True,
                },
            ],
        },
        'rows': [
            (0, 1066, 0, 'england', 'not conquered'),
            (1, None, 0, 'england', 'english'),
            (2, 1066, None, 'england', 'being conquered'),
            (3, 1066, 0, None, 'medieval'),
            (4, 1067, 0, 'england', 'conquered'),
            (5, 1066, 1, 'england', 'cold'),
            (6, 1066, 0, 'wales', 'neighborly'),
        ],
        'conflict_rows': [
            (10, 1066, 0, 'england', 'what'),
            (11, None, 0, 'england', 'what'),
            (12, 1066, None, 'england', 'what'),
            (13, 1066, 0, None, 'what'),
            (14, 1067, 0, 'england', 'what'),
            (15, 1066, 1, 'england', 'what'),
            (16, 1066, 0, 'wales', 'what'),
        ],
    }

