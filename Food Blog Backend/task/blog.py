from sqlite3 import Connection, Cursor
import sqlite3
from typing import List, Tuple, Union
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("database")
parser.add_argument("--ingredients")
parser.add_argument("--meals")
args = parser.parse_args()

data = {
    "meals": ("breakfast", "brunch", "lunch", "supper"),
    "ingredients": ("milk", "cacao", "strawberry", "blueberry", "blackberry", "sugar"),
    "measures": ("ml", "g", "l", "cup", "tbsp", "tsp", "dsp", "")
}


def turn_on_foreign_key(cur: Cursor, con: Connection):
    cur.execute('''
        PRAGMA foreign_key = ON
    ''')
    con.commit()


def create_serve_table(cur: Cursor, con: Connection):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS serve (
            serve_id INTEGER PRIMARY KEY,
            meal_id INTEGER NOT NULL,
            recipe_id INTEGER NOT NULL,
            FOREIGN KEY (meal_id)
                REFERENCES meals(meal_id),
            FOREIGN KEY (recipe_id)
                REFERENCES recipes(recipe_id)
        ) 
    ''')
    con.commit()


def create_quantity_table(cur: Cursor, con: Connection):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS quantity (
            quantity_id INTEGER PRIMARY KEY,
            quantity INTEGER NOT NULL,
            recipe_id INTEGER NOT NULL,
            measure_id INTEGER NOT NULL,
            ingredient_id INTEGER NOT NULL,
            FOREIGN KEY (recipe_id)
                REFERENCES recipes(recipe_id),
            FOREIGN KEY (measure_id)
                REFERENCES measures(measure_id),
            FOREIGN KEY (ingredient_id)
                REFERENCES ingredients(ingredient_id)
        )
    ''')
    con.commit()


def create_table(cur: Cursor, con: Connection):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS measures (
            measure_id INTEGER PRIMARY KEY,
            measure_name TEXT UNIQUE
        )
        """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ingredients (
           ingredient_id INTEGER PRIMARY KEY,
           ingredient_name TEXT NOT NULL UNIQUE
        )
        """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meals (
            meal_id INTEGER PRIMARY KEY,
            meal_name TEXT NOT NULL UNIQUE
        )
        """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipes (
            recipe_id INTEGER PRIMARY KEY,
            recipe_name TEXT NOT NULL,
            recipe_description TEXT
        )
        """)
    con.commit()
    turn_on_foreign_key(cur=cur, con=con)
    create_serve_table(cur=cur, con=con)
    create_quantity_table(cur=cur, con=con)


def insert_data(cur: Cursor, con: Connection):
    cur.executemany('''
        INSERT INTO 
            measures (measure_name)
        VALUES (?)
        ''', [(x,) for x in data['measures']])
    cur.executemany('''
        INSERT INTO 
            ingredients (ingredient_name)
        VALUES (?)
        ''', [(x,) for x in data['ingredients']])
    cur.executemany('''
        INSERT INTO 
            meals (meal_name)
        VALUES (?)
        ''', [(x,) for x in data['meals']])
    con.commit()


def get_recipe_data(cur: Cursor, con: Connection):
    print('Pass the empty recipe name to exit.')
    while True:
        name: str = input('Recipe name:')
        if not name:
            break
        description: str = input('Recipe description:')
        cur.execute('''
            INSERT INTO
                recipes (
                    recipe_name, 
                    recipe_description
                )
            VALUES (?, ?)
            ''', (name, description))
    con.commit()


def get_serve_and_quantity_data(cur: Cursor, con: Connection):
    print('Pass empty recipe name to exit.')
    while True:
        name: str = input('Recipe name:')
        if not name:
            break
        description: str = input('Recipe description:')
        recipe_id = cur.execute('''
            INSERT INTO
                recipes (recipe_name, recipe_description)
            VALUES (?, ?)
        ''', [name, description]).lastrowid
        meals = cur.execute('''
            SELECT
                *
            FROM
                meals
        ''').fetchall()
        print('  '.join([f'{meal_id}) {meal_name}' for meal_id, meal_name in meals]))
        meals_id = [int(meal_id) for meal_id in input('When the dish can be served:').split()]
        cur.executemany('''
            INSERT INTO
                serve (
                    meal_id,
                    recipe_id
                )
            VALUES (?, ?)
        ''', [(meal_id, recipe_id) for meal_id in meals_id])
        con.commit()
        quantity_data = list()
        while True:
            user_input: List[str] = input('Input quantity of ingredient <press enter to stop>:').split()
            if not user_input:
                break
            quantity = list()
            if len(user_input) == 2:
                quantity.append(int(user_input[0]))
                quantity.append("")
                quantity.append(user_input[1])
            elif len(user_input) == 3:
                quantity.append(int(user_input[0]))
                quantity.append(user_input[1])
                quantity.append(user_input[2])
            else:
                raise ValueError('the number of arguments is not correct')
            measures = cur.execute('''
                SELECT
                    measure_id
                FROM
                    measures
                WHERE
                    measure_name LIKE ?
            ''', (f'{quantity[1]}{"%" if quantity[1] else ""}',)).fetchall()
            ingredients = cur.execute('''
                SELECT
                    ingredient_id
                FROM
                    ingredients
                WHERE
                    ingredient_name LIKE ?
            ''', (f'{quantity[2]}%',)).fetchall()
            if len(measures) != 1:
                print('The measure is not conclusive!')
                continue
            if len(ingredients) != 1:
                print('The ingredient is not conclusive')
                continue
            quantity_data.append((quantity[0], recipe_id, measures[0][0], ingredients[0][0], ))
        cur.executemany('''
            INSERT INTO
                quantity (
                    quantity,
                    recipe_id,
                    measure_id,
                    ingredient_id
                )
            VALUES (?, ?, ?, ?)
        ''', quantity_data)
        con.commit()


def search_recipes(cur: Cursor, con: Connection):
    ingredients = args.ingredients.split(',')
    meals = args.meals.split(',')
    print(ingredients, meals)
    sql: str = '''
        WITH 
            meals_recipes AS (
                SELECT
                    meals.meal_id,
                    recipes.recipe_id,
                    recipes.recipe_name
                FROM
                    serve    
                JOIN
                    meals
                ON
                    serve.meal_id = meals.meal_id
                JOIN
                    recipes
                ON
                    serve.recipe_id = recipes.recipe_id
                WHERE
                    meals.meal_name IN {meals}
            ),
            recipes_ingredients AS (
                SELECT DISTINCT 
                    ingredients.ingredient_id,
                    recipes.recipe_id,
                    recipes.recipe_name,
                    1 AS num
                FROM
                    quantity
                JOIN
                    ingredients
                ON
                    quantity.ingredient_id = ingredients.ingredient_id
                JOIN
                    recipes
                ON
                    quantity.recipe_id = recipes.recipe_id
                WHERE
                    ingredients.ingredient_name IN {ingredients}
            ),
        recipes_selected AS (
            SELECT
                recipe_id,
                recipe_name
            FROM
                recipes_ingredients
            GROUP BY 
                recipe_id,
                recipe_name
            HAVING
                SUM(num) = {siz}
        )
        SELECT
            recipes_selected.recipe_name
        FROM
            recipes_selected
        JOIN
            meals_recipes
        ON
            recipes_selected.recipe_id = meals_recipes.recipe_id
    '''.format(
        meals="(" + ", ".join(["'" + meal + "'" for meal in meals]) + ")",
        ingredients="(" + ", ".join(["'" + ingredient + "'" for ingredient in ingredients]) + ")",
        siz=len(ingredients),
    )
    print(sql)
    recipes = cur.execute(sql).fetchall()
    if not recipes:
        print('There are no such recipes in the database.')
    else:
        print(f'Recipes selected for you: {", ".join([x[0] for x in recipes])}')


data_base_name = args.database
con: Connection = sqlite3.connect(data_base_name)
cur: Cursor = con.cursor()
if not args.ingredients and not args.meals:
    create_table(cur=cur, con=con)
    insert_data(cur=cur, con=con)
    # get_recipe_data(cur=cur, con=con)
    get_serve_and_quantity_data(cur=cur, con=con)
else:
    search_recipes(cur=cur, con=con)
con.close()
