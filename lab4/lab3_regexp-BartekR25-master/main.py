import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');




def film_in_category(category:Union[int,str])->pd.DataFrame:

    if isinstance(category, int):
        df = f"""SELECT film.title, language.name as languge, category.name as category
                    FROM film
                    INNER JOIN language ON language.language_id = film.language_id
                    INNER JOIN film_category ON film_category.film_id = film.film_id
                    INNER JOIN category ON category.category_id = film_category.category_id
                    WHERE category.category_id = {category}
                    ORDER BY film.title, languge"""
    elif isinstance(category, str):
        df = f"""SELECT film.title, l.name as languge, fl.category
                    FROM film
                    INNER JOIN film_list fl ON fl.fid = film.film_id
                    INNER JOIN language l ON l.language_id = film.language_id
                    WHERE fl.category = '{category}'
                    ORDER BY film.title, languge"""
    else:
        return None

    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str, dokładnie taki jak podana wartość
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)




    
def film_in_category_case_insensitive(category:Union[int,str])->pd.DataFrame:

    if isinstance(category, int):
        df = f"""SELECT film.title, language.name as languge, category.name as category
                    FROM film
                    INNER JOIN language ON language.language_id = film.language_id
                    INNER JOIN film_category ON film_category.film_id = film.film_id
                    INNER JOIN category ON category.category_id = film_category.category_id
                    WHERE category.category_id = {category}
                    ORDER BY film.title, languge"""
    elif isinstance(category, str):
        df = f"""SELECT film.title, l.name as languge, fl.category
                    FROM film
                    INNER JOIN film_list fl ON fl.fid = film.film_id
                    INNER JOIN language l ON l.language_id = film.language_id
                    WHERE fl.category ILIKE '{category}'
                    ORDER BY film.title, languge"""
    else:
        return None


    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)






def film_cast(title:str)->pd.DataFrame:

    if not isinstance(title, str):
        return None

    df = f"""SELECT a.first_name, a.last_name
            FROM actor a
            INNER JOIN film_actor fa ON fa.actor_id = a.actor_id
            INNER JOIN film f ON f.film_id = fa.film_id
            WHERE f.title LIKE '{title}'
            ORDER BY a.last_name, a.first_name"""


    ''' Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |
    |0	|Greg       |Chaplin    | 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    title (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)




def film_title_case_insensitive(words:list) :

    if not isinstance(words, list):
        return None

    words_str = '|'.join(words)

    df = f"""SELECT film.title
            FROM film
            WHERE title ~* '(?:^| )({words_str})""" + """{1,}(?:$| )'
            ORDER BY film.title
            """




    ''' Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
    Przykład wynikowej tabeli:
    |   |title              |
    |0	|Crystal Breaking 	| 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    words(list): wartość minimalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)