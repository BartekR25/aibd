import numpy as np
import pickle
import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');





def film_in_category(category_id:int)->pd.DataFrame:
    
    if not isinstance(category_id, int):
        return None

    df =        f'''SELECT film.title, language.name as language, film_list.category as category
                    FROM film
                    INNER JOIN film_list ON film_list.fid = film.film_ID
                    INNER JOIN language ON language.language_id = film.language_id
                    INNER JOIN film_category ON film_category.film_id = film.film_id
                    WHERE film_category.category_id = {category_id}
                    ORDER BY title'''


    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)







def number_films_in_category(category_id:int)->pd.DataFrame:

    if not isinstance(category_id, int):
        return None

    df = f'''SELECT category, COUNT(title) as count
            FROM film_list
            INNER JOIN film_category ON film_category.film_id = film_list.fid
            WHERE film_category.category_id = {category_id}
            GROUP BY category'''


    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)







def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :

    if not isinstance(min_length, (int, float)):
        return None
    if not isinstance(max_length, (int, float)):
        return None
    if min_length > max_length:
        return None

    df = f'''SELECT length, COUNT(title)
            FROM film
            WHERE length BETWEEN {min_length} AND {max_length}
            GROUP BY length'''

    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)






'''NIEE DZIAŁA'''
def client_from_city(city:str)->pd.DataFrame:

    if not isinstance(city, str):
        return None

    df = f"""SELECT city.city, c.first_name, c.last_name
            FROM city
            INNER JOIN address ON address.city_id = city.city_id
            INNER JOIN customer c ON c.address_id = address.address_id
            WHERE city.city = '{city}'"""

    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)

print(client_from_city("London"))





def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:

    if not isinstance(length, (int, float)):
        return None

    df = f'''SELECT film.length, AVG(p.amount)
            FROM film
            INNER JOIN inventory i ON i.film_id = film.film_id
            INNER JOIN rental r ON r.inventory_id = i.inventory_id
            INNER JOIN payment p ON p.rental_id = r.rental_id
            WHERE film.length = {length}
            GROUP BY film.length'''

    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389
    
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)







def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:


    if not isinstance(sum_min, (int, float)):
        return None

    if sum_min < 0:
        return None

    df = f'''SELECT c.first_name, c.last_name, SUM(film.length)
            FROM film
            INNER JOIN inventory i ON i.film_id = film.film_id
            INNER JOIN rental r ON r.inventory_id = i.inventory_id
            INNER JOIN customer c ON c.customer_id = r.customer_id
            GROUP BY c.first_name, c.last_name
            HAVING SUM(film.length) > {sum_min}
            ORDER BY SUM(film.length) DESC'''


    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265
    
    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)







def category_statistic_length(name:str)->pd.DataFrame:


    if not isinstance(name, str):
        return None

    df = f'''SELECT fl.category, AVG(film.length), SUM(film.length), MIN(film.length), MAX(film.length)
            FROM film
            INNER JOIN film_list fl ON fl.fid = film.film_id
            WHERE fl.category = '{name}'
            GROUP BY fl.category'''


    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return pd.read_sql_query(df, con=connection)