#!/usr/bin/env python

import os
import asyncpg
import asyncio
from asyncpg import Record
from typing import List, Tuple, Union
import random


CREATE_BRAND_TABLE = \
    """
    CREATE TABLE IF NOT EXISTS brand(
    brand_id SERIAL PRIMARY KEY,
    brand_name TEXT NOT NULL
    );"""

CREATE_PRODUCT_TABLE = \
    """
    CREATE TABLE IF NOT EXISTS product(
    product_id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    brand_id INT NOT NULL,
    FOREIGN KEY (brand_id) REFERENCES brand(brand_id)
    );"""

CREATE_PRODUCT_COLOR_TABLE = \
    """
    CREATE TABLE IF NOT EXISTS product_color(
    product_color_id SERIAL PRIMARY KEY,
    product_color_name TEXT NOT NULL
    );"""

CREATE_PRODUCT_SIZE_TABLE = \
    """
    CREATE TABLE IF NOT EXISTS product_size(
    product_size_id SERIAL PRIMARY KEY,
    product_size_name TEXT NOT NULL
    );"""

CREATE_SKU_TABLE = \
    """
    CREATE TABLE IF NOT EXISTS sku(
    sku_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    product_size_id INT NOT NULL,
    product_color_id INT NOT NULL,
    FOREIGN KEY (product_id)
    REFERENCES product(product_id),
    FOREIGN KEY (product_size_id)
    REFERENCES product_size(product_size_id),
    FOREIGN KEY (product_color_id)
    REFERENCES product_color(product_color_id)
    );"""

COLOR_INSERT = \
    """
    INSERT INTO product_color VALUES(1, 'Blue');
    INSERT INTO product_color VALUES(2, 'Black');
    """

SIZE_INSERT = \
    """
    INSERT INTO product_size VALUES(1, 'Small');
    INSERT INTO product_size VALUES(2, 'Medium');
    INSERT INTO product_size VALUES(3, 'Large');
    """

dsns = {
    # asyncpg doesnt like postgresql+psycopg2
  'sql': 'sqlite:///sqlite.db', 
  'dbx1': f'postgresql://dbxdba:{os.getenv("DBX_1_PASSWORD")}@10.34.31.29:6001/druid_db',
  'dbx4': f'postgresql://dbxdba:{os.getenv("DBX_4_PASSWORD")}@10.34.244.80:6001/druid_db',  
  'dbx8': f'postgresql://dbxdba:{os.getenv("DBX_8_PASSWORD")}@10.34.242.3:6001/druid_db',      
    # just two slashes for sqlserver!
  'Canela20': f'mssql+pyodbc://SVCCompassusr:{os.getenv("SQLSERVER_PASSWORD")}@10.33.229.20:14482/Canela20?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes',
  'neh': f'postgresql://dbxdba:{os.getenv("DBX_NEH_PASSWORD")}@10.10.0.254:7578/druid_db',
  #'pg': f'postgresql+psycopg2://tom@localhost:6000/druid_db',
  'pg': f'postgresql://tom@localhost:6000/products',
  }


async def init_db(connection):
    version = connection.get_server_version()
    print(f'Connected! Postgres version is {version}')

    statements = [
        CREATE_BRAND_TABLE,
        CREATE_PRODUCT_TABLE,
        CREATE_PRODUCT_COLOR_TABLE,
        CREATE_PRODUCT_SIZE_TABLE,
        CREATE_SKU_TABLE,
        SIZE_INSERT,
        COLOR_INSERT
        ]
    
    print('Creating the product database...')
    for statement in statements:
        status = await connection.execute(statement)
        print(status)
    print('Finished creating the product database!')


def load_common_words() -> List[str]:
        with open('common_words.txt') as f:
            words = []
            for l in f.readlines():
                words.append(l.strip())
            return words


def generate_brand_names(words: List[str]) -> List[Tuple[Union[str, ]]]:
    return [(words[index],) for index in random.sample(range(100), 100)]

async def insert_brands(connection) -> int:
    common_words = load_common_words()    
    brands = generate_brand_names(common_words)
    insert_brands = "INSERT INTO brand VALUES(DEFAULT, $1)"
    return await connection.executemany(insert_brands, brands)


async def main():
    dsn = dsns['pg']
    connection = await asyncpg.connect(dsn)

    #  await init_db(connection)

    # await connection.execute("INSERT INTO brand VALUES(DEFAULT, 'Levis')")
    # await connection.execute("INSERT INTO brand VALUES(DEFAULT, 'Seven')")
    # brand_query = 'SELECT brand_id, brand_name FROM brand'
    # results: List[Record] = await connection.fetch(brand_query)
    # for brand in results:
    #     print(f'id: {brand["brand_id"]}, name: {brand["brand_name"]}')
    
    await insert_brands(connection)



    await connection.close()



if __name__ == "__main__":
    asyncio.run(main())
