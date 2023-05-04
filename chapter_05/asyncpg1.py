#!/usr/bin/env python

import os
import asyncpg
import asyncio
from asyncpg import Record
from typing import List, Tuple, Union
import random

from util import async_timed


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
    FOREIGN KEY (product_id) REFERENCES product(product_id),
    FOREIGN KEY (product_size_id) REFERENCES product_size(product_size_id),
    FOREIGN KEY (product_color_id) REFERENCES product_color(product_color_id)
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

def generate_brand_names(words: List[str], id_start: int, id_end: int) -> List[Tuple[Union[str, int]]]:
    # return [(words[index],) for index in random.sample(range(id_start, id_end+1), count)]
    brands = []
    for pkey in range(id_start, id_end+1):
        brand = words[random.randint(0, 1000)]  # Choose from first 1001 words
        brands.append((pkey, brand))
    return brands

async def insert_brands(connection) -> None:
    # brand_id SERIAL PRIMARY KEY,
    # brand_name TEXT NOT NULL    
    words = load_common_words()    
    id_start = 1
    id_end = 100
    tuples = generate_brand_names(words, id_start, id_end)
    sql = "INSERT INTO brand VALUES($1, $2)"
    await connection.executemany(sql, tuples)


def generate_products(words: List[str], fkey_start: int, fkey_end: int, count: int) -> List[Tuple[int, str, int]]:
    products = []
    for pkey in range(1, count+1):
        # 10 random words per description (random.sample() samples w/o replacement)
        description = " ".join([words[index] for index in random.sample(range(1000), 10)])
        # brand_id is randomly selected from an integer range
        brand_id = random.randint(fkey_start, fkey_end)  # closed
        # Each product is a tuple(str, int)
        products.append((pkey, description, brand_id))
    return products

async def insert_products(connection) -> None:
    # product_id SERIAL PRIMARY KEY,
    # product_name TEXT NOT NULL,
    # brand_id INT NOT NULL,   FKEY 
    words = load_common_words()
    brand_id_start = 1 
    brand_id_end = 100
    product_count = 1000
    tuples = generate_products(words, brand_id_start, brand_id_end, product_count)
    sql = "INSERT INTO product VALUES($1, $2, $3)"
    await connection.executemany(sql, tuples)  

def generate_skus(fkey_start: int, fkey_end: int, count: int) -> List[Tuple[int, int, int, int]]:
    skus = []
    for pkey in range(1, count+1):
        # create random integers from a range
        product_id = random.randint(fkey_start, fkey_end)  # random.randint is closed, np.random.randint is half-open?
        size_id = random.randint(1, 3)  # 1-3 from SIZE_INSERT
        color_id = random.randint(1, 2) # 1-2 from COLOR_INSERT
        skus.append((pkey, product_id, size_id, color_id))
    return skus

async def insert_skus(connection) -> None:
    # sku_id SERIAL PRIMARY KEY,
    # product_id INT NOT NULL,       # FKEY
    # product_size_id INT NOT NULL,  # FKEY
    # product_color_id INT NOT NULL, # FKEY
    product_id_start = 1
    product_id_end = 1000
    sku_count = 100000
    tuples =  generate_skus(product_id_start, product_id_end, sku_count)
    sql = "INSERT INTO sku VALUES($1, $2, $3, $4)"
    await connection.executemany(sql, tuples) 

async def truncate_tables(connection) -> None:
    await connection.execute("TRUNCATE TABLE brand CASCADE; TRUNCATE TABLE product CASCADE; TRUNCATE TABLE sku CASCADE")

async def initialize_tables(dsn):
    ## Insert a few rows and readback using execute() and fetch()/fetchone()
    # await connection.execute("INSERT INTO brand VALUES(DEFAULT, 'Levis')")
    # await connection.execute("INSERT INTO brand VALUES(DEFAULT, 'Seven')")
    # brand_query = 'SELECT brand_id, brand_name FROM brand'
    # results: List[Record] = await connection.fetch(brand_query)  # fetchone
    # for brand in results:
    #     print(f'id: {brand["brand_id"]}, name: {brand["brand_name"]}')

    connection = await asyncpg.connect(dsn)

    ## Create tables using execute()
    #  await init_db(connection)

    # Populate tables
    print("Populate tables")    
    random.seed(42)
    print("Truncate tables")
    await truncate_tables(connection)
    print("Load brands")
    await insert_brands(connection)
    print("Load products")
    await insert_products(connection)
    print("Load skus")
    await insert_skus(connection)    

    await connection.close()

def product_query_1() -> str:
    qry = \
        """
        SELECT
        p.product_id,
        p.product_name,
        p.brand_id,
        s.sku_id,
        pc.product_color_name,
        ps.product_size_name
        FROM product as p
        JOIN sku as s on s.product_id = p.product_id    
        JOIN product_color as pc on pc.product_color_id = s.product_color_id
        JOIN product_size as ps on ps.product_size_id = s.product_size_id
        WHERE p.product_id = 100
        """
    return qry

async def query_product(qry, pool):
    async with pool.acquire() as connection:
        rsp = await connection.fetchrow(qry)
        print(rsp)
        return rsp


@async_timed()
async def query_products_synchronously(pool, queries):
    return [await query_product(pool) for _ in range(queries)]

@async_timed()
async def query_products_concurrently(pool, queries):
    queries = [query_product(pool) for _ in range(queries)]
    return await asyncio.gather(*queries)

async def main():
    dsn = dsns['pg']

    #  await initialize_tables(dsn)

    # sqlalchemy.engine is also a connection pool
    async with asyncpg.create_pool(dsn=dsn, min_size=6, max_size=6) as pool:
        # qry = product_query_1()
        # await asyncio.gather(
        #     query_product(qry, pool),
        #     query_product(qry, pool),
        # )

        await query_products_synchronously(pool, 10000)
        await query_products_concurrently(pool, 10000)
    



if __name__ == "__main__":
    asyncio.run(main())


