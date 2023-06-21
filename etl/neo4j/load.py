import psycopg2
import os
from dotenv import load_dotenv
from job import job

from py2neo import Graph

load_dotenv()

dbname = f'{os.getenv("DBNAME_OP")}'
operasional = psycopg2.connect(
    f'host={os.getenv("DBHOST_OP")} dbname={dbname} user={os.getenv("DBUSER_OP")} password={os.getenv("DBPASS_OP")}')

uri = os.getenv("DBURI_DM_NEO4J")
dbname = f'{os.getenv("DBNAME_DM_NEO4J")}'
graph = Graph(uri,
                auth=(os.getenv("DBUSER_DM_NEO4J"), os.getenv("DBPASS_DM_NEO4J")), name=dbname)

job(operasional, graph)