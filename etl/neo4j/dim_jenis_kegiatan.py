from dotenv import load_dotenv
from py2neo import Graph
from py2neo.bulk import create_nodes
import petl
import psycopg2
import os

load_dotenv()

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

start_index = 0
end_index = 100_000

table_jenis_kegiatan = petl.fromdb(operasional, "SELECT * FROM jenis_kegiatan")
input_table = petl.rowslice(table_jenis_kegiatan, start_index, end_index)

graph = Graph("neo4j://localhost:7687/",
              auth=("neo4j", "@Harris99"), name="datamart")

while petl.nrows(input_table) > 0:
    input_table = petl.dicts(input_table)

    create_nodes(graph.auto(), input_table, ("DimJenisKegiatan",
                                             "id_jenis_kegiatan", "jenis_kegiatan"))
    print(graph.nodes.match("DimJenisKegiatan").count())

    start_index = end_index
    end_index += 100_000
    input_table = petl.rowslice(table_jenis_kegiatan, start_index, end_index)
