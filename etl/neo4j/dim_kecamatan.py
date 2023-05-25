from dotenv import load_dotenv
from py2neo import Graph
from py2neo.bulk import create_nodes
import petl
import psycopg2
import os

load_dotenv()

def dim_kecamatan():
    print("==LOADING KECAMATAN==")

    start_index = 0
    end_index = 100_000

    operasional = psycopg2.connect(
        f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

    table_kecamatan = petl.fromdb(operasional, "SELECT * FROM kecamatan")
    input_table = petl.rowslice(table_kecamatan, start_index, end_index)

    graph = Graph("neo4j://localhost:7687/",
                auth=("neo4j", "@Harris99"), name="datamart")

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["DimKecamatan"])
        print(graph.nodes.match("DimKecamatan").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_kecamatan, start_index, end_index)

    graph.run(
        "MATCH (kecamatan:DimKecamatan), (kota:DimKabKota) WHERE kecamatan.id_kab_kota = kota.id_kab_kota CREATE (kecamatan)-[r:KECAMATAN_DARI]->(kota) return kota, kecamatan")
