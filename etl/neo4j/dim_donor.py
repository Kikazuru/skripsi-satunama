from dotenv import load_dotenv
from py2neo import Graph
from py2neo.bulk import create_nodes
import petl
import psycopg2
import os

load_dotenv()


def dim_donor():
    print("==LOADING DONOR==")

    operasional = psycopg2.connect(
        f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

    start_index = 0
    end_index = 100_000

    table_donor = petl.fromdb(
        operasional, "select * from donor")
    input_table = petl.rowslice(table_donor, start_index, end_index)

    graph = Graph("neo4j://localhost:7687/",
                  auth=("neo4j", "@Harris99"), name="datamart")

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["DimDonor"])
        print(graph.nodes.match("DimDonor").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_donor, start_index, end_index)