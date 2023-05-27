from dotenv import load_dotenv
from py2neo import Graph
from py2neo.bulk import create_nodes
import petl
import psycopg2
import os

load_dotenv()

def dim_negara(operasional, graph):
    print("==LOADING NEGARA==")

    start_index = 0
    end_index = 100_000

    table_negara = petl.fromdb(operasional, "SELECT * FROM negara")
    input_table = petl.rowslice(table_negara, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["DimNegara"])
        print(graph.nodes.match("DimNegara").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_negara, start_index, end_index)
