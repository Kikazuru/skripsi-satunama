from dotenv import load_dotenv
from py2neo import Graph
from py2neo.bulk import create_nodes
import petl
import psycopg2
import os

load_dotenv()

def dim_pekerja(operasional, graph):
    print("==LOADING PEKERJA==")
        
    start_index = 0
    end_index = 100_000

    table_pekerja = petl.fromdb(
        operasional, "select * from pekerja")
    input_table = petl.rowslice(table_pekerja, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["DimPekerja"])
        print(graph.nodes.match("DimPekerja").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_pekerja, start_index, end_index)
