from dotenv import load_dotenv
from py2neo import Graph
from py2neo.bulk import create_nodes
import petl
import psycopg2
import os

load_dotenv()


def dim_isu(operasional, graph):
    print("==LOADING ISU==")

    start_index = 0
    end_index = 100_000

    table_isu = petl.fromdb(operasional, "SELECT * FROM isu")
    input_table = petl.rowslice(table_isu, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["DimIsu"])
        print(graph.nodes.match("DimIsu").count())
        graph.run(
            "CREATE RANGE INDEX dimIsu IF NOT EXISTS FOR (isu:DimIsu) on (isu.id_isu, isu.nama_isu)")

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_isu, start_index, end_index)
