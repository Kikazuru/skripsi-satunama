from dotenv import load_dotenv
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
import petl
import psycopg2
import os

load_dotenv()


def dim_kab_kota(operasional, graph):
    print("==LOADING KAB KOTA==")

    start_index = 0
    end_index = 100_000

    table_kab_kota = petl.fromdb(operasional, "SELECT * FROM kabupaten_kota")
    input_table = petl.rowslice(table_kab_kota, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["DimKabKota"])
        print(graph.nodes.match("DimKabKota").count())

        relation = [
            ((row["id_kab_kota"],), {}, row["id_provinsi"]) for row in input_table
        ]

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_kab_kota, start_index, end_index)

    graph.run(
        "MATCH (provinsi:DimProvinsi), (kota:DimKabKota) WHERE provinsi.id_provinsi = kota.id_provinsi CREATE (kota)-[r:KOTA_DARI]->(provinsi) return provinsi, kota")
