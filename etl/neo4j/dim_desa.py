from dotenv import load_dotenv
from py2neo import Graph
from py2neo.bulk import create_nodes
import petl
import psycopg2
import os

load_dotenv()


def dim_desa(operasional, graph):
    print("==LOADING DESA==")

    start_index = 0
    end_index = 100_000

    table_desa = petl.fromdb(operasional, "SELECT * FROM desa_kelurahan")
    input_table = petl.rowslice(table_desa, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["DimDesaKelurahan"])
        graph.run(
            "CREATE RANGE INDEX dimDesa IF NOT EXISTS FOR (desa:DimDesaKelurahan) on (desa.id_desa, desa.nama_desa)")

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_desa, start_index, end_index)

    graph.run(
        "MATCH (kecamatan:DimKecamatan), (desa:DimDesaKelurahan) WHERE kecamatan.id_kecamatan = desa.id_kecamatan CREATE (desa)-[r:DESA_DARI]->(kecamatan) return desa, kecamatan")
