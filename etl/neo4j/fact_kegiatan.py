import petl
import psycopg2
from py2neo import Graph
from py2neo.bulk import create_nodes
from neo4j import GraphDatabase
import os

from dotenv import load_dotenv
load_dotenv()


def fact_kegiatan():
    print("==LOADING FACT KEGIATAN==")

    start_index = 0
    end_index = 100_000

    operasional = psycopg2.connect(
        f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

    table_proyek = petl.fromdb(operasional, "SELECT * FROM kegiatan")
    input_table = petl.rowslice(table_proyek, start_index, end_index)

    graph = Graph("neo4j://localhost:7687/",
                  auth=("neo4j", "@Harris99"), name="datamart")

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["FactKegiatan"])
        print(graph.nodes.match("FactKegiatan").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_proyek, start_index, end_index)

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (proyek:FactProyek) WHERE kegiatan.id_proyek = proyek.id_proyek CREATE (kegiatan)-[r:KEGIATAN_DARI]->(proyek)")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (waktu:DimWaktu) WHERE kegiatan.tanggal_rencana = waktu.tanggal CREATE (kegiatan)-[r:DIRENCANAKAN_PADA]->(waktu)")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (waktu:DimWaktu) WHERE kegiatan.tanggal_pelaksanaan = waktu.tanggal CREATE (kegiatan)-[r:DILAKSANAKAN_PADA]->(waktu)")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (lembaga:DimLembagaPelaksana) WHERE kegiatan.id_lembaga_pelaksana = lembaga.id_lembaga_pelaksana CREATE (lembaga)-[r:MELAKSANAKAN]->(kegiatan)")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (penerima:DimPenerimaManfaat) WHERE kegiatan.id_penerima_manfaat = penerima.id_penerima_manfaat CREATE (kegiatan)-[r:MENARGETKAN]->(penerima)")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (jenis:DimJenisKegiatan) WHERE kegiatan.id_jenis_kegiatan = jenis.id_jenis_kegiatan CREATE (kegiatan)-[r:BERJENIS]->(jenis)")

    # RELASI LOKASI
    graph.run(
        "MATCH (kegiatan:FactKegiatan), (kecamatan:DimKecamatan) WHERE kegiatan.id_kecamatan = kecamatan.id_kecamatan CREATE (kegiatan)-[r:BERADA]->(kecamatan)")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (kota:DimKabKota) WHERE kegiatan.id_kota = kota.id_kab_kota CREATE (kegiatan)-[r:BERADA]->(kota)")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (desa:DimDesaKelurahan) WHERE kegiatan.id_desa = desa.id_desa_kel CREATE (kegiatan)-[r:BERADA]->(desa)")

    start_index = 0
    end_index = 1_000_000

    br_peserta_kegiatan = petl.fromdb(
        operasional, "SELECT * FROM peserta_kegiatan_proyek")
    input_table = petl.rowslice(br_peserta_kegiatan, start_index, end_index)

    absolute_path = os.path.dirname(__file__)
    relative_path = "csv"
    full_path = os.path.join(absolute_path, relative_path)

    print("===LOADING RELASI PESERTA KEGIATAN===")
    while petl.nrows(input_table) > 0:
        print(end_index)
        petl.tocsv(input_table, full_path + "/br_peserta_kegiatan.csv")
        file_url = "file:///" + \
            str(full_path).replace("\\", "/") + "/br_peserta_kegiatan.csv"

        query = f"""
                CALL apoc.periodic.iterate(
                "LOAD CSV WITH HEADERS FROM '{file_url}' AS row return row",
                "MATCH (peserta:DimPeserta {{id_peserta : toInteger(row.id_peserta)}})
                MATCH (pelatihan:FactKegiatan {{id_kegiatan: toInteger(row.id_kegiatan)}})
                CREATE (peserta)-[r:MENGIKUTI]->(pelatihan)",
                {{batchSize : 10000, parallel: true}}
                )
                """
        # print(query)
        graph.run(query)

        start_index = end_index
        end_index += 1_000_000
        input_table = petl.rowslice(
            br_peserta_kegiatan, start_index, end_index)
    # for value in petl.dicts(br_peserta_kegiatan):
    #     id_peserta = value["id_peserta"]
    #     id_kegiatan = value["id_kegiatan"]

    #     print(id_peserta, id_kegiatan)

    #     graph.run(
    #         f"MATCH (peserta:DimPeserta), (kegiatan:FactKegiatan) WHERE peserta.id_peserta = {id_peserta} AND kegiatan.id_kegiatan = {id_kegiatan} CREATE (peserta)-[r:MENGIKUTI]->(kegiatan) return kegiatan, peserta")
