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
        "MATCH (kegiatan:FactKegiatan), (proyek:FactProyek) WHERE kegiatan.id_proyek = proyek.id_proyek CREATE (kegiatan)-[r:KEGIATAN_DARI]->(proyek) return proyek, kegiatan")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (waktu:DimWaktu) WHERE kegiatan.tanggal_rencana = waktu.tanggal CREATE (kegiatan)-[r:DIRENCANAKAN_PADA]->(waktu) return waktu, kegiatan")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (waktu:DimWaktu) WHERE kegiatan.tanggal_pelaksanaan = waktu.tanggal CREATE (kegiatan)-[r:DILAKSANAKAN_PADA]->(waktu) return waktu, kegiatan")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (lembaga:DimLembagaPelaksana) WHERE kegiatan.id_lembaga_pelaksana = lembaga.id_lembaga_pelaksana CREATE (lembaga)-[r:MELAKSANAKAN]->(kegiatan) return lembaga, kegiatan")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (penerima:DimPenerimaManfaat) WHERE kegiatan.id_penerima_manfaat = penerima.id_penerima_manfaat CREATE (kegiatan)-[r:MENARGETKAN]->(penerima) return penerima, kegiatan")

    graph.run(
        "MATCH (kegiatan:FactKegiatan), (jenis:DimJenisKegiatan) WHERE kegiatan.id_jenis_kegiatan = jenis.id_jenis_kegiatan CREATE (kegiatan)-[r:BERJENIS]->(jenis) return jenis, kegiatan")

    # RELASI LOKASI
    start_index = 0
    end_index = 10_000

    br_peserta_kegiatan = petl.fromdb(
        operasional, "SELECT * FROM peserta_kegiatan_proyek")

    input_table = petl.rowslice(br_peserta_kegiatan, start_index, end_index)

    print("===LOADING RELASI PESERTA KEGIATAN===")
    uri = "neo4j://localhost:7687/"
    with GraphDatabase.driver(uri, auth=("neo4j", "@Harris99")) as driver:
        session = driver.session(database="datamart")

        while petl.nrows(input_table) > 0:
            absolute_path = os.path.dirname(__file__)
            relative_path = "csv"
            full_path = os.path.join(absolute_path, relative_path)

            petl.tocsv(input_table, full_path + "/br_peserta_kegiatan.csv")
            file_url = "file:///" + str(full_path).replace("\\",
                                                        "/") + "/br_peserta_kegiatan.csv"

            query = f"""
                    LOAD CSV WITH HEADERS FROM '{file_url}' AS row
                    MATCH (peserta:DimPeserta {{id_peserta : row.id_peserta}})
                    MATCH (pelatihan:FactKegiatan {{id_kegiatan: row.id_kegiatan}})
                    CREATE (peserta)-[r:MENGIKUTI]->(pelatihan)
                    """
            session.run(query)

            print(start_index, end_index)

            start_index = end_index
            end_index += 10_000
            input_table = petl.rowslice(
                br_peserta_kegiatan, start_index, end_index)

        session.close()

    # for value in petl.dicts(br_peserta_kegiatan):
    #     id_peserta = value["id_peserta"]
    #     id_kegiatan = value["id_kegiatan"]

    #     print(id_peserta, id_kegiatan)

    #     graph.run(
    #         f"MATCH (peserta:DimPeserta), (kegiatan:FactKegiatan) WHERE peserta.id_peserta = {id_peserta} AND kegiatan.id_kegiatan = {id_kegiatan} CREATE (peserta)-[r:MENGIKUTI]->(kegiatan) return kegiatan, peserta")
