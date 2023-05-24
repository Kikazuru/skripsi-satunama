import petl
import psycopg2
from py2neo import Graph
from py2neo.bulk import create_nodes
import os

from dotenv import load_dotenv
load_dotenv()

start_index = 0
end_index = 100_000

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

table_proyek = petl.fromdb(operasional, "SELECT * FROM proyek")
br_pekerja_proyek = petl.fromdb(operasional, "SELECT * FROM pekerja_proyek")
input_table = petl.rowslice(table_proyek, start_index, end_index)

graph = Graph("neo4j://localhost:7687/",
              auth=("neo4j", "@Harris99"), name="datamart")

while petl.nrows(input_table) > 0:
    input_table = petl.dicts(input_table)

    create_nodes(graph.auto(), input_table, labels=["FactProyek"])
    print(graph.nodes.match("FactProyek").count())

    start_index = end_index
    end_index += 100_000
    input_table = petl.rowslice(table_proyek, start_index, end_index)

graph.run(
    "MATCH (proyek:FactProyek), (negara:DimNegara) WHERE proyek.id_negara = negara.id_negara CREATE (proyek)-[r:BERADA]->(negara) return proyek, negara")

graph.run(
    "MATCH (proyek:FactProyek), (provinsi:DimProvinsi) WHERE proyek.id_provinsi = provinsi.id_provinsi CREATE (proyek)-[r:BERADA]->(provinsi) return proyek, provinsi")

graph.run(
    "MATCH (proyek:FactProyek), (kota:DimKabKota) WHERE proyek.id_kota = kota.id_kab_kota CREATE (proyek)-[r:BERADA]->(kota) return proyek, kota")

graph.run(
    "MATCH (proyek:FactProyek), (isu:DimIsu) WHERE proyek.id_isu = isu.id_isu CREATE (proyek)-[r:ISU]->(isu) return proyek, isu")

graph.run(
    "MATCH (proyek:FactProyek), (donor:DimDonor) WHERE proyek.id_negara = donor.id_donor CREATE (proyek)-[r:DONOR]->(donor) return proyek, donor")

for value in petl.dicts(br_pekerja_proyek):
    id_proyek = value["id_proyek"]
    id_pekerja = value["id_pekerja"]

    graph.run(
        f"MATCH (proyek:FactProyek), (pekerja:DimPekerja) WHERE proyek.id_proyek = {id_proyek} AND pekerja.id_pekerja = {id_pekerja} CREATE (pekerja)-[r:BEKERJA]->(proyek) return pekerja, proyek")
