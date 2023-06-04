import petl
from py2neo.bulk import create_nodes
import os

from dotenv import load_dotenv
load_dotenv()


def fact_proyek(operasional, graph):
    print("==LOADING FACT PROYEK==")

    start_index = 0
    end_index = 100_000

    table_proyek = petl.fromdb(operasional, "SELECT * FROM proyek")
    br_pekerja_proyek = petl.fromdb(
        operasional, "SELECT * FROM pekerja_proyek")
    kegiatan = petl.fromdb(operasional, "SELECT * FROM kegiatan")

    lkp_kegiatan = petl.dictlookup(kegiatan, "id_proyek")

    table_proyek = petl.addfield(table_proyek,
                                 field="jumlah_kegiatan",
                                 value=lambda row: len(lkp_kegiatan[row["id_proyek"]]))

    table_proyek = petl.addfield(table_proyek,
                                 field="pengeluaran_proyek",
                                 value=lambda row: sum(map(lambda kegiatan: kegiatan["pengeluaran"], lkp_kegiatan[row["id_proyek"]])))

    input_table = petl.rowslice(table_proyek, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["Fact", "Proyek"])
        print(graph.nodes.match("Fact", "Proyek").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_proyek, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX proyekIndex IF NOT EXISTS FOR (proyek:Proyek) on (proyek.id_proyek)")

    graph.run(
        "MATCH (proyek:Proyek), (negara:Negara) WHERE proyek.id_negara = negara.id_negara CREATE (proyek)-[r:BERADA]->(negara)")

    graph.run(
        "MATCH (proyek:Proyek), (provinsi:Provinsi) WHERE proyek.id_provinsi = provinsi.id_provinsi CREATE (proyek)-[r:BERADA]->(provinsi)")

    graph.run(
        "MATCH (proyek:Proyek), (kota:Kota|Kabupaten) WHERE proyek.id_kota = kota.id_kab_kota CREATE (proyek)-[r:BERADA]->(kota)")

    graph.run(
        "MATCH (proyek:Proyek), (isu:Isu) WHERE proyek.id_isu = isu.id_isu CREATE (proyek)-[r:MENGANGKAT]->(isu)")

    graph.run(
        "MATCH (proyek:Proyek), (donor:Donor) WHERE proyek.id_donor = donor.id_donor CREATE (proyek)-[r:DIDANAI_OLEH]->(donor)")

    graph.run(
        "MATCH (proyek:Proyek), (waktu:Waktu) WHERE proyek.tanggal_mulai_proyek = waktu.tanggal CREATE (proyek)-[r:MULAI_PADA]->(waktu)")

    graph.run(
        "MATCH (proyek:Proyek), (waktu:Waktu) WHERE proyek.tanggal_selesai_proyek = waktu.tanggal CREATE (proyek)-[r:SELESAI_PADA]->(waktu)")

    absolute_path = os.path.dirname(__file__)
    relative_path = "csv"
    full_path = os.path.join(absolute_path, relative_path)

    petl.tocsv(br_pekerja_proyek, full_path + "/br_pekerja_proyek.csv")
    file_url = "file:///" + str(full_path).replace("\\",
                                                   "/") + "/br_pekerja_proyek.csv"

    query = f"""
            CALL apoc.periodic.iterate(
            "LOAD CSV WITH HEADERS FROM '{file_url}' AS row return row",
            "MATCH (pekerja:Pekerja {{id_pekerja : toInteger(row.id_pekerja)}})
            MATCH (proyek:Proyek {{id_proyek: toInteger(row.id_proyek)}})
            CREATE (pekerja)-[r:BEKERJA_DI]->(proyek)",
            {{batchSize : 10000, parallel: true}}
            )
            """

    graph.run(query)
