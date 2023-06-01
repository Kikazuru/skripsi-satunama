from py2neo.bulk import create_nodes
import petl


def dim_kab_kota(operasional, graph):
    print("==LOADING KAB KOTA==")

    start_index = 0
    end_index = 100_000

    table_kab_kota = petl.fromdb(operasional, "SELECT * FROM kabupaten_kota")
    input_table = petl.rowslice(table_kab_kota, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=[
                     "Dimensi", "Kota", "Kabupaten"])
        print(graph.nodes.match("Dimensi", "Kota", "Kabupaten").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_kab_kota, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX kabKotaIndex IF NOT EXISTS FOR (kota:Kota) on (kota.id_kab_kota, kota.nama_kab_kota)")

    graph.run(
        "CREATE RANGE INDEX kabKotaIndex IF NOT EXISTS FOR (kab:Kabupaten) on (kab.id_kab_kota, kab.nama_kab_kota)")

    graph.run(
        "MATCH (provinsi:Provinsi), (kota:Kabupaten|Kota) WHERE provinsi.id_provinsi = kota.id_provinsi CREATE (kota)-[r:KOTA_DARI]->(provinsi) return provinsi, kota")
