import petl
import os

def br_peserta_kegiatan(operasional, graph):
    print("===LOADING RELASI PESERTA KEGIATAN===")
    
    start_index = 0
    end_index = 100_000

    br_peserta_kegiatan = petl.fromdb(
        operasional, "SELECT * FROM peserta_kegiatan_proyek")
    input_table = petl.rowslice(br_peserta_kegiatan, start_index, end_index)

    absolute_path = os.path.dirname(__file__)
    relative_path = "csv"
    full_path = os.path.join(absolute_path, relative_path)

    while petl.nrows(input_table) > 0:
        print(end_index)
        petl.tocsv(input_table, full_path + "/br_peserta_kegiatan.csv")
        file_url = "file:///" + \
            str(full_path).replace("\\", "/") + "/br_peserta_kegiatan.csv"

        query = f"""
                CALL apoc.periodic.iterate(
                "LOAD CSV WITH HEADERS FROM '{file_url}' AS row return row",
                "MATCH (peserta:Peserta {{id_peserta : toInteger(row.id_peserta)}})
                MATCH (kegiatan:Kegiatan {{id_kegiatan: toInteger(row.id_kegiatan)}})
                CREATE (peserta)-[r:MENGIKUTI]->(kegiatan)",
                {{batchSize : 10000, parallel: true}}
                )
                """

        graph.run(query)

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(
            br_peserta_kegiatan, start_index, end_index)