import petl
import os

def br_pekerja_proyek(operasional, graph):
    print("===LOADING RELASI PEKERJA PROYEK===")
    br_pekerja_proyek = petl.fromdb(
        operasional, "SELECT * FROM pekerja_proyek")

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
