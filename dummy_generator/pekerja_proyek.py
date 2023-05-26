import petl
import random

def pekerja_proyek(connection, jumlah_pekerja, seed=42):
    print("===DUMMY PEKERJA PROYEK===")

    proyek = petl.fromdb(connection, "SELECT * FROM proyek")
    pekerja = petl.fromdb(connection, "SELECT * FROM pekerja")
    jabatan = petl.fromdb(connection, "SELECT * FROM jabatan_proyek")

    if petl.nrows(proyek) == 0 or petl.nrows(pekerja) == 0:
        print("Masukan proyek dan pekerja terlebih dahulu")
    else:
        list_id_proyek = list(proyek["id_proyek"])
        list_id_pekerja = list(pekerja["id_pekerja"])
        list_id_jabatan = list(jabatan["id_jabatan"])

        tabel_pekerja_proyek = []
        for id_proyek in list_id_proyek:
            sample_pekerja = random.sample(list_id_pekerja, jumlah_pekerja)
            for id_pekerja in sample_pekerja:
                tabel_pekerja_proyek.append(
                    {
                        "id_proyek": id_proyek,
                        "id_pekerja": id_pekerja,
                        "id_jabatan_proyek": random.choice(list_id_jabatan)
                    }
                )

        dummy_pekerja = petl.fromdicts(tabel_pekerja_proyek)

        cursor = connection.cursor()
        cursor.execute("TRUNCATE pekerja_proyek RESTART IDENTITY CASCADE")

        petl.todb(dummy_pekerja, cursor, "pekerja_proyek")
