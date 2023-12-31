import petl
import random
import datetime
from functools import partial
from utils import random_dates

def proyek(connection, n, seed=42):
    print("===DUMMY PROYEK===")

    donor = petl.fromdb(connection, "SELECT * FROM donor")
    provinsi = petl.fromdb(connection, "SELECT * FROM provinsi")
    kabupaten_kota = petl.fromdb(connection, "SELECT * FROM kabupaten_kota")
    isu = petl.fromdb(connection, "SELECT * FROM isu")

    if petl.nrows(donor) == 0 or petl.nrows(provinsi) == 0 or petl.nrows(kabupaten_kota) == 0:
        print("Masukan data donor, provinsi, dan kota terlebih dahulu")
    else:

        id_donor = list(donor["id_donor"])
        id_kota = list(kabupaten_kota["id_kab_kota"])
        id_isu = list(isu["id_isu"])

        fields = [
            ("tanggal_mulai_proyek",
             partial(random_dates,
                     datetime.date(2017, 1, 1),
                     datetime.date(2020, 1, 1))),
            ("tanggal_selesai_proyek",
             partial(random_dates,
                     datetime.date(2020, 1, 1),
                     datetime.date(2025, 1, 1))),
            ("dana_anggaran", partial(random.randrange,
                                      100_000_000, 7_000_000_000, 30_000_000)),
            ("id_donor", partial(random.choice, id_donor)),
            ("id_kab_kota", partial(random.choice, id_kota)),
            ("id_isu", partial(random.choice, id_isu))
        ]

        dummy_proyek = petl.dummytable(n, fields=fields, seed=seed)
        dummy_proyek = petl.addcolumn(dummy_proyek,
                                      field="nama_proyek",
                                      col=[f"proyek{i}" for i in range(n)],
                                      index=0)
        dummy_proyek = petl.addfield(dummy_proyek,
                                     field="satuan_anggaran",
                                     value="IDR",
                                     index=4)
        dummy_proyek = petl.addfield(dummy_proyek,
                                     field="id_negara",
                                     value=78)
        dummy_proyek = petl.join(
            dummy_proyek, kabupaten_kota, key="id_kab_kota")
        dummy_proyek = petl.cutout(dummy_proyek, "nama_kab_kota", "kode_bps")
        dummy_proyek = petl.rename(dummy_proyek, "id_kab_kota", "id_kota")
        dummy_proyek = petl.sort(dummy_proyek, key="nama_proyek")

        cursor = connection.cursor()
        cursor.execute("TRUNCATE proyek RESTART IDENTITY CASCADE")
        petl.todb(dummy_proyek, cursor, "proyek")
