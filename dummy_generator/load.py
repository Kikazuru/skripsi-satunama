import petl
import psycopg2
import random
import peserta
import peserta_kegiatan
import pekerja
import pekerja_proyek

db = psycopg2.connect(
    "host=localhost dbname=op_manpro_satunama user=postgres password=@Harris99")
cursor = db.cursor()

kegiatan_proyek = petl.fromxlsx("kegiatan_proyek.xlsx", "proyek")
kota = petl.fromdb(db, "SELECT * FROM kabupaten_kota")
id_kota = list(kota["id_kab_kota"])

kegiatan_proyek = petl.addfield(kegiatan_proyek, field="id_kota",
                                value=random.choice(id_kota))
kegiatan_proyek = petl.cutout(kegiatan_proyek, "No")
kegiatan_proyek = petl.filldown(kegiatan_proyek)


penerima_manfaat = [("penerima_manfaat", ), ] + [(row, )
                                                 for row in list(set(kegiatan_proyek["penerima_manfaat"]))]
cursor.execute("TRUNCATE penerima_manfaat RESTART IDENTITY CASCADE")
petl.todb(penerima_manfaat, cursor, "penerima_manfaat")
penerima_manfaat = petl.fromdb(db, "SELECT * FROM penerima_manfaat")
lkp_penerima_manfaat = petl.dictlookupone(
    penerima_manfaat, key="penerima_manfaat")

jenis_kegiatan = [("jenis_kegiatan", ), ] + [(row, )
                                             for row in list(set(kegiatan_proyek["jenis_kegiatan"]))]
cursor.execute("TRUNCATE jenis_kegiatan RESTART IDENTITY CASCADE")
petl.todb(jenis_kegiatan, cursor, "jenis_kegiatan")
jenis_kegiatan = petl.fromdb(db, "SELECT * FROM jenis_kegiatan")
lkp_jenis_kegiatan = petl.dictlookupone(jenis_kegiatan, key="jenis_kegiatan")


kegiatan_proyek = petl.convert(kegiatan_proyek,
                               {
                                   "penerima_manfaat": lambda penerima_manfaat: lkp_penerima_manfaat[penerima_manfaat]["id_penerima_manfaat"],
                                   "jenis_kegiatan": lambda jenis_kegiatan: lkp_jenis_kegiatan[jenis_kegiatan]["id_jenis_kegiatan"]
                               })

kegiatan_proyek = petl.rename(kegiatan_proyek,
                              {
                                  "penerima_manfaat": "id_penerima_manfaat",
                                  "jenis_kegiatan": "id_jenis_kegiatan",
                                  "lembaga_pelaksana": "id_lembaga_pelaksana",
                                  "proyek": "id_proyek"
                              })

cursor.execute("TRUNCATE kegiatan RESTART IDENTITY CASCADE")
petl.todb(kegiatan_proyek, cursor, "kegiatan")

peserta.peserta(db, 523)
peserta_kegiatan.peserta_kegiatan(db, 10)
pekerja.pekerja(db, 34)
pekerja_proyek.pekerja_proyek(db, 7)

