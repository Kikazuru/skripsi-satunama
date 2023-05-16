import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

kegiatan = petl.fromdb(operasional, "SELECT * FROM kegiatan")

fact_proyek = petl.fromdb(data_mart, "SELECT * FROM fact_proyek")
dim_waktu = petl.fromdb(data_mart, "SELECT * FROM dim_waktu")
dim_jenis_kegiatan = petl.fromdb(data_mart, "SELECT * FROM dim_jenis_kegiatan")
dim_penerima_manfaat = petl.fromdb(data_mart, "SELECT * FROM dim_penerima_manfaat")
dim_lembaga_pelaksana = petl.fromdb(data_mart, "SELECT * FROM dim_lembaga_pelaksana")
dim_kabupaten_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")
dim_kecamatan = petl.fromdb(data_mart, "SELECT * FROM dim_kecamatan")
dim_desa_kelurahan = petl.fromdb(data_mart, "SELECT * FROM dim_desa_kelurahan")

lkp_fact_proyek = petl.dictlookupone(fact_proyek, "id_proyek")
lkp_dim_waktu = petl.dictlookupone(dim_waktu, "tanggal")
lkp_dim_jenis_kegiatan = petl.dictlookupone(dim_jenis_kegiatan, "id_jenis_kegiatan")
lkp_dim_penerima_manfaat = petl.dictlookupone(dim_penerima_manfaat, "id_penerima_manfaat")
lkp_dim_lembaga_pelaksana = petl.dictlookupone(dim_lembaga_pelaksana, "id_lembaga_pelaksana")
lkp_dim_kabupaten_kota = petl.dictlookupone(dim_kabupaten_kota, "id_kab_kota")
lkp_dim_kecamatan = petl.dictlookupone(dim_kecamatan, "id_kecamatan")
lkp_dim_desa_kelurahan = petl.dictlookupone(dim_desa_kelurahan, "id_desa_kel")

fact_kegiatan = petl.convert(kegiatan,
                             {
                                 "id_proyek": lambda id_proyek: lkp_fact_proyek[id_proyek]["proyek_key"],
                                 "tanggal_rencana": lambda tanggal: lkp_dim_waktu[tanggal]["waktu_key"],
                                 "tanggal_pelaksanaan": lambda tanggal: lkp_dim_waktu[tanggal]["waktu_key"],
                                 "id_jenis_kegiatan": lambda id_jenis_kegiatan: lkp_dim_jenis_kegiatan[id_jenis_kegiatan]["jenis_kegiatan_key"],
                                 "id_penerima_manfaat": lambda id_penerima_manfaat: lkp_dim_penerima_manfaat[id_penerima_manfaat]["penerima_manfaat_key"],
                                 "id_lembaga_pelaksana": lambda id_lembaga_pelaksana: lkp_dim_lembaga_pelaksana[id_lembaga_pelaksana]["lembaga_key"],
                                 "id_kota": lambda id_kota: lkp_dim_kabupaten_kota[id_kota]["kab_kota_key"],
                                 "id_kecamatan": lambda id_kecamatan: lkp_dim_kecamatan[id_kecamatan]["kecamatan_key"],
                                 "id_desa": lambda id_desa: lkp_dim_desa_kelurahan[id_desa]["desa_kel_key"],
                             })

fact_kegiatan = petl.rename(fact_kegiatan,
                            {
                                "id_proyek": "proyek_key",
                                "tanggal_rencana": "tanggal_rencana_key",
                                "tanggal_pelaksanaan": "tanggal_pelaksanaan_key",
                                "id_jenis_kegiatan": "jenis_kegiatan_key",
                                "id_penerima_manfaat": "penerima_manfaat_key",
                                "id_lembaga_pelaksana": "lembaga_pelaksana_key",
                                "id_kota": "kota_key",
                                "id_kecamatan": "kecamatan_key",
                                "id_desa": "desa_kel_key",
                            })

cursor = data_mart.cursor()
cursor.execute("TRUNCATE fact_kegiatan RESTART IDENTITY CASCADE")
petl.todb(fact_kegiatan, cursor, "fact_kegiatan")