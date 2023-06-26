from concurrent import futures
from dotenv import load_dotenv
import psycopg2
import os
import petl
from datetime import date
load_dotenv()

n_proyek = 10000


dbname_dm = f'{os.getenv("DBNAME_DM_PSQL")}_{n_proyek}_proyek'
data_mart = psycopg2.connect(
    f'host={os.getenv("DBHOST_DM_PSQL")} dbname={dbname_dm} user={os.getenv("DBUSER_DM_PSQL")} password={os.getenv("DBPASS_DM_PSQL")}')

dbname_op = f'{os.getenv("DBNAME_OP")}_{n_proyek}_proyek'
operasional = psycopg2.connect(
    f'host={os.getenv("DBHOST_OP")} dbname={dbname_op} user={os.getenv("DBUSER_OP")} password={os.getenv("DBPASS_OP")}')


proyek = petl.fromdb(operasional, "SELECT * FROM proyek")

dim_isu = petl.fromdb(data_mart, "SELECT * FROM dim_isu")
dim_donor = petl.fromdb(data_mart, "SELECT * FROM dim_donor")
dim_waktu = petl.fromdb(data_mart, "SELECT * FROM dim_waktu")
dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")
dim_provinsi = petl.fromdb(data_mart, "SELECT * FROM dim_provinsi")
dim_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")

lkp_dim_isu = petl.dictlookupone(dim_isu, "id_isu")
lkp_dim_waktu = petl.dictlookupone(dim_waktu, "tanggal")
lkp_dim_donor = petl.dictlookupone(dim_donor, "id_donor")
lkp_dim_negara = petl.dictlookupone(dim_negara, "id_negara")
lkp_dim_provinsi = petl.dictlookupone(dim_provinsi, "id_provinsi")
lkp_dim_kota = petl.dictlookupone(dim_kota, "id_kab_kota")

fact_proyek = petl.convert(proyek,
                           {
                               "id_donor": lambda id_donor: lkp_dim_donor[id_donor]["donor_key"],
                               "id_isu": lambda id_isu: lkp_dim_isu[id_isu]["isu_key"],
                               "id_negara": lambda id_negara: lkp_dim_negara[id_negara]["negara_key"],
                               "id_provinsi": lambda id_provinsi: lkp_dim_provinsi[id_provinsi]["provinsi_key"],
                               "id_kota": lambda id_kota: lkp_dim_kota[id_kota]["kab_kota_key"],
                               "tanggal_mulai_proyek": lambda tanggal_mulai_proyek: lkp_dim_waktu[tanggal_mulai_proyek]["waktu_key"],
                               "tanggal_selesai_proyek": lambda tanggal_selesai_proyek: lkp_dim_waktu[tanggal_selesai_proyek]["waktu_key"]
                           })

fact_proyek_data = petl.fromdb(
    data_mart, "select proyek_key, id_proyek, max(tanggal_load) as last_load from fact_proyek GROUP BY proyek_key")
lkp_fact_proyek = petl.dictlookupone(fact_proyek_data, "id_proyek")
load_date = date.today()

kegiatan = petl.fromdb(operasional, "SELECT * FROM kegiatan")
lkp_kegiatan = petl.dictlookup(kegiatan, "id_proyek")

fact_proyek = petl.addfield(fact_proyek,
                            field="jumlah_kegiatan",
                            value=lambda row: len(list(filter(lambda kegiatan:  kegiatan["tanggal_pelaksanaan"] > (lkp_fact_proyek.get(kegiatan["id_proyek"], {}).get("last_load") or date(1, 1, 1)),
                                                              lkp_kegiatan[row["id_proyek"]]))))
# kegiatan["tanggal_pelaksanaan"] >
print(fact_proyek)
