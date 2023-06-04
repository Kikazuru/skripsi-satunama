from concurrent import futures
from dotenv import load_dotenv
import psycopg2
import os

from fact_proyek import fact_proyek
from fact_kegiatan import fact_kegiatan
from dim_waktu import dim_waktu
from dim_provinsi import dim_provinsi
from dim_peserta import dim_peserta
from dim_penerima_manfaat import dim_penerima_manfaat
from dim_pekerja import dim_pekerja
from dim_negara import dim_negara
from dim_lembaga_pelaksana import dim_lembaga_pelaksana
from dim_kecamatan import dim_kecamatan
from dim_kab_kota import dim_kab_kota
from dim_jenis_kegiatan import dim_jenis_kegiatan
from dim_isu import dim_isu
from dim_donor import dim_donor
from dim_desa_kelurahan import dim_desa_kelurahan
from br_peserta_kegiatan import br_peserta_kegiatan
from br_pekerja_proyek import br_pekerja_proyek


def dim_lokasi(data_mart, operasional):
    dim_negara(data_mart, operasional)
    dim_provinsi(data_mart, operasional)
    dim_kab_kota(data_mart, operasional)
    dim_kecamatan(data_mart, operasional)
    dim_desa_kelurahan(data_mart, operasional)


load_dotenv()

for i in range(1, 6):
    n_proyek = 10 ** i

    print(f"===ETL PROYEK {n_proyek}===")

    dbname_dm = f'{os.getenv("DBNAME_DM_PSQL")}_{n_proyek}_proyek'
    data_mart = psycopg2.connect(
        f'host={os.getenv("DBHOST_DM_PSQL")} dbname={dbname_dm} user={os.getenv("DBUSER_DM_PSQL")} password={os.getenv("DBPASS_DM_PSQL")}')

    dbname_op = f'{os.getenv("DBNAME_OP")}_{n_proyek}_proyek'
    operasional = psycopg2.connect(
        f'host={os.getenv("DBHOST_OP")} dbname={dbname_op} user={os.getenv("DBUSER_OP")} password={os.getenv("DBPASS_OP")}')

    with futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.submit(dim_isu, data_mart, operasional)
        executor.submit(dim_lokasi, data_mart, operasional)
        executor.submit(dim_pekerja, data_mart, operasional)
        executor.submit(dim_waktu, data_mart)
        executor.submit(dim_jenis_kegiatan, data_mart, operasional)
        executor.submit(dim_penerima_manfaat, data_mart, operasional)
        executor.submit(dim_peserta, data_mart, operasional)

    with futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(dim_lembaga_pelaksana, data_mart, operasional)
        executor.submit(dim_donor, data_mart, operasional)

    fact_proyek(data_mart, operasional)
    fact_kegiatan(data_mart, operasional)
    br_pekerja_proyek(data_mart, operasional)
    br_peserta_kegiatan(data_mart, operasional)

    print()

print("ETL SELESAI")
