from dim_desa import dim_desa
from dim_isu import dim_isu
from dim_jenis_kegiatan import dim_jenis_kegiatan
from dim_kab_kota import dim_kab_kota
from dim_provinsi import dim_provinsi
from dim_kecamatan import dim_kecamatan
from dim_lembaga_pelaksana import dim_lembaga_pelaksana
from dim_negara import dim_negara
from dim_pekerja import dim_pekerja
from dim_penerima_manfaat import dim_penerima_manfaat
from dim_peserta import dim_peserta
from dim_waktu import dim_waktu
from dim_donor import dim_donor
from fact_kegiatan import fact_kegiatan
from fact_proyek import fact_proyek
from br_pekerja_proyek import br_pekerja_proyek
from br_peserta_kegiatan import br_peserta_kegiatan


def job(operasional, graph):
    dim_isu(operasional, graph)
    dim_pekerja(operasional, graph)
    dim_waktu(graph)
    dim_jenis_kegiatan(operasional, graph)
    dim_penerima_manfaat(operasional, graph)
    dim_peserta(operasional, graph)

    dim_negara(operasional, graph)
    dim_provinsi(operasional, graph)
    dim_kab_kota(operasional, graph)
    dim_kecamatan(operasional, graph)
    dim_desa(operasional, graph)

    dim_lembaga_pelaksana(operasional, graph)
    dim_donor(operasional, graph)

    fact_proyek(operasional, graph)
    fact_kegiatan(operasional, graph)

    br_pekerja_proyek(operasional, graph)
    br_peserta_kegiatan(operasional, graph)
