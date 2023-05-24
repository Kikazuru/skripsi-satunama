from donor import donor
from isu import isu
from pekerja import pekerja
from pekerja_proyek import pekerja_proyek
from proyek import proyek
from kegiatan import kegiatan
from peserta import peserta
from peserta_kegiatan import peserta_kegiatan

n_isu = 20

n_proyek = 10000
# n_goal = 2 * n_proyek
# n_outcome = 4 * n_goal
# n_output = 6 * n_outcome
n_kegiatan = 15 * n_proyek

n_peseta = 5 * n_kegiatan

n_pekerja = 100

isu(n_isu)

pekerja(n_pekerja)
proyek(n_proyek)
pekerja_proyek(jumlah_pekerja=10)

kegiatan(n_kegiatan)
peserta(n_peseta)
peserta_kegiatan(jumlah_peserta=10)

print("selesai")
