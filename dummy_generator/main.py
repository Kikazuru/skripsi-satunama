from donor import donor
from isu import isu
from pekerja import pekerja
from pekerja_proyek import pekerja_proyek
from proyek import proyek
from kegiatan import kegiatan
from peserta import peserta
from peserta_kegiatan import peserta_kegiatan

n_donor = 30
n_isu = 20

n_proyek = 10000
# n_goal = 2 * n_proyek
# n_outcome = 4 * n_goal
# n_output = 6 * n_outcome
n_kegiatan = 15 * n_proyek

n_peseta = 5 * n_kegiatan
n_peserta_kegiatan = n_kegiatan * 30

n_pekerja = 100
n_pekerja_proyek = 10 * n_proyek

donor(n_donor)
isu(n_isu)

pekerja(n_pekerja)
proyek(n_proyek)
pekerja_proyek(n_pekerja_proyek)

kegiatan(n_kegiatan)
peserta(n_peseta)
peserta_kegiatan(n_peserta_kegiatan)

print("selesai")
