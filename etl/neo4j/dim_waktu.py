import petl
import datetime
import locale
from py2neo import Graph
from py2neo.bulk import create_nodes
locale.setlocale(locale.LC_TIME, "id")

from dotenv import load_dotenv
load_dotenv()

n = 50_000
start_date = datetime.date(1900, 1, 1)

dim_waktu = []

for i in range(n):
    tanggal = start_date + datetime.timedelta(i)
    timetuple = tanggal.timetuple()
    dim_waktu.append({
        "tanggal": tanggal,
        "hari_per_minggu": timetuple.tm_wday,
        "nama_hari_per_minggu": tanggal.strftime("%A"),
        "hari_per_bulan":timetuple.tm_mon,
        "hari_per_tahun": timetuple.tm_yday,
        "minggu_per_tahun": tanggal.isocalendar().week,
        "bulan": tanggal.month,
        "nama_bulan": tanggal.strftime("%B"),
        "kuartal": (tanggal.month + 1) // 3,
        "tahun": tanggal.year
    })

dim_waktu = petl.fromdicts(dim_waktu)

start_index = 0
end_index = 100_000

graph = Graph("neo4j://localhost:7687/",
              auth=("neo4j", "@Harris99"), name="datamart")

input_table = petl.rowslice(dim_waktu, start_index, end_index)

while petl.nrows(input_table) > 0:
    input_table = petl.dicts(input_table)

    create_nodes(graph.auto(), input_table, ("DimWaktu",
                                             "hari_per_minggu", "tanggal", "nama_hari_per_minggu", "hari_per_bulan", "hari_per_tahun", "minggu_per_tahun", "bulan", "nama_bulan", "kuartal", "tahun"))
    print(graph.nodes.match("DimWaktu").count())

    start_index = end_index
    end_index += 100_000
    input_table = petl.rowslice(dim_waktu, start_index, end_index)