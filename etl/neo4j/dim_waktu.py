from dotenv import load_dotenv
import petl
import datetime
import locale
from py2neo import Graph
from py2neo.bulk import create_nodes
locale.setlocale(locale.LC_TIME, "id")

load_dotenv()


def dim_waktu(graph):
    print("==LOADING WAKTU==")

    n = 50_000
    start_date = datetime.date(1900, 1, 1)

    dim_waktu = []

    for i in range(n):
        tanggal = start_date + datetime.timedelta(i)
        timetuple = tanggal.timetuple()
        dim_waktu.append({
            "tanggal": tanggal,
            "hari_per_minggu": str(timetuple.tm_wday + 1),
            "nama_hari_per_minggu": tanggal.strftime("%A"),
            "hari_per_bulan": str(timetuple.tm_mon),
            "hari_per_tahun": str(timetuple.tm_yday),
            "minggu_per_tahun": str(tanggal.isocalendar().week),
            "bulan": str(tanggal.month),
            "nama_bulan": tanggal.strftime("%B"),
            "kuartal": str((tanggal.month + 1) // 3 + 1),
            "tahun": str(tanggal.year)
        })

    create_nodes(graph.auto(), dim_waktu, labels=["Dimensi", "Waktu"])
    print(graph.nodes.match("Dimensi", "Waktu").count())

    graph.run(
        "CREATE RANGE INDEX waktuIndex IF NOT EXISTS FOR (waktu:Waktu) on (waktu.tanggal)")
