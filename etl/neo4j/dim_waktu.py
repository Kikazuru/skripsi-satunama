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
            "hari_per_minggu": timetuple.tm_wday,
            "nama_hari_per_minggu": tanggal.strftime("%A"),
            "hari_per_bulan": timetuple.tm_mon,
            "hari_per_tahun": timetuple.tm_yday,
            "minggu_per_tahun": tanggal.isocalendar().week,
            "bulan": tanggal.month,
            "nama_bulan": tanggal.strftime("%B"),
            "kuartal": (tanggal.month + 1) // 3,
            "tahun": tanggal.year
        })
        
    create_nodes(graph.auto(), dim_waktu, labels=["DimWaktu"])
    print(graph.nodes.match("DimWaktu").count())
