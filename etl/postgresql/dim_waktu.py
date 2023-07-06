from load_dim import load_dim
from dotenv import load_dotenv
import petl
import datetime
import locale
locale.setlocale(locale.LC_TIME, "id")

load_dotenv()


def dim_waktu(data_mart):
    print("===DIM WAKTU===")
    n = 50_000
    start_date = datetime.date(1900, 1, 1)

    waktu = []

    for i in range(n):
        tanggal = start_date + datetime.timedelta(i)
        timetuple = tanggal.timetuple()
        waktu.append({
            "tanggal": tanggal,
            "hari_per_minggu": str(timetuple.tm_wday + 1),
            "nama_hari_per_minggu": tanggal.strftime("%A"),
            "hari_per_bulan": str(timetuple.tm_mon),
            "hari_per_tahun": str(timetuple.tm_yday),
            "minggu_per_tahun": str(tanggal.isocalendar().week),
            "bulan": str(tanggal.month),
            "nama_bulan": tanggal.strftime("%B"),
            "kuartal": str((tanggal.month + 1) // 3 + 1),
            "tahun": tanggal.year
        })

    waktu = petl.fromdicts(waktu)
    dim_waktu = petl.fromdb(data_mart, "SELECT * FROM dim_waktu")

    cursor = data_mart.cursor()
    # petl.todb(waktu, cursor, "dim_waktu")
    load_dim("dim_waktu", dim_waktu, waktu, "waktu_key", "tanggal", cursor)
