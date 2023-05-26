import petl
import datetime
import locale
locale.setlocale(locale.LC_TIME, "id")

from dotenv import load_dotenv
load_dotenv()

def dim_waktu(data_mart):
    print("===DIM WAKTU===")
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

    cursor = data_mart.cursor()
    cursor.execute("TRUNCATE dim_waktu RESTART IDENTITY CASCADE")
    petl.todb(dim_waktu, cursor, "dim_waktu")