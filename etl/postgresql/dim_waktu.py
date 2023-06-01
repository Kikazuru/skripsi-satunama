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
            "tahun": tanggal.year
        })

    dim_waktu = petl.fromdicts(dim_waktu)

    cursor = data_mart.cursor()
    cursor.execute("TRUNCATE dim_waktu RESTART IDENTITY CASCADE")
    petl.todb(dim_waktu, cursor, "dim_waktu")
