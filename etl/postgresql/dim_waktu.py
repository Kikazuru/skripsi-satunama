from load_dim import load_dim
from dotenv import load_dotenv
import petl
import datetime
import locale
locale.setlocale(locale.LC_TIME, "id")

load_dotenv()


def dim_waktu(data_mart):
    print("===DIM WAKTU===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_waktu
    (
        waktu_key serial NOT NULL,
        tanggal date NOT NULL,
        hari_per_minggu integer NOT NULL,
        hari_per_bulan integer NOT NULL,
        bulan integer NOT NULL,
        kuartal integer NOT NULL,
        tahun integer NOT NULL,
        hari_per_tahun integer NOT NULL,
        nama_hari_per_minggu character varying NOT NULL,
        nama_bulan character varying NOT NULL,
        minggu_per_tahun integer NOT NULL,
        CONSTRAINT dim_waktu_pkey PRIMARY KEY (waktu_key)
    )
    """)

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

    load_dim("dim_waktu", dim_waktu, waktu, "waktu_key", "tanggal", cursor)
