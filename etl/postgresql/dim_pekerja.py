import petl
from load_dim import load_dim


def dim_pekerja(data_mart, operasional):
    print("===DIM PEKERJA===")

    pekerja = petl.fromdb(operasional, "SELECT * FROM pekerja")
    dim_pekerja = petl.fromdb(data_mart, "SELECT * FROM dim_pekerja")

    cursor = data_mart.cursor()

    load_dim("dim_pekerja", dim_pekerja, pekerja,
             "pekerja_key", "id_pekerja", cursor)
