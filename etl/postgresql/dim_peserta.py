import petl
from load_dim import load_dim


def dim_peserta(data_mart, operasional):
    print("===DIM PESERTA===")
    peserta = petl.fromdb(operasional, "SELECT * FROM peserta")
    dim_peserta = petl.fromdb(data_mart, "SELECT * FROM dim_peserta")

    cursor = data_mart.cursor()
    # petl.todb(peserta, cursor, "dim_peserta")
    load_dim("dim_peserta", dim_peserta, peserta,
             "peserta_key", "id_peserta", cursor)
