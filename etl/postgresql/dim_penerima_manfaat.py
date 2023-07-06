import petl
from load_dim import load_dim


def dim_penerima_manfaat(data_mart, operasional):
    print("===DIM PENERIMAAN MANFAAT===")
    penerima_manfaat = petl.fromdb(
        operasional, "SELECT * FROM penerima_manfaat")
    dim_penerima_manfaat = petl.fromdb(
        data_mart, "SELECT * FROM dim_penerima_manfaat")

    cursor = data_mart.cursor()
    # petl.todb(penerima_manfaat, cursor, "dim_penerima_manfaat")
    load_dim("dim_penerima_manfaat", dim_penerima_manfaat, penerima_manfaat,
             "penerima_manfaat_key", "id_penerima_manfaat", cursor)
