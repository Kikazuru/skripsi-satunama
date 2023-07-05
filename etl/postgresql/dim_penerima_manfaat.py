import petl


def dim_penerima_manfaat(data_mart, operasional):
    print("===DIM PENERIMAAN MANFAAT===")
    penerima_manfaat = petl.fromdb(
        operasional, "SELECT * FROM penerima_manfaat")

    cursor = data_mart.cursor()
    petl.todb(penerima_manfaat, cursor, "dim_penerima_manfaat")
