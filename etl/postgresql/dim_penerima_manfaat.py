import petl


def dim_penerima_manfaat(data_mart, operasional):
    print("===DIM PENERIMAAN MANFAAT===")
    penerima_manfaat = petl.fromdb(
        operasional, "SELECT * FROM penerima_manfaat")

    cursor = data_mart.cursor()
    cursor.execute("TRUNCATE dim_penerima_manfaat RESTART IDENTITY CASCADE")
    petl.todb(penerima_manfaat, cursor, "dim_penerima_manfaat")
