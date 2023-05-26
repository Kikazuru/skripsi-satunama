import petl


def dim_pekerja(data_mart, operasional):
    print("===DIM PEKERJA===")

    pekerja = petl.fromdb(operasional, "SELECT * FROM pekerja")

    cursor = data_mart.cursor()
    cursor.execute("TRUNCATE dim_pekerja RESTART IDENTITY CASCADE")
    petl.todb(pekerja, cursor, "dim_pekerja")
