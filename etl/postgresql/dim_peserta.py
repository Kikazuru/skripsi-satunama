import petl

def dim_peserta(data_mart, operasional):
    print("===DIM PESERTA===")
    peserta = petl.fromdb(operasional, "SELECT * FROM peserta")

    cursor = data_mart.cursor()
    petl.todb(peserta, cursor, "dim_peserta")
