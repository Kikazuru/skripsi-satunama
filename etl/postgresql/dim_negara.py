import petl

def dim_negara(data_mart, operasional):
    print("===DIM NEGARA===")

    negara = petl.fromdb(operasional, "SELECT * FROM negara")

    cursor = data_mart.cursor()
    petl.todb(negara, cursor, "dim_negara")