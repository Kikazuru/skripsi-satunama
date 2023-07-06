import petl
from load_dim import load_dim


def dim_negara(data_mart, operasional):
    print("===DIM NEGARA===")

    negara = petl.fromdb(operasional, "SELECT * FROM negara")
    dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")
    # print(negara, dim_negara)
    cursor = data_mart.cursor()
    # petl.todb(negara, cursor, "dim_negara")
    load_dim("dim_negara", dim_negara, negara,
             "negara_key", "id_negara", cursor)
