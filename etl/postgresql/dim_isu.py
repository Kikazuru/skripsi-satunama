import petl
from load_dim import load_dim


def dim_isu(data_mart, operasional):
    print("===DIM ISU===")

    isu = petl.fromdb(operasional, "SELECT * FROM isu")
    dim_isu = petl.fromdb(data_mart, "SELECT * FROM dim_isu")

    cursor = data_mart.cursor()

    load_dim("dim_isu", dim_isu, isu, "isu_key", "id_isu", cursor)
