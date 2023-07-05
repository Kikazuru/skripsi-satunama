import petl

def dim_isu(data_mart, operasional):
    print("===DIM ISU===")

    isu = petl.fromdb(operasional, "SELECT * FROM isu")

    cursor = data_mart.cursor()
    petl.todb(isu, cursor, "dim_isu")