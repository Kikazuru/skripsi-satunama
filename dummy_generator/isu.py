import petl


def isu(connection, n):
    print("===DUMMY ISU===")
    dummy_isu = [{"nama_isu": f"isu{i}"} for i in range(n)]
    dummy_isu = petl.fromdicts(dummy_isu)

    cursor = connection.cursor()
    cursor.execute("TRUNCATE isu RESTART IDENTITY CASCADE")
    petl.todb(dummy_isu, cursor, "isu")
