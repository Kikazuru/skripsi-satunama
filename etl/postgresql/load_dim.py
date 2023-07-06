import petl


def load_dim(dm_table_name, dm_table, op_table, key_dm, key_op, cursor):
    lkp_dim = petl.dictlookupone(dm_table, key_op)
    dim_no_key = petl.cutout(dm_table, key_dm)

    new_data_id = petl.recordcomplement(
        petl.cut(op_table, key_op), petl.cut(dim_no_key, key_op))
    list_new_data_id = list(new_data_id[key_op])
    new_data = petl.select(
        op_table, lambda row: row[key_op] in list_new_data_id)

    petl.appenddb(new_data, cursor, dm_table_name)

    new_value = petl.recordcomplement(op_table, dim_no_key)
    new_value = petl.select(
        new_value, lambda row: row[key_op] not in list_new_data_id)
    new_value = petl.addfield(
        new_value, field=key_dm, value=lambda row: lkp_dim[row[key_op]][key_dm])
    headers = petl.header(new_value)

    field_set = ",\n".join([
        f"{header} = %s" for header in headers[:-1]
    ])

    query_update = f"""
        UPDATE {dm_table_name}
        SET
        {field_set}
        WHERE
        {headers[-1]} = %s
    """

    for value in petl.data(table=new_value):
        cursor.execute(query_update, value)
