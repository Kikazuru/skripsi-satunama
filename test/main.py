from dotenv import load_dotenv
import psycopg2
import os
import benchmark
import json
import csv

load_dotenv()

absolute_path = os.path.dirname(__file__)
result_folder = os.path.join(absolute_path, "result")
if not os.path.exists(result_folder):
    os.makedirs(result_folder)

query_file_path = os.path.join(absolute_path, "query.json")
with open(query_file_path, "r") as query_file:

    queries = json.load(query_file)
    for query in queries:
        folder_path = os.path.join(result_folder, query["nama"])
        is_exist = os.path.exists(folder_path)
        if not is_exist:
            os.makedirs(folder_path)
            os.makedirs(os.path.join(folder_path, "sql"))
            os.makedirs(os.path.join(folder_path, "neo4j"))

        for i in range(1, 6):
            n_proyek = 10 ** i
            print(f"PROYEK {n_proyek}".center(20).replace(" ", "="))

            # SQL
            dbname_dm = f'{os.getenv("DB_NAME_DM")}_{n_proyek}_proyek'

            data_mart = psycopg2.connect(
                f'dbname={dbname_dm} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

            sql_result = benchmark.sql(
                data_mart, query["psql_query"], 10)
            file_path_sql = os.path.join(
                folder_path, "sql", f"{dbname_dm}.csv")

            with open(file_path_sql, "w") as file_result_sql:
                writer = csv.DictWriter(
                    file_result_sql, fieldnames=sql_result[0].keys())
                writer.writeheader()
                writer.writerows(sql_result)
