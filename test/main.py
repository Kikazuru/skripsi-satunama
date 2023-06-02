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
        sql_folder = os.path.join(folder_path, "sql")
        neo4j_folder = os.path.join(folder_path, "neo4j")

        is_exist = os.path.exists(folder_path)
        if not is_exist:
            os.makedirs(folder_path)
            os.makedirs(sql_folder)
            os.makedirs(neo4j_folder)

        summary_sql = []
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
                sql_folder, f"sql_{n_proyek}.csv")

            with open(file_path_sql, "w") as file_result_sql:
                writer = csv.DictWriter(
                    file_result_sql, fieldnames=["run", "execution_time"])
                writer.writeheader()
                writer.writerows([{"run": i, "execution_time": result}
                                 for i, result in enumerate(sql_result, 1)])

            summary_sql.append({
                "num_proyek": n_proyek,
                "best_time": min(sql_result),
                "average_time": round(sum(sql_result) / len(sql_result), 4),
                "worst_time": max(sql_result)
            })

        # SQL SUMMARY
        sql_summary_path = os.path.join(sql_folder, "sql_summary.csv")
        with open(sql_summary_path, "w") as file_summary_sql:
            writer = csv.DictWriter(
                file_summary_sql, fieldnames=["num_proyek", "best_time", "average_time", "worst_time"])
            writer.writeheader()
            writer.writerows(summary_sql)
