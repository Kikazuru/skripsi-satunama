from dotenv import load_dotenv
import psycopg2
from neo4j import GraphDatabase
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
        query_folder = os.path.join(result_folder, query["nama"])
        sql_folder = os.path.join(query_folder, "sql")
        neo4j_folder = os.path.join(query_folder, "neo4j")

        print("\n")
        print(f" TEST {query['nama']} ".center(49, "="))

        is_exist = os.path.exists(query_folder)
        if not is_exist:
            os.makedirs(query_folder)
            os.makedirs(sql_folder)
            os.makedirs(neo4j_folder)

        summary_sql = []
        summary_neo4j = []
        for i in range(1, 6):
            n_proyek = 10 ** i
            print()
            print(f"PROYEK {n_proyek}".center(49, "="))

            # SQL
            dbname_dm = f'{os.getenv("DB_NAME_DM")}_{n_proyek}_proyek'

            data_mart = psycopg2.connect(
                f'dbname={dbname_dm} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

            sql_result = benchmark.sql(
                data_mart, query["psql_query"], 10)
            file_path_sql = os.path.join(
                sql_folder, f"sql_{n_proyek}.csv")

            with open(file_path_sql, "w", newline="") as file_result_sql:
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

            # neo4j
            with GraphDatabase.driver("neo4j://localhost:7687/", auth=("neo4j", "@Harris99")) as driver:
                session = driver.session(database=f"datamart{n_proyek}")

                neo4j_result = benchmark.neo4j(
                    session, query=query["neo4j_query"], n=10)
                file_path_neo4j = os.path.join(
                    neo4j_folder, f"neo4j_{n_proyek}.csv")

                with open(file_path_neo4j, "w", newline="") as file_result_neo4j:
                    writer = csv.DictWriter(
                        file_result_neo4j, fieldnames=["run", "execution_time"])
                    writer.writeheader()
                    writer.writerows([{"run": i, "execution_time": result}
                                      for i, result in enumerate(neo4j_result, 1)])

                summary_neo4j.append({
                    "num_proyek": n_proyek,
                    "best_time": min(neo4j_result),
                    "average_time": round(sum(neo4j_result) / len(neo4j_result), 4),
                    "worst_time": max(neo4j_result)
                })

        # SQL SUMMARY
        sql_summary_path = os.path.join(sql_folder, "sql_summary.csv")
        with open(sql_summary_path, "w", newline="") as file_summary_sql:
            writer = csv.DictWriter(
                file_summary_sql, fieldnames=["num_proyek", "best_time", "average_time", "worst_time"])
            writer.writeheader()
            writer.writerows(summary_sql)

        neo4j_summary_path = os.path.join(neo4j_folder, "neo4j_summary.csv")
        with open(neo4j_summary_path, "w", newline="") as file_summary_neo4j:
            writer = csv.DictWriter(
                file_summary_neo4j, fieldnames=["num_proyek", "best_time", "average_time", "worst_time"])
            writer.writeheader()
            writer.writerows(summary_neo4j)

        summary_all_path = os.path.join(query_folder, "summary.csv")
        with open(summary_all_path, "w", newline="") as file_summary:
            summary_all = [{
                "num_proyek": 10 ** i,
                "average_time_psql": summary_sql[i - 1]["average_time"],
                "average_time_neo4j": summary_neo4j[i - 1]["average_time"]
            } for i in range(1, 6)]
            writer = csv.DictWriter(
                file_summary, fieldnames=["num_proyek", "average_time_psql", "average_time_neo4j"])
            writer.writeheader()
            writer.writerows(summary_all)
