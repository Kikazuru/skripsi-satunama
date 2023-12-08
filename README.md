# Data Mart Manajemen Proyek Yayasan SATUNAMA

> Source code ETL dengan Python untuk data mart

## Virtual Environment (venv)

> venv digunakan untuk mengisolasi library dari projek lain

1.  Install

    ```sh
    pip install virtualenv
    ```

2.  Create

    ```sh
    python -m venv <virtual-environment-name>
    ```

3.  Activate

    - On Unix or MacOS, using the bash shell

    ```sh
    source /path/to/venv/bin/activate
    ```

    - On Unix or MacOS, using the csh shell

    ```sh
    source /path/to/venv/bin/activate.csh
    ```

    - On Unix or MacOS, using the fish shell

    ```sh
    source /path/to/venv/bin/activate.fish
    ```

    - On Windows using the Command Prompt

    ```sh
    path\to\venv\Scripts\activate.bat
    ```

    - On Windows using PowerShell

    ```sh
    path\to\venv\Scripts\Activate.ps1
    ```

4.  Install Project Requirement
    ```sh
    pip install -r requirements.txt
    ```

## Add Env File

> Tambahkan file .env pada root folder untuk mengisi nama database operasional, data mart postgresql dan data mart neo4J. OP = operasional, DM_PSQL = data mart postgre sql, DM_NEO4J = data mart neo4j

```
DBHOST_OP="localhost"
DBNAME_OP="nama_database"
DBUSER_OP="username_database"
DBPASS_OP="password_database"

DBHOST_DM_PSQL="localhost"
DBNAME_DM_PSQL="nama_database_op"
DBUSER_DM_PSQL="username_datamart"
DBPASS_DM_PSQL="password_datamart"

DBURI_DM_NEO4J="neo4j://localhost:7687/"
DBNAME_DM_NEO4J="nama_database"
DBUSER_DM_NEO4J="neo4j"
DBPASS_DM_NEO4J="password_neo4j"
```
