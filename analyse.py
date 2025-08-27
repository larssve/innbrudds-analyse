#!venv/bin/python3
import argparse
import duckdb

DB_FILE = "db.duck"
INIT_DB = "sql/init_db.sql"


def hotspots(con):
    vprint("[*] Starting hotspot analysis")
    return sql_from_file("sql/hotspots.sql", con)

def trends(con):
    vprint("[*] Starting trend analysis")
    return sql_from_file("sql/trends.sql", con)

def sql_from_file(filename, con):
    with open(filename, "r") as f:
        return con.sql(f.read())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="./analyse.py",
        description="lager analyser av <filename>"
    )
    
    parser.add_argument("filename")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    # print function for verbose mode
    vprint = print if args.verbose else lambda *p : None

    vprint(f"[*] Starting analysis program, file: {args.filename}")

    # check if input file exists os.path.isfile(args.filename)

    vprint(f"[*] Connecting to db {DB_FILE}")
    with duckdb.connect(DB_FILE) as con:
        with open(INIT_DB, "r") as sql:
            vprint(f"[*] Initializing db")
            con.execute(sql.read(), {"csv_file": args.filename})

        hotspots(con).show()
        trends(con).show()
    
