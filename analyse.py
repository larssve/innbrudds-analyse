#!venv/bin/python3
import argparse
import duckdb
import logging
import os
import sys

DB_FILE = "db.duck"
INIT_DB = "sql/init_db.sql"

logger = logging.getLogger(__name__)

def hotspots(con):
    logger.debug("Starting hotspot analysis")
    return sql_from_file("sql/hotspots.sql", con)

def trends(con):
    logger.debug("Starting trend analysis")
    return sql_from_file("sql/trends.sql", con)

def sql_from_file(filename, con):
    with open(filename, "r") as f:
        return con.sql(f.read())

def handle_default(df):
    logging.debug("handle_default")
    df.show()
    
def handle_csv(df):
    logging.debug("handle_csv")
    df.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="./analyse.py",
        description="lager analyser av <filename>"
    )
    
    parser.add_argument("filename")
    parser.add_argument("-v", "--verbose", action="store_const", const=logging.DEBUG, default=logging.INFO)
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--csv", action="store_const", const=handle_csv, default=handle_default, dest="output")

    args = parser.parse_args()
    logging.basicConfig(level=args.verbose)

    logger.debug(f"Starting analysis program, file: {args.filename}")
    if not os.path.isfile(args.filename):
        logger.error(f"Could not find file '{args.filename}'")
        sys.exit(1)

    logger.debug(f"Connecting to db {DB_FILE}")
    with duckdb.connect(DB_FILE) as con:
        with open(INIT_DB, "r") as sql:
            logger.debug(f"Initializing db")
            con.execute(sql.read(), {"csv_file": args.filename})

            data = [hotspots(con),
                    trends(con)]

            for analysis in data:
                args.output(analysis)
            
