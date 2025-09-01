#!venv/bin/python
import argparse
import logging
import os
import sys

import duckdb
import pandas

DB_FILE = "db.duck"
INIT_DB = "sql/init_db.sql"

logger = logging.getLogger(__name__)

## analyses
def hotspots(db):
    logger.debug("Starting hotspot analysis")
    return sql_from_file("sql/hotspots.sql", db), "hotspots"

def trends(db):
    logger.debug("Starting trend analysis")
    return sql_from_file("sql/trends.sql", db), "trends"

def sql_from_file(filename, db):
    with open(filename, "r", encoding="utf-8") as f:
        return db.sql(f.read())

## output handlers
def output_default(analyses):
    logger.debug("output default")
    for df,_ in analyses:
        df.show()

def output_csv(analyses):
    logging.debug("output csv")
    for df,filename in analyses:
        logger.info("Skriver analyse til %s.csv", filename)
        df.to_csv(f"{filename}.csv")

def output_excel(analyses):
    output = "rapport.xlsx"
    logging.debug("output excel")
    with pandas.ExcelWriter(output) as writer:
        logger.info("Lagrer rapport som %s", output)
        for df,sheet_name in analyses:
            df.to_df().to_excel(writer, sheet_name=sheet_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="./analyse.py",
        description="lager analyser av <filnavn>"
    )

    parser.add_argument("filename")
    parser.add_argument("-v", "--verbose", action="store_const", const=logging.DEBUG, default=logging.INFO)
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--csv", action="store_const", const=output_csv, dest="output")
    output_group.add_argument("--excel", action="store_const", const=output_excel, dest="output")
    output_group.set_defaults(output=output_default)

    args = parser.parse_args()
    logging.basicConfig(level=args.verbose)

    logger.debug("Starting analysis program, file: %s", args.filename)
    if not os.path.isfile(args.filename):
        logger.error("Finner ikke fil %s", args.filename)
        sys.exit(1)

    logger.debug("Connecting to db %s", DB_FILE)
    with duckdb.connect(DB_FILE) as con:
        with open(INIT_DB, "r", encoding="utf-8") as sql:
            logger.debug("Initializing db")
            con.execute(sql.read(), {"csv_file": args.filename})

            data = [hotspots(con),
                    trends(con)]

            args.output(data)
