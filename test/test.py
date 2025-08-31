import unittest
import duckdb
import csv
import os

from analyse import INIT_DB

class TestRequirements(unittest.TestCase):
    def test_innbrudsdato_format(self):
        """
        Tests that the date format is formatted YYYY-MM-DD
        """
        test_file = "test/feil-format.csv"
        test_data = [("01-05-2025","DD-MM-YYYY"),
                     ("2025-13-01","YYYY-DD-MM"),
                     ("13-05-2025","MM-DD-YYYY"),
                     ("2025/01/05","YYYY/MM/DD")]

        with open(INIT_DB, "r") as sql_file:
            sql = sql_file.read()

            for test_date, test_format in test_data:
                with open(test_file, "w") as csvfile:
                    w = csv.writer(csvfile)
                    mottatt_tid = "2025-01-05T10:15:08Z"
                    sted = "Bislett"
                    w.writerow(["mottatt_tid", "innbruddsdato", "stedsnavn"])
                    w.writerow([mottatt_tid,"2025-01-05",sted])
                    w.writerow([mottatt_tid,test_date,sted])

                with self.subTest(msg="Test format: tf, date: td", tf=test_format, td=test_date):
                    with duckdb.connect() as con:
                        con.execute(sql, {"csv_file": test_file})

                        actual, = con.sql("SELECT COUNT(*) FROM innbruddstips").fetchone()
                        expected = 1
                        self.assertEqual(actual, expected)

                        errors, = con.sql("SELECT COUNT(*) FROM reject_errors").fetchone()
                        expected_errors = 1
                        self.assertEqual(errors, expected_errors)

        os.remove(test_file)

    def test_duplicates(self):
        """
        Tests that duplicate rows are only included once
        """
        test_file = "test/test-duplicates.csv"

        with open(INIT_DB, "r") as sql_file:
            sql = sql_file.read()

            with open(test_file, "w") as csvfile:
                w = csv.writer(csvfile)
                w.writerow(["mottatt_tid", "innbruddsdato", "stedsnavn"])
                w.writerow(["2025-04-04T21:10:00Z","2025-04-04","Bislett"])
                w.writerow(["2025-04-04T21:10:00Z","2025-04-04","Bislett"])

            with duckdb.connect() as con:
                con.execute(sql, {"csv_file": test_file})

                actual, = con.sql("SELECT COUNT(*) FROM innbruddstips").fetchone()
                expected = 1
                self.assertEqual(actual, expected)

