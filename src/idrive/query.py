#!/usr/bin/env python

import argparse
import os
import pathlib
import sqlite3 as sql

from idrive.db_sqlite import idrive_db_select_files
from idrive.util import strip1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', '--database', type=pathlib.Path, default='index.db')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    db_path = args.database
    verbose = args.verbose

    if not os.path.isfile(db_path):
        parser.error(f"ERROR: {db_path} is not a valid database")
        return

    connection = sql.connect(f"file:{db_path}?mode=ro", uri=True)

    cursor = connection.cursor()
    results = idrive_db_select_files(cursor)
    for file in results:
        print(strip1(file['ibfolder.NAME'], "'") + strip1(file['ibfile.NAME'], "'"))

if __name__ == '__main__':
    main()
