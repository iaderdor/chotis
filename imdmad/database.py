import sqlite3
import datetime


class Database:
    def __init__(self):
        self.conn = None

        try:
            self.conn = sqlite3.connect("conf/database.sqlite3")
        except sqlite3.Error as e:
            print(e)
        finally:
            if self.conn:
                self.conn.close

    def __del__(self):
        self.conn.close()

    def __create_datasets(self):
        c = self.conn.cursor()
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS datasets (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            dataset_last_modified TEXT NOT NULL,
            timestamp_created_at TEXT NOT NULL,
            timestamp_updated_at TEXT NOT NULL
        );
        """
        )
        self.conn.commit()

    def __create_stations(self):
        c = self.conn.cursor()
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            element_type TEXT NOT NULL,
            x_utm REAL NOT NULL,
            y_utm REAL NOT NULL,
            timestamp_created_at TEXT NOT NULL,
            timestamp_updated_at TEXT NOT NULL
        );
        """
        )

        self.conn.commit()

    def __create_counts(self):
        c = self.conn.cursor()
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS counts (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            type INTEGER NOT NULL,
            station_id INTEGER NOT NULL,
            datetime TEXT NOT NULL,
            intensity TEXT NOT NULL,
            timestamp_created_at TEXT NOT NULL,
            timestamp_updated_at TEXT NOT NULL,
            FOREIGN KEY(station_id) REFERENCES stations(id)
        );
        """
        )

        self.conn.commit()

    def __create_daily_counts(self):
        c = self.conn.cursor()
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS counts (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            type INTEGER NOT NULL,
            station_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            intensity TEXT NOT NULL,
            timestamp_created_at TEXT NOT NULL,
            timestamp_updated_at TEXT NOT NULL,
            FOREIGN KEY(station_id) REFERENCES stations(id)
        );
        """
        )

        self.conn.commit()

    def initialize(self):
        self.__create_datasets()
        self.__create_stations()
        self.__create_counts()
        self.__create_daily_counts()
