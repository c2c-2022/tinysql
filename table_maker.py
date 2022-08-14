from typing import  List
import sqlite3
import os

class TableLoader:
    def __init__(self, table_data: List[List[str]], table_name: str, database_file=None):
        """
        Create an in-memory table

        The first list is column names, subsequent ones are data
        Expect each to have the same number of items
        Populates a table in sqlite
        """

        self.table_name=table_name
        # self.database_name = '/tmp/thowaway.sqlite3'

        if database_file is None:
            self.connection = sqlite3.connect('file::memory:?cache=shared', uri=True)
        else:
            self.connection = sqlite3.connect(database_file)

        header = table_data.pop(0)

        column_string = [f"'{x}'" for x in header]
        column_string = ','.join(column_string)
        print(f"{column_string=}")

        create_table = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({column_string});"
        self.connection.execute(create_table)
        cur = self.connection.cursor()
        parameter_string = ','.join('?'*len(header))
        cur.executemany(f"Insert into {self.table_name} ({column_string}) values ({parameter_string})", table_data)
        self.connection.commit()

    def get_select_star(self):
        cur = self.connection.cursor()
        return cur.execute(f"SELECT * FROM {self.table_name};")

    def run_select(self, select):
        return self.connection.execute(select)


if __name__  == "__main__":
    cute = TableLoader([['thrice', 'twice', 'planet_id'], ['hi', 'sailor', 5], ['love', 'always', 5]], 'chromium')
    cuter = TableLoader([['id', 'name', 'diameter'], [1, 'mornin', 'fish'], [5, 'evening', 'tide']], 'planet')
    cute_data = cute.get_select_star()
    from pprint import pprint
    pprint(list(cute_data))
    join_data = cuter.run_select("SELECT planet.name , max(twice) as max_twice, min(twice) as min_twice"
                                 " FROM chromium join planet on planet.id = chromium.planet_id "
                                 " GROUP BY planet.name")

    pprint(list(join_data))
    pprint(join_data.description)

