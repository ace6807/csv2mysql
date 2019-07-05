import sys
import re

import pymysql
import pandas as pd
import click

dataframe_type_map = {
    'float64': 'DOUBLE',
    'object': 'VARCHAR(1000)',
    'int64': 'INT'
}


def clean_column_name(column):
    "Not going to worry about the regex for now"
    column = column.replace("(", "_")
    column = column.replace(")", "")  # Had to change this to an empty space
    column = column.replace("-", "_")
    column = column.replace(" ", "_")
    column = column.replace("&", "_")
    column = column.replace("!", "_")
    column = column.replace("@", "_")
    column = column.replace("#", "_")
    column = column.replace("$", "_")
    column = column.replace("%", "_")
    column = column.replace("^", "_")
    column = column.replace("*", "_")
    column = column.replace("=", "_")
    column = column.replace("+", "_")
    column = column.replace("`", "_")
    column = column.replace("~", "_")
    column = column.replace("[", "_")
    column = column.replace("]", "_")
    column = column.replace("{", "_")
    column = column.replace("}", "_")
    column = column.replace(";", "_")
    column = column.replace(":", "_")
    column = column.replace("\'", "")
    column = column.replace("\"", "")
    # issue with slashes here- if we happen to have \n or \a, issues arise:
    column = column.replace("\a", "_a")
    column = column.replace("\n", "_n")
    column = column.replace("\b", "_b")
    column = column.replace("\f", "_f")
    # with these out of the way, we can procede like usual
    column = column.replace("\\", "_")
    column = column.replace(",", "_")
    column = column.replace(".", "_")
    column = column.replace("<", "_")
    column = column.replace(">", "_")
    column = column.replace("/", "_")
    # that ought to do it. I'm not manually checking unicode for emojis or anything crazy here, just things that might actually happen
    # a little formatting afterward:
    column = column.replace("__", "_")
    column = column.lower()  # Just so the column names are a little more consistent
    return column


def get_dataframe_column_type(df, column):
    """
    Returns the DB type that corresponds to the given column in the given df
    or None if unmapped type is found
    """
    try:
        return dataframe_type_map[df[column].dtypes.name]
    except KeyError as e:
        print(f"Incompatible data type found in column {column}")
        return None


def create_table(df, table_name, conn):
    """Create the new table using the df. Return only columns where datatype could be mapped"""

    columns = df.columns.values
    new_db_column_names = {clean_column_name(column): get_dataframe_column_type(df, column) for column in columns}

    # Only use the columns where the mapping didn't produce None
    valid = {k: v for k, v in new_db_column_names.items() if v}

    create_table_qs = f"CREATE TABLE {table_name} ({', '.join([f'{k} {v}' for k, v in valid.items()])});"

    with conn.cursor() as cur:
        cur.execute(create_table_qs)

    return valid


def load_table_from_file(conn, filepath, table_name):
    """
    To load from a local file I needed to log into the server using the mysql cli tool and run
    SET GLOBAL local_infile = ON
    and also pass local_infile=True
    """

    load_data_qs = f"""
    LOAD DATA LOCAL INFILE '{filepath}' 
    INTO TABLE {table_name}
    FIELDS TERMINATED BY ','
    ENCLOSED BY ''
    LINES TERMINATED BY '\n' 
    IGNORE 1 LINES;
    """

    with conn.cursor() as cur:
        cur.execute(load_data_qs)

    conn.commit()


@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('table_name')
def main(file_path, table_name):

    df = pd.read_csv(file_path)

    # I would never put real credentials or connection params in the code.
    # These should be in a config file that doesn't get checked
    # into source control and read out from there
    conn = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='root',
        db='testdb',
        local_infile=True,
    )

    db_columns = create_table(df, table_name, conn)
    load_table_from_file(conn, file_path, table_name)


if __name__ == '__main__':
    main()
