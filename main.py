import pandas as pd
import sqlite3
import os
import re
directory = r"\PycharmProjects\IP block package\country_ip_ranges"


conn = sqlite3.connect('ip_ranges.db')

expected_num_columns = 5

for filename in os.listdir(directory):
    if filename.endswith('.csv'):  # Check whether the file is a CSV
        table_name = os.path.splitext(filename)[0]  # Get the file name without the extension for the table name
        file_path = os.path.join(directory, filename)

        data = []

        # Read each line in the file manually
        with open(file_path, 'r', encoding='utf-8') as file:
            for line_number, line in enumerate(file, start=1):
                line = line.strip()
                if not line:
                    continue

                parts = line.split(',', maxsplit=4)

                if len(parts) == expected_num_columns:
                    data.append(parts)
                else:
                    print(
                        f"Skipping line {line_number} in {filename}: {len(parts)} fields found but {expected_num_columns} expected.")

        if data:
            df = pd.DataFrame(data, columns=['start_ip', 'end_ip', 'size', 'date', 'owner'])

            df['owner'] = df['owner'].ffill()

            df['date'] = pd.to_datetime(df['date'], format='%d/%m/%y', errors='coerce').dt.date

            table_name = re.sub(r'\W+', '_', table_name)

            cursor = conn.cursor()
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    start_ip TEXT,
                    end_ip TEXT,
                    size INTEGER,
                    date DATE,
                    owner TEXT
                )
            ''')
            conn.commit()

            df.to_sql(table_name, conn, if_exists='replace', index=False)
        else:
            print(f"No data to process in {filename}.")

conn.close()
