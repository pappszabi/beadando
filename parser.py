import pandas as pd
from influxdb import DataFrameClient

# InfluxDB connection parameters
user = 'root'
password = 'root'
host='influxdb'
port=8086
dbname='new_custom_db'
protocol = 'line'

# Create InfluxDB client
client = DataFrameClient(host, port, user, password, dbname)

# Read the CSV file into a DataFrame
df = pd.read_csv('new_custom_db.csv')

# Parse dates
df['szepdatum'] = pd.to_datetime(df['Subscription Date'], dayfirst=False)

# Set the index to be the datetime of the Subscription Date and Time
df.index = pd.to_datetime(df['szepdatum'].astype(str) + 'T' + df['Time'])

# Sort the DataFrame by index
df = df.sort_index(ascending=False)

# Define the field and tag columns
field_columns = ['FN', 'LN']
tag_columns = ['Subscription Date', 'Company', 'City']

# Subset the DataFrame
df2 = df[field_columns + tag_columns].head(500)

# Drop and recreate the database and retention policy
client.drop_database(dbname)
client.create_database(dbname)
client.create_retention_policy(dbname, '1000d', 1, default=True)

# Write the points to InfluxDB
client.write_points(df2, 'new_custom_db', protocol=protocol,
                    field_columns=field_columns,
                    tag_columns=tag_columns)
