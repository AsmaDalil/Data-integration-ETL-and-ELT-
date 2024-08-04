from datetime import datetime
import psycopg2
import pandas as pd
import csv

# List of Excel file paths
xlsx_files = [
    'sales_records_n1.xlsx',  
    'sales_records_n2.xlsx',
    'sales_records_n3.xlsx',
    'sales_records_n4.xlsx',
    'sales_records_n5.xlsx'
]

# Read each Excel file into a DataFrame and concatenate them
dataframes = [pd.read_excel(file, engine='openpyxl') for file in xlsx_files]
combined_df = pd.concat(dataframes, ignore_index=True)

# Save the combined data to a CSV file
combined_df.to_csv('data.csv', index=False)


# Function to transform and insert data into the PostgreSQL database
def transform():
    with open('data.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        data = list(csv_reader)

    processed_data = []

    for row in data:
        # Transformations
        row[5] = datetime.strptime(row[5], '%m/%d/%Y').strftime('%Y/%m/%d')  # Order Date
        row[7] = datetime.strptime(row[7], '%m/%d/%Y').strftime('%Y/%m/%d')  # Ship Date
        exchange_rate = 0.92
        row[11] = str(float(row[11]) * exchange_rate)  # Total Revenue
        row[12] = str(float(row[12]) * exchange_rate)  # Total Cost
        row[13] = str(float(row[13]) * exchange_rate)  # Total Profit

        # Transform Order Priority
        priority_map = {
            'H': 'High priority',
            'L': 'Low priority',
            'M': 'Medium priority',
            'C': 'Critical priority'
        }
        row[4] = priority_map.get(row[4], 'Normal priority')  # Default to 'Normal priority' if not matched

        processed_data.append(row)  # Move this line inside the loop

    return processed_data

# Function to connect to PostgreSQL
def connect_to_postgres():
    db_config = {
        "host": "localhost",
        "database": "Project1_DATA",
        "user": "postgres",
        "password": "123456*Ac",
        "port": "5432"
    }
    
    return psycopg2.connect(**db_config)


def save(processed_data):
    conn = connect_to_postgres()
    cursor = conn.cursor()

    try:
        cursor.executemany("INSERT INTO data (Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Ship_Date, Units_Sold, Unit_Price, Unit_Cost, Total_Revenue, Total_Cost, Total_Profit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", processed_data)
        conn.commit()  # Commit once after all rows have been inserted
    except Exception as e:
        print(f"Failed to insert rows: {e}")
        conn.rollback()  # Rollback the transaction on error
    finally:
        cursor.close()
        conn.close()

# Execute the transformation and loading process
processed_data = transform()
save(processed_data)




