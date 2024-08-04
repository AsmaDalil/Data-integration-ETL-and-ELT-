from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
import pandas as pd
import psycopg2
import csv
import xml.etree.ElementTree as ET
import requests
import io

@task
def read_and_combine_excel_files():
    base_url = 'https://raw.githubusercontent.com/anbento0490/tutorials/master/sales_xlsx/'
    

    dataframes = []
    for file_name in file_names:
        url = base_url + file_name
        response = requests.get(url)
        if response.status_code == 200:
            excel_file = io.BytesIO(response.content)
            df = pd.read_excel(excel_file)
            dataframes.append(df)
        else:
            raise Exception(f'Failed to download {file_name} from {url}')

    combined_df = pd.concat(dataframes, ignore_index=True)
    csv_file_path = 'C:/Users/HP/OneDrive - Al Akhawayn University in Ifrane/Documents/Ma_DataEng/projectOnw/data.csv'
    combined_df.to_csv(csv_file_path, index=False)
    return csv_file_path  # Return the file path for further processing


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

@task
def extract_and_save_xml(xml_file_path: str):  # Specify the parameter type
    tree = ET.parse(xml_file_path)
    root = tree.getroot()

    data = []
    for record in root.findall('record'):
        row = [
            record.find('Region').text,
            record.find('Country').text,
            record.find('Item_Type').text,
            record.find('Sales_Channel').text,
            record.find('Order_Priority').text,
            record.find('Order_Date').text,
            record.find('Order_ID').text,
            record.find('Ship_Date').text,
            record.find('Units_Sold').text,
            record.find('Unit_Price').text,
            record.find('Unit_Cost').text,
            record.find('Total_Revenue').text,
            record.find('Total_Cost').text,
            record.find('Total_Profit').text
        ]
        data.append(row)

    with open('xml_data.csv', 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([
            'Region', 'Country', 'Item_Type', 'Sales_Channel', 'Order_Priority',
            'Order_Date', 'Order_ID', 'Ship_Date', 'Units_Sold', 'Unit_Price',
            'Unit_Cost', 'Total_Revenue', 'Total_Cost', 'Total_Profit'
        ])
        csv_writer.writerows(data)

    return 'xml_data.csv'

@task
def transform(file_path: str):
    with open(file_path, 'r') as csv_file:
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

@task
def save(processed_data):
    conn = connect_to_postgres()
    cursor = conn.cursor()

    for row in processed_data:
        cursor.execute("INSERT INTO Sales (Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Ship_Date, Units_Sold, Unit_Price, Unit_Cost, Total_Revenue, Total_Cost, Total_Profit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13]))

    conn.commit()
    conn.close()

# DAG configurations
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# Define the DAG
with DAG('data_elt', default_args=default_args, schedule_interval='@daily') as dag:
    start_task = DummyOperator(task_id='start')
    
 


    read_excel_task = read_and_combine_excel_files()
    extract_xml_task = extract_and_save_xml('/path/to/xml_file.xml')  
    transform_task = transform(read_excel_task) # Pass the file path from read_excel_task
    load_task = save(transform_task)


    end_task = DummyOperator(task_id='end')

    start_task >> read_excel_task >> transform_task >> load_task >> end_task
    start_task >> extract_xml_task >> end_task




