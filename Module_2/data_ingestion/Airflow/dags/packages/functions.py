import numpy as np
import pandas as pd
import dotenv
import os
import json
import regex as re

from dotenv import load_dotenv
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook


conn = BaseHook.get_connection("DATAFRIK_DEST_DB")
# GET DB CREDENTIALS
#load_dotenv()

DB_NAME = conn.schema
DB_USER = conn.login
DB_PASS = conn.password
DB_HOST = conn.host
DB_PORT= conn.port

DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATA_PATH = "/opt/airflow/data/"

employees_path =  os.path.join(DATA_PATH, "employees.csv") 
customers_path =  os.path.join(DATA_PATH, "customers.csv") 
products_path =  os.path.join(DATA_PATH, "products.csv")
stores_path =  os.path.join(DATA_PATH, "stores.csv") 
orders_path =  os.path.join(DATA_PATH, "orders.json") 

def extract():
    """"
    Extract data from the files and return a tuple of dictionaries
    """

    df_employees = pd.read_csv(employees_path) if os.path.exists(employees_path) else pd.DataFrame()
    df_customers = pd.read_csv(customers_path) if os.path.exists(customers_path) else pd.DataFrame()
    df_products = pd.read_csv(products_path) if os.path.exists(products_path) else pd.DataFrame()
    df_stores = pd.read_csv(stores_path) if os.path.exists(stores_path) else pd.DataFrame()

    with open(orders_path, "r") as file:
        orders = json.load(file)

    return df_employees.to_dict(), df_customers.to_dict(), df_products.to_dict(),df_stores.to_dict(),orders

"""   
    Transform the data inorder to match the expected data format in our destination db i.e
    - Full_Name in customer + employee table
    - Order_items table
    - Ensure validity of data being entered
    - No duplicate values
    - Fill the missing data e.g Employee data has missing phone number and emails.
    """  

   # Check validity

def is_valid_emaiil(email):
   
   #Returns True is email follows a valid format, else False

  pattern = r"ˆ[a-zA-Z0-9_.+]+@[a-zA-Z0-9]+\.[a-zA-Z0-9-.]+$"
  return bool(re.match(pattern,email))


def is_valid_phone(phone):
  pattern = r"ˆ\+?\d{1,10}$"
  return bool(re.match(pattern,phone))

extracted_data = extract()


def transform(extracted_data):

  # We transform dataframes hence, change the dicts back to dataframes
  df_employee = pd.DataFrame(extracted_data[0])
  df_customers = pd.DataFrame(extracted_data[1])
  df_products = pd.DataFrame(extracted_data[2])
  df_stores = pd.DataFrame(extracted_data[3])

  
  #print(df_employee)

  # Fill missing values
  for col in ["Phone","Email","First_Name","Last_Name"]:
    if col not in df_employee.columns:
      df_employee[col]  = np.nan

  for col in ["Phone","Email","First_Name","Last_Name"]:
    if col not in df_customers.columns:
      df_customers[col] = np.nan

  
  #print(df_products)
  # drop records that lack crictical data i.e Primary key columns in db

  df_customers = df_customers.dropna(subset = ["Customer_ID"])
  df_employee = df_employee.dropna(subset = ["Employee_ID"])
  df_stores = df_stores.dropna(subset = ["Store_ID"])
  df_products = df_products.dropna(subset=["Product_ID"])
  
  # Check validity and replace invalid fields
  df_employee["Email"] = df_employee["Email"].apply(lambda x: x if is_valid_emaiil(str(x)) else "Unknown")
  df_customers["Email"] = df_customers["Email"].apply(lambda x: x if is_valid_emaiil(str(x)) else "Unknown")

  df_employee["Phone"] = df_employee["Phone"].apply(lambda x: x if is_valid_phone(str(x)) else "Unknown")
  df_customers["Phone"] = df_customers["Phone"].apply(lambda x: x if is_valid_phone(str(x)) else "Unknown")

  # Merge our Full_Name bt first fill that record if value is missing

  df_customers["Full_Name"] = df_customers["First_Name"].fillna("") + " " + df_customers["Last_Name"].fillna("")
  df_employee["Full_Name"] = df_employee["First_Name"].fillna("") + " " + df_employee["Last_Name"].fillna("")

  # drop First name and last name columns since we have the full name - no redundancy
  df_employee.drop(columns=["First_Name","Last_Name"], inplace=True)
  df_customers.drop(columns=["First_Name","Last_Name"], inplace=True)

  # Drop duplicates
  df_employee.drop_duplicates(inplace=True)
  df_customers.drop_duplicates(inplace=True)
  df_stores.drop_duplicates(inplace=True)
  df_products.drop_duplicates(inplace=True)

  # Get orders and order_items from the orders json data
  orders = []
  order_items = []
  orders_json = extracted_data[4]

  for order in orders_json:
    orders.append([
      order["Order_ID"],order["Customer_ID"],order["Order_Date"],order["Store_ID"],order["Total_Amount"]])
    for item in order['Items']:
      order_items.append([order['Order_ID'],item['Product_ID'],item['Quantity'],item['Unit_Price']])
    
  # Transform the lists into a dataframe so we can do further transformations and later dump into db

  df_orders = pd.DataFrame(orders,columns=["Order_ID","Customer_ID","Order_Date","Store_ID","Total_Amount"])

  df_order_items = pd.DataFrame(order_items, columns=["Order_ID","Product_ID","Quantity","Unit_Price"])
  df_order_items.insert(0, 'Order_Item_ID', range(1, len(df_order_items) + 1))

  # Per usual, drop duplicates

  df_orders.drop_duplicates(inplace=True)
  df_order_items.drop_duplicates(inplace=True)


  return df_employee.to_dict(),df_customers.to_dict(),df_products.to_dict(),df_stores.to_dict(),df_orders.to_dict(),df_order_items.to_dict()
        

#transform(extracted_data)

transformed_Data = transform(extracted_data)
def load(transformed_Data):
  df_employee = pd.DataFrame(transformed_Data[0])
  df_customers = pd.DataFrame(transformed_Data[1])
  df_products = pd.DataFrame(transformed_Data[2])
  df_stores = pd.DataFrame(transformed_Data[3])
  df_orders = pd.DataFrame(transformed_Data[4])
  df_order_items = pd.DataFrame(transformed_Data[5])

  print(df_stores)

  engine = create_engine(DB_CONNECTION_STRING)

  if not df_employee.empty:
    df_employee.to_sql("employees", engine, if_exists="append", index=False)
  
  if not df_customers.empty:
    df_customers.to_sql("customers", engine, if_exists="append",index=False)

  if not df_products.empty:
    df_products.to_sql("products", engine, if_exists="append",index=False)

  if not df_stores.empty:
    df_stores.to_sql("stores", engine, if_exists="append",index=False)

  if not df_orders.empty:
    df_orders.to_sql("orders", engine, if_exists="append",index=False)

  if not df_order_items.empty:
    df_order_items.to_sql("order_items", engine, if_exists="append",index=False)

load(transformed_Data)
print("Data loaded successfully")


