
import snowflake.connector
import os
from dotenv import load_dotenv
import os

load_dotenv("/opt/airflow/project/.env") 
#Get password from os var

password =  os.getenv("Amusement_Pass")
account =  os.getenv("Snow_Account")
# Establish connection
conn = snowflake.connector.connect(
    user="AmusmentParkAdmin",
    password=password,
    account=account, 
    warehouse="COMPUTE_WH",
    database="AMUSEMENTPARK",
    authenticator="snowflake"
)

# Create a cursor object
cur = conn.cursor()

print(cur)

