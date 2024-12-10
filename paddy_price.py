

#july to october


import requests
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import sql

# URL of the target webpage
url="https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity=2&Tx_State=TN&Tx_District=0&Tx_Market=0&DateFrom=01-Jul-2024&DateTo=31-Oct-2024&Fr_Date=01-Jul-2024&To_Date=31-Oct-2024&Tx_Trend=0&Tx_CommodityHead=Paddy(Dhan)(Common)&Tx_StateHead=Tamil+Nadu&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--"

# Request and parse the HTML
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Try to find the table, and if not found, raise an error
table = soup.find("table")
if not table:
    raise ValueError("Could not find the data table on the page.")

# Initialize an empty list to store each row of data
data = []

# Extract table rows, skipping the first two rows (header)
for row in table.find_all("tr")[2:]:
    cells = row.find_all("td")
    
    # Extract cell data and append to data list
    if len(cells) == 10:  # Ensure there are 10 columns in each row
        data.append([
            cells[0].get_text(strip=True),  # NO
            cells[1].get_text(strip=True),  # district_name
            cells[2].get_text(strip=True),  # market_name
            cells[3].get_text(strip=True),  # commodity
            cells[4].get_text(strip=True),  # variety
            cells[5].get_text(strip=True),  # grade
            cells[6].get_text(strip=True),  # Min_Price
            cells[7].get_text(strip=True),  # Max_price
            cells[8].get_text(strip=True),  # model_price
            cells[9].get_text(strip=True)   # price_date
        ])

# Define column names
columns = ["NO", "district_name", "market_name", "commodity", "variety", "grade", "Min_Price", "Max_price", "model_price", "price_date"]

# Convert to DataFrame
df = pd.DataFrame(data, columns=columns)

# Save to CSV (optional)
df.to_csv('paddy_price.csv', index=False)
print("Data saved to paddy_price.csv")

# Database connection settings
db_config = {
    "dbname": "test",
    "user": "admin",
    "password": "godspeed123",
    "host": "192.168.50.26",
    "port": "5431"
}

try:
    # Connect to PostgreSQL
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS paddy_price (
        NO SERIAL PRIMARY KEY,
        district_name VARCHAR(100),
        market_name VARCHAR(100),
        commodity VARCHAR(50),
        variety VARCHAR(50),
        grade VARCHAR(50),
        min_price VARCHAR(20),
        max_price VARCHAR(20),
        model_price VARCHAR(20),
        price_date VARCHAR(20)
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    print("Table is ready in PostgreSQL.")

    # Insert data into the PostgreSQL table
    for _, row in df.iterrows():
        insert_query = """
        INSERT INTO paddy_price (district_name, market_name, commodity, variety, grade, min_price, max_price, model_price, price_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        data_tuple = (
            row["district_name"], row["market_name"], row["commodity"],
            row["variety"], row["grade"], row["Min_Price"],
            row["Max_price"], row["model_price"], row["price_date"]
        )
        cursor.execute(insert_query, data_tuple)

    # Commit the transaction
    connection.commit()
    print("Data has been inserted into PostgreSQL.")

except Exception as e:
    print("An error occurred:", e)

finally:
    # Close the database connection
    if connection:
        cursor.close()
        connection.close()







#CREATE SCHEMA IF NOT EXISTS market_data;
