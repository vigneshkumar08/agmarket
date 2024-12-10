import requests
import pandas as pd
from bs4 import BeautifulSoup


# URL of the target webpage
url = "https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity=23&Tx_State=TN&Tx_District=0&Tx_Market=0&DateFrom=07-Oct-2024&DateTo=07-Oct-2024&Fr_Date=07-Oct-2024&To_Date=07-Oct-2024&Tx_Trend=0&Tx_CommodityHead=Onion&Tx_StateHead=Tamil+Nadu&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--",
url="https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity=23&Tx_State=TN&Tx_District=0&Tx_Market=0&DateFrom=08-Oct-2024&DateTo=08-Oct-2024&Fr_Date=08-Oct-2024&To_Date=08-Oct-2024&Tx_Trend=0&Tx_CommodityHead=Onion&Tx_StateHead=Tamil+Nadu&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--",
url="https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity=23&Tx_State=TN&Tx_District=0&Tx_Market=0&DateFrom=09-Oct-2024&DateTo=09-Oct-2024&Fr_Date=09-Oct-2024&To_Date=09-Oct-2024&Tx_Trend=0&Tx_CommodityHead=Onion&Tx_StateHead=Tamil+Nadu&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--",
url="https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity=23&Tx_State=TN&Tx_District=0&Tx_Market=0&DateFrom=10-Oct-2024&DateTo=07-Nov-2024&Fr_Date=10-Oct-2024&To_Date=07-Nov-2024&Tx_Trend=0&Tx_CommodityHead=Onion&Tx_StateHead=Tamil+Nadu&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--"

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
df.to_csv('onion.csv', index=False)
print("Data saved to onion.csv")
