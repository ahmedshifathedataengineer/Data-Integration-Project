#This file will handle the extraction of data from the Shipping Partner API.
# shipping_data.py
import requests
import pandas as pd

def extract_shipping_data(api_url):
    """
    Extracts data from the Shipping Partner API.

    Args:
    api_url (str): The URL of the API endpoint.

    Returns:
    DataFrame: A DataFrame containing the extracted data.
    """
    try:
        # Send a GET request to the API endpoint
        response = requests.get(api_url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Extract JSON data from the response
            shipping_data = response.json()

            # Check if JSON has the expected structure
            if 'iss_position' in shipping_data and 'timestamp' in shipping_data:
                # Flatten the nested structure and create a DataFrame
                df_data = {
                    'latitude': [shipping_data['iss_position']['latitude']],
                    'longitude': [shipping_data['iss_position']['longitude']],
                    'timestamp': [shipping_data['timestamp']]
                }
                shipping_df = pd.DataFrame(df_data)
                return shipping_df
            else:
                print("Unexpected JSON structure")
                return pd.DataFrame()
        else:
            print(f"Failed to retrieve data from API. Status code: {response.status_code}")
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while making the API request: {e}")
        return pd.DataFrame()

# Example usage
if __name__ == "__main__":
    api_url = "http://api.open-notify.org/iss-now.json"
    shipping_df = extract_shipping_data(api_url)
    print("Shipping Data:")
    print(shipping_df.head())
