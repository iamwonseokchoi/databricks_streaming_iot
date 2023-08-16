import pandas as pd
import time
from azure.storage.blob import BlobServiceClient
from faker import Faker

def simulate_IoT_device():
    fake = Faker()

    account_name = "<storage_account>"
    account_key = "<key>"
    container_name = "<container>"

    # Connect to the Azure storage account
    blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
    container_client = blob_service_client.get_container_client(container_name)

    index = 0
    while index <= 9_999:
        rows = [] # List to collect the 10 rows
        for _ in range(10):
            row_data = {
                'Recorded_At': pd.Timestamp.now(),
                'Device': f"Device-00{fake.random_number(digits=1)}",
                'Signal_Value': fake.pydecimal(left_digits=2, right_digits=3, positive=True),
                'Latitude': fake.latitude(),
                'Longitude': fake.longitude()
            }
            rows.append(row_data)
        df = pd.DataFrame(rows)

        file_name = f"mock_data_{index}.json"

        df.to_json(file_name, orient='records', lines=True)

        # Upload to Azure Container
        with open(file_name, 'rb') as data:
            blob_client = container_client.upload_blob(name=f"/path/{file_name}", data=data)

        print(f'File {file_name} uploaded')
        index += 1
        # force sleep for 10 seconds
        time.sleep(5)
        

if __name__ == '__main__':
    simulate_IoT_device()
