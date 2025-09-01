import json
from mongo_helper import get_mongo_data
import pandas as pd

from s3_helper import upload_file_to_s3

def get_provider_data_from_npi(npi):
    query = {
        "certification.ACLS": {
            "$elemMatch": {
            "npi": npi
            }
        }
    }

    provider_data=get_mongo_data("providers",query)
    return provider_data

def write_to_excel(data,filename="tmp/provider_data.xlsx"):
    if not data:
        return ("No data found")
    df = pd.DataFrame(data if isinstance(data, list) else [data])
    df.to_excel(filename, index=False)
    print(f"Data written to {filename}")


def lambda_handler(event, context):
    npi = event.get("npi")
    if not npi:
        return {
            'statusCode': 400,
            'body': json.dumps("NPI not provided.")
        }

    provider_data = get_provider_data_from_npi(npi)
    print(provider_data)
    file_path = write_to_excel(provider_data)

    if file_path == "No data found":
        return {
            'statusCode': 404,
            'body': json.dumps("No data found for the provided NPI.")
        }

    # Optional: upload to S3
    s3_url = upload_file_to_s3(file_path, event.get("bucket_name"), f"{npi}/provider_data.xlsx")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Excel file uploaded to: {s3_url}")
    }

# get the providers data for the providers data by id

def get_providers_data(provider_id):
    mongo_query={
        "UUID":provider_id
    }
    projection = {
    "_id": 1,                   
    "personal_information": 1,  
    "speciality": 1             
    }
    provider=get_mongo_data('providers',mongo_query,projection)
    return provider
    