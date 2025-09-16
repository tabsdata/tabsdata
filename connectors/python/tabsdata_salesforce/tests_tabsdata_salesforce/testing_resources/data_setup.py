#
#  Copyright 2025. Tabs Data Inc.
#

import logging
import os

import pandas as pd
from simple_salesforce import Salesforce

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sf_connection():
    # Connect to Salesforce
    username = os.getenv("SF0__USERNAME")
    password = os.getenv("SF0__PASSWORD")
    security_token = os.getenv("SF0__SECURITY_TOKEN")
    domain = os.getenv("SF0__INSTANCE_URL")  # Default to 'login' if not set
    return Salesforce(
        username=username,
        password=password,
        security_token=security_token,
        instance_url=domain,
    )


def delete_bulk_contacts(sf):
    query = "SELECT Contact.Id FROM Contact WHERE Contact.AccountId = null"
    result = sf.bulk.Contact.query_all(query)
    record_ids = [{"Id": record["Id"]} for record in result]
    sf.bulk.Contact.hard_delete(record_ids, batch_size=5000)
    print(f"Records deleted: {len(record_ids)}")


def upload_bulk_contacts(sf, contacts_list):
    sf.bulk.Contact.insert(contacts_list, batch_size=5000)


def upload_csv(sf, data_file):
    try:
        df = pd.read_csv(data_file)
        print(f"Record loaded from file: {len(df)}")

        field_mapping = {
            "first": "FirstName",
            "last": "LastName",
            "email": "Email",
            "phone": "Phone",
        }

        contact_list = []
        for _, row in df.iterrows():
            contact = {}
            for csv_field, sf_field in field_mapping.items():
                if csv_field in row:
                    contact[sf_field] = row[csv_field]
            contact_list.append(contact)

        upload_bulk_contacts(sf, contact_list)

        print(f"Records uploaded to salesforce: {len(contact_list)}")

    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")


if __name__ == "__main__":
    sf = sf_connection()
    delete_bulk_contacts(sf)
    upload_csv(sf, "data.csv")
    print("Data seeded in Salesforce successfully.")
