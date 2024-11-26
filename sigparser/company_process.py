# company_process.py

import pandas as pd
import os
import json
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

class CompanyConverter:
    def __init__(self, csv_file: str, json_file: str):
        self.csv_file = csv_file
        self.json_file = json_file

        # Creating Supabase Client
        self.supabase_url: str = os.getenv('SUPABASE_URL')
        self.supabase_key: str = os.getenv('SUPABASE_KEY')
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)

    def format_no_company(self, company):
        if pd.isna(company) or company == '[No Name]':
            return None  # Exclude companies with '[No Name]'
        return company

    # Process the CSV and apply necessary transformations
    def process_csv(self):
        try:
            data = pd.read_csv(self.csv_file, low_memory=False)
            print(f"Loaded {len(data)} records from {self.csv_file}")
        except FileNotFoundError:
            print(f"Error: {self.csv_file} not found")
            return None

        # Remove entries with '[No Name]' in Company Name
        data['Company Name'] = data['Company Name'].apply(self.format_no_company)
        data = data.dropna(subset=['Company Name'])  # Drop rows with None in Company Name

        # Rename columns to match the new schema
        data.rename(columns={
            "SigParser Company ID": "uid",
            "Company Name": "company",
            "Company Website": "website",
            "Company LinkedIn": "linkedin",
            "Company Industry": "industry",
            "Email Domain": "domain",
            "Company Location": "location"
        }, inplace=True)

        # Set JSON fields for location and address
        data['address'] = None  # Placeholder for address field

        # Filter to include only relevant columns
        filtered_columns = ['uid', 'company', 'website', 'linkedin', 'domain', 'industry', 'location', 'address']
        filtered_data = data[filtered_columns]
        filtered_data = filtered_data.fillna('')

        # Fetch addresses from stock_contacts table where company name matches
        #* filtered_data['address'] = data['name'].apply(self.get_address_from_contacts)

        print(filtered_data)
        return filtered_data

    def get_address_from_contacts(self, company_name):
        # Fetch address data for contacts matching the company name
        response = self.supabase.table('stock_contacts').select('address').eq('company', company_name).execute()
        addresses = [item['address'] for item in response.data if item['address']]
        return addresses if addresses else None

    # Save data to JSON
    def save_to_json(self, data):
        try:
            if os.path.exists(self.json_file):
                previous_data = pd.read_json(self.json_file)
                if not data.equals(previous_data):
                    data.to_json(self.json_file, orient='records', indent=2)
                    print(f"{self.json_file} updated successfully")
                else:
                    print(f"No changes detected. {self.json_file} not updated")
            else:
                print(f"Creating {self.json_file}")
                data.to_json(self.json_file, orient='records', indent=2)
        except Exception as e:
            print(f"Error: Unable to write to {self.json_file}. {e}")
            return False
        return True

    # Upload data to Supabase
    def upload_to_supabase(self):
        try:
            with open(self.json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            print(f'Successfully loaded {len(json_data)} records from {self.json_file}')
        except FileNotFoundError:
            print(f"Error: {self.json_file} not found")
            return False
        except json.JSONDecodeError as e:
            print(f"Error: Unable to load records from {self.json_file}. {e}")
            return False
        
        new_companies = []
        updates = []

        # Upsert company data to avoid duplicates and ensure unique entries
        for company in json_data:
            uid = company.get('uid')
            
            if uid:
                updates.append(company)
            else:
                new_companies.append(company)
        
        # Batch upload companies to Supabase
        batch_size = 500
        for i in range(0, len(updates), batch_size):
            batch_data = json_data[i:i + batch_size]
            try:
                self.supabase.table('companies').upsert(batch_data).execute()
                print(f"Uploaded {len(batch_data)} records")
            except Exception as e:
                print(f"Error: Unable to upload records. {e}")
                return False
            
        for i in range(0, len(new_companies), batch_size):
            batch_data = new_companies[i:i + batch_size]
            try:
                self.supabase.table('companies').insert(batch_data).execute()
                print(f"Inserted {len(batch_data)} records")
            except Exception as e:
                print(f"Error: Unable to insert records. {e}")
                return False

        print(f"Uploaded {len(json_data)} records to the database\nProcess Completed Successfully")
        os.remove(self.json_file)
        return True

    # Main function to run the conversion and upload
    def run(self):
        data = self.process_csv()
        if data is not None:
            if self.save_to_json(data):
                print('Saving to JSON')
                self.upload_to_supabase()
                print('Uploading to Supabase')

# Example usage in a desktop app
if __name__ == "__main__":    
    company_converter = CompanyConverter('SigParser.csv', 'StockCompanies.json')
    company_converter.run()
