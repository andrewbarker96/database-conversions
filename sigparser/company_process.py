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
        self.supabase_key: str = os.getenv('SERVICE_ROLE_KEY')
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)

    def format_no_company(self, company):
        if pd.isna(company) or company == '[No Name]':
            return None  # Exclude companies with '[No Name]'
        return company
    
    def apply_filters(self, data):
        original_count = len(data)
        
        # Define filters as a list of tuples (column, condition)
        filters = [
            ('Total Emails', 1, '>'),         
            ('Company Contacts', 1, '>')         
        ]

        # Apply each filter in the list
        for column, value, condition in filters:
            if condition == '!=':
                data = data[data[column] != value]
            elif condition == '>':
                data = data[data[column] > value]


        # Exclude bot emails from the relevant column (assuming it's 'Email Address')
        bot_keywords = ['reply', 'noreply', 'invoices', 'support', 'do-not-reply', 'donotreply', 'bids', 'billing', 'vendor']
        data = data[~data['Company Name'].str.lower().str.contains('|'.join(bot_keywords), na=False)]

        filtered_count = len(data)
        print(f"Filtered data from {original_count} to {filtered_count} records")
        return data

    # Process the CSV and apply necessary transformations
    def process_csv(self):
        try:
            data = pd.read_csv(self.csv_file, low_memory=False)
            print(f"Loaded {len(data)} records from {self.csv_file}")
        except FileNotFoundError:
            print(f"Error: {self.csv_file} not found")
            return None

        # Rename columns to match the new schema
        data.rename(columns={
            "SigParser Company ID": "uid",
            "Company Name": "company",
            "Company Website": "website",
            "Company LinkedIn": "linkedin",
            "Company Industry": "industry",
            "Email Domain": "domain",
            "Company Location": "location",  # Ensure this matches the actual column name
            "Interaction Status": "interaction_status",
            "Latest Interaction": "latest_interaction"
        }, inplace=True)

        # Ensure critical columns exist, adding them if missing
        for col in ['location', 'address']:
            if col not in data.columns:
                data[col] = None  # Add an empty column if missing

        # Filter to include only relevant columns
        filtered_columns = ['uid', 'company', 'website', 'linkedin', 'domain', 'industry', 'location', 'latest_interaction']
        filtered_data = data[filtered_columns]
        filtered_data = filtered_data.fillna('')

        print(filtered_data)
        return filtered_data

    def get_address_from_companies(self, company_name):
        # Fetch address data for companies matching the company name
        response = self.supabase.table('stock_companies').select('address').eq('company', company_name).execute()
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
        # Batch size for querying/updating data
        batch_size = 500
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
        
        uids = [record.get('uid') for record in json_data if record.get('uid')]
        
        companies_uids = set()
        deleted_companies_uids = set()

        for i in range(0, len(uids), batch_size):
            batch_uids = uids[i:i + batch_size]
            
            # Query companies table for batch
            companies_result = (
                self.supabase.table('companies')
                .select('uid')
                .in_('uid', batch_uids)
                .eq('allow_sigparser', True)
                .execute()
                .data
            )
            if companies_result:
                companies_uids.update(contact['uid'] for contact in companies_result)

            # Query deleted_companies table for batch
            deleted_companies_result = (
                self.supabase.table('deleted_companies')
                .select('uid')
                .in_('uid', batch_uids)
                .eq('allow_sigparser', True)
                .execute()
                .data
            )
            if deleted_companies_result:
                deleted_companies_uids.update(contact['uid'] for contact in deleted_companies_result)

        # Separate new records and updates
        new_records = []
        updates = []
        deleted_updates = []

        for record in json_data:
            uid = record.get('uid')
            if uid in companies_uids:
                updates.append(record)
            elif uid in deleted_companies_uids:
                deleted_updates.append(record)
            else:
                new_records.append(record)

        # Print the results
        print(f'New companies Found: {len(new_records)}')
        print(f'Potential updates in companies: {len(updates)}')
        print(f'Potential updates in deleted_companies: {len(deleted_updates)}')

        # for i in range(0, len(updates), batch_size):
        #     batch_data = updates[i:i + batch_size]
        #     try:
        #         self.supabase.table('companies').upsert(batch_data).execute()
        #     except Exception as e:
        #         print(f"Error: Unable to update new records. {e}")
        #         print(f"Data Example: {batch_data[-1]}") 
        #         return False

        # for i in range(0, len(deleted_updates), batch_size):
        #     batch_data = deleted_updates[i:i + batch_size]
        #     try:
        #         self.supabase.table('deleted_companies').upsert(batch_data).execute()
        #     except Exception as e:
        #         print(f"Error: Unable to update new records. {e}")
        #         print(f"Data Example: {batch_data[-1]}") 
        #         return False
            
        if len(new_records) > 0:
            for i in range(0, len(new_records), batch_size):
                batch_data = new_records[i:i + batch_size]
                try:
                    self.supabase.table('companies').upsert(batch_data).execute()
                except Exception as e:
                    print(f"Error: Unable to insert new records. {e}")
                    print(f"Data Exmple: {batch_data[-1]}")
                    return False
        else:
            print('No new records to add')

        print(f"Uploaded {len(json_data)} records to the database\nProcess Completed Successfully")
        os.remove(self.json_file)   
        
        
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
