# contact_process.py

import pandas as pd
import re
import os
import phonenumbers
import json
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime
import uuid

load_dotenv()
class ContactConverter:
    def __init__(self, csv_file: str, json_file: str):
        self.csv_file = csv_file
        self.json_file = json_file

        # Creating Supabase Client
        self.supabase_url: str = os.getenv('SUPABASE_URL')
        self.supabase_key: str = os.getenv('SERVICE_ROLE_KEY')
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)

    # Format Phone Numbers to be XXXYYYZZZZ format
    def format_phone_number(self, phone):
        if pd.isna(phone):
            return None
        phone = str(phone).strip()  # Convert to string and strip whitespace
        phone = re.sub(r'\D', '', phone)
        if len(phone) > 10:
            phone = f'{phone[-10:]}'
        elif len(phone) == 10:
            phone = f'{phone}'
        return phone

    # Validate Phone Number
    def validate_phone_number(self, phone):
        if pd.isna(phone):  # Check if the value is NaN
            return None
        try:
            parsed_number = phonenumbers.parse(phone, 'US')
            return phonenumbers.is_valid_number(parsed_number)
        except phonenumbers.phonenumberutil.NumberParseException:
            return False

    # Format Date
    def format_date(self, date):
        date_column = ['Date Last Updated (Details)']
        for col in date_column:
            # Convert date to 'YYYY-MM-DD' format
            date[col] = date[col].apply(lambda x: datetime.strptime(x, '%b %d %Y').strftime('%Y-%m-%d')
                                        if pd.notna(x) else None)

    # Format Company Name
    def format_no_company(self, company):
        if pd.isna(company) or company == '[No Name]':
            company = None # Convert '[No Name]' to None 
        return company
    
    # Get Company id from companies table
    def get_company_id(self, company):
        # Ensure company is not null and is properly formatted
        if company and company.strip():
            company = company.strip()  # Remove leading/trailing spaces
            try:
                # Use ilike for case-insensitive substring matching instead of text_search
                search = self.supabase.table('companies').select('uid').ilike('company', f'%{company}%').execute()

                # Check if any matching company was found
                if search.data:
                    return search.data[0]['uid']

                # If no matching company, insert a new entry
                new_company_data = {
                    'uid': str(uuid.uuid4()),  # Generate a new UUID for the company
                    'company': company,
                    'website': None,
                    'linkedin': None,
                    'domain': None,
                    'industry': None,
                    'location': None
                }
                insert_response = self.supabase.table('companies').insert(new_company_data).execute()
                print(f"Inserted new company '{company}' with uid: {insert_response.data[0]['uid']}")
                return insert_response.data[0]['uid']

            except Exception as e:
                print(f"Error processing company '{company}': {e}")
                return None  # Handle exceptions without halting execution
            
        return None  # Return None if company is empty or invalid
            
                
    # Clean name fields by removing leading or trailing apostrophes
    def clean_name(self, name):
        if pd.isna(name):
            return None
        name = str(name).strip()
        if name.startswith("'"):
            name = name[1:]
        if name.startswith("*"):
            name = name[1:]
        if name.startswith("_"):
            name = name[1:]
        if name.startswith("-"):
            name = name[1:]
        if name.endswith("'"):
            name = name[:-1]
        return name
    
    # Apply filters to data and count records before and after, excluding bot emails
    def apply_filters(self, data):
        original_count = len(data)
        
        # Define filters as a list of tuples (column, condition)
        filters = [
            ('Email Address Type', 'Non-Person', '!='),
            ('Email Includes Unsubscribe', "False", '!='),
            ('Email Domain Type', 'Automated', '!='),
            ('Interaction Status', '', '!='),
            # ('Total Emails', 0, '>'),                  
        ]

        # Apply each filter in the list
        for column, value, condition in filters:
            if condition == '!=':
                data = data[data[column] != value]
            elif condition == '>':
                data = data[data[column] > value]


        # Exclude bot emails from the relevant column (assuming it's 'Email Address')
        bot_keywords = ['reply', 'noreply', 'invoices', 'support', 'do-not-reply', 'donotreply']
        data = data[~data['Email Address'].str.lower().str.contains('|'.join(bot_keywords), na=False)]

        filtered_count = len(data)
        print(f"Filtered data from {original_count} to {filtered_count} records")
        return data

    # Process the CSV and convert phone numbers
    def process_csv(self):
        try:
            data = pd.read_csv(self.csv_file, low_memory=False)
        except FileNotFoundError:
            print(f"Error: {self.csv_file} not found")
            return False

        # Apply filters to data
        data = self.apply_filters(data)
        
        # Format phone numbers
        phone_columns = ['Home Phone', 'Office Phone', 'Direct Phone', 'Mobile Phone', 'Fax Phone']
        for col in phone_columns:
            data[col] = data[col].apply(self.format_phone_number)
            data[col] = data[col].apply(lambda x: x if self.validate_phone_number(x) else None)

        # Remove entries with '[No Name]' in Company Name
        data['Company Name'] = data['Company Name'].apply(self.format_no_company)     

        # Apply the clean_name function to name columns
        name_columns = ['First Name', 'Middle Name', 'Last Name', 'Full Name']
        for col in name_columns:
            data[col] = data[col].apply(self.clean_name)

        # Rename columns
        data.rename(columns={
            "Name Prefix": "prefix",
            "First Name": "first_name",
            "Middle Name": "middle_name",
            "Last Name": "last_name",
            "Name Suffix": "suffix",
            "Full Name": "full_name",
            "Company Name": "company",            
            "Job Title": "title",
            "Email Address": "email",
            "Full Address": "address",
            "Home Phone": "home_phone",
            "Office Phone": "office_phone",
            "Direct Phone": "direct_phone",
            "Mobile Phone": "mobile_phone",
            "Fax Phone": "fax",
            "SigParser Contact ID": "uid",
            "Interaction Status": "interaction_status",
            "Latest Interaction": "latest_interaction",
            "Contact Status": "contact_status",
            "Date Last Updated (Details)": "last_updated",
            "Total Emails" : "total_emails",
            "Email Validation": "email_validation",
        }, inplace=True)      

        filtered_columns = ['uid', 'prefix', 'first_name', 'middle_name', 'last_name', 'suffix', 'full_name', 'title', 'company', 'email', 'address', 
                            'home_phone', 'office_phone', 'direct_phone', 'mobile_phone', 'contact_status', 'interaction_status', 'latest_interaction', 'fax', 'last_updated']
        filtered_data = data[filtered_columns]
        filtered_data = filtered_data.fillna('')

        return filtered_data

    # Save to JSON
    def save_to_json(self, data):
        try:
            if os.path.exists(self.json_file):
                previous_data = pd.read_json(self.json_file)
                if not data.equals(previous_data):
                    data.to_json(self.json_file, orient='records')
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

        new_records = []
        updates = []
        
        for record in json_data:
            uid = record.get('uid')
            sigparser_check = record.get('allow')
            
            if sigparser_check:  # Only update if sigparser_check is True
                contact_exists = (
                    self.supabase.table('contacts').select('uid').eq('uid', uid).execute().data or
                    self.supabase.table('deleted_contacts').select('uid').eq('uid', uid).execute().data
                )
                
                if contact_exists:
                    updates.append(record)
                else:
                    new_records.append(record)


        # Batch update and insert
        batch_size = 500
        for i in range(0, len(updates), batch_size):
            batch_data = updates[i:i + batch_size]
            try:
                self.supabase.table('stock_contacts').upsert(batch_data).execute()
                print(f"Updated {len(batch_data)} records")
            except Exception as e:
                print(f"Error: Unable to update records. {e}")
                return False

        for i in range(0, len(new_records), batch_size):
            batch_data = new_records[i:i + batch_size]
            try:
                self.supabase.table('stock_contacts').insert(batch_data).execute()
                print(f"Inserted {len(batch_data)} records")
            except Exception as e:
                print(f"Error: Unable to insert records. {e}")
                return False

        print(f"Uploaded {len(json_data)} records to the database\nProcess Completed Successfully")
        os.remove(self.json_file)   
    # Main function to run the conversion and upload
    def run(self):
        data = self.process_csv()
        if data is not None:
            if self.save_to_json(data):
                self.upload_to_supabase()
                pass


# Example usage in a desktop app
if __name__ == "__main__":
    contact_converter = ContactConverter('SigParser.csv', 'StockContacts.json')
    contact_converter.run()
