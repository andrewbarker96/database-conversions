import csv
from docx import Document
import os
import pandas as pd
import json
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
class ProjectListConverter:
    def __init__(self, csv_file: str, json_file: str, docx_file: str):
        self.csv_file = csv_file
        self.json_file = json_file
        self.docx_file = Document(docx_file)
        
        self.supabase_url: str = os.getenv('SUPABASE_URL')
        self.supabase_key: str = os.getenv('SERVICE_ROLE_KEY')
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        
    def process_docx(self):
        extracted_data = []
        processed_data = []

        for table in self.docx_file.tables:
            for row in table.rows:
                row_data = [cell.text.replace("\n", " ").strip() for cell in row.cells]

                unique_data = list(dict.fromkeys(row_data))  # Remove duplicates
                for i in range(0, len(unique_data), 2):
                    extracted_data.append(unique_data[i:i + 2])

        for row in extracted_data:
            if len(row) >= 2:
                job_no, description = row[0], row[1]
                confidential = False  # Default to False
                notes = ""
        
                # Check if "confidential" exists in the description (case-insensitive)
                if "confidential" in description.casefold():
                    confidential = True
                elif "confidential" in job_no.casefold():
                    confidential = True
    
                # Check for additional items besides job_no
                if " " in job_no:
                    job_no, notes = job_no.split(" ", 1)

                processed_data.append({"job_no": job_no, "description": description, "notes": notes, "confidential": confidential})

        return processed_data

    def process_csv(self, data):
        df = pd.DataFrame(data)
        df.to_csv(self.csv_file, index=False)
        print(f"Processed CSV saved to {self.csv_file}")
        return df

    def save_to_json(self, df):
        df.to_json(self.json_file, orient="records", indent=2)
        print(f"Data saved to JSON file: {self.json_file}")

    def upload_to_supabase(self):
        batch_size = 500
        
        try:
            with open(self.json_file, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
            print(f'Successfully loaded {len(json_data)} projects from {self.json_file}')
        except FileNotFoundError:
            print(f'Error: {self.json_file} not found')
            return False
        except json.JSONDecodeError as e:
            print(f'Error... Unable to load records from {self.json_file}: {e}')
            
        job_nos = [record.get('job_no') for record in json_data if record.get('job_no')]
        
        projects = set()
        
        for i in range(0, len(job_nos), batch_size):
            batch_projects = job_nos[i:i + batch_size]
            
            projects_result = (
                self.supabase.table('project_list')
                .select('job_no')
                .in_('job_no', batch_projects)
                .execute()
                .data
            )
            if projects_result:
                projects.update(project['job_no']for project in projects_result)
                
        new_records = []
        updates = [] 
        
        for record in json_data:
            job_no = record.get('job_no')
            if job_no in projects:
                updates.append(record)
            else:
                new_records.append(record)
            
        print(f'New Projects Found: {len(new_records)}')
        print(f'Potential Updates to Projects: {len(updates)}')
        
        if len(new_records) > 0:
            for i in range(0, len(new_records), batch_size):
                batch_data = new_records[i:i + batch_size]
                try:
                    self.supabase.table('project_list').upsert(batch_data).execute()
                    print(f"Inserted {len(batch_data)} records")
                except Exception as e:
                    print(f"Error: Unable to insert records. {e}")
                    print(f"Batch Data: {batch_data}")
                    return False
        else:
            print('No new records to add')
    
        print(f"Uploaded {len(json_data)} records to the database\nProcess Completed Successfully")
        os.remove(self.json_file)   
        os.remove(self.csv_file)

    def run(self):
        # Step 1: Process DOCX
        processed_data = self.process_docx()
        if not processed_data:
            print("Error: No data extracted from DOCX file.")
            return

        # Step 2: Save to CSV
        df = self.process_csv(processed_data)

        # Step 3: Save to JSON
        self.save_to_json(df)

        # Step 4: Upload to Supabase
        self.upload_to_supabase()


# Example usage
if __name__ == "__main__":
    converter = ProjectListConverter("projectList.csv", "projectList.json", "projectList.docx")
    converter.run()
