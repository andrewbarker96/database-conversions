# main.py

from tkinter import *
import customtkinter as ctk
from tkinter import filedialog
from contact_process import ContactConverter
from company_process import CompanyConverter
import threading
import time


# Set Theme
ctk.set_appearance_mode("light")
ctk.set_widget_scaling(1.25)
ctk.set_window_scaling(1.5)


class Popover(ctk.CTkToplevel):
    def __init__(self, master, **kwags):
        super().__init__(master, **kwags)
        self.title("Process Status")
        self.geometry("300x100")
        self.progressbar = ctk.CTkProgressBar(self, width=250, height=10, corner_radius=5, mode='determinate')
        self.progressbar.pack(padx=10, pady=10)
        
        self.label = ctk.CTkLabel(self, font=("Aptos", 14))
        self.label.pack(padx=10, pady=10)
        
        self.button = ctk.CTkButton(self, text="Close", command=self.on_close)
        self.button.pack(padx=10, pady=10)
        self.button.configure(state='disabled')
        self.button._fg_color = 'white'
    
    def message(self, message):
        self.progressbar.stop()
        self.label.configure(text=message)
        self.transient(self.master)
        self.button.configure(state='normal')
        self.button._fg_color = '#007bff'
        self.grab_set()
        self.wait_window()
        
    def loadStart(self, message):
        self.label.configure(text=message)
        self.progressbar.start()

    def updateProgress(self, value, message):
        self.progressbar.set(value, message)
        
    def on_close(self):
        self.destroy()


class Header(ctk.CTkLabel):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        
        # Header of Application
        self.header = ctk.CTkLabel(self, text="Stock & Associates\nSigparser to Supabase Conversion Application", font=("Aptos", 18))
        self.header.grid(row=0, column=0, padx=10, pady=10, sticky="ew")
                
        self.text = ctk.CTkLabel(self, wraplength=500, text="Welcome to the Stock & Associates Contact Management Application.\nPlease upload the SigParser file to update the Database.", font=("Aptos", 14))
        self.text.grid(row=1, column=0, padx=10, pady=10, sticky="ew")


class Body(ctk.CTkLabel):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        # Frame Configuration
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)
        
        # File upload button
        self.upload_button = ctk.CTkButton(self, text="Upload File", height=30, command=self.run_program)
        self.upload_button.grid(row=0, column=0, padx=10, pady=5, sticky="ew")
                
        # Exit button
        self.exit_button = ctk.CTkButton(self, text_color='white', text="Exit", fg_color='#b22222', hover_color='darkgray', height=30, command=self.on_close)
        self.exit_button.grid(row=1, column=0, padx=10, pady=5, sticky="ew")
    
    def run_program(self):
        file_path = filedialog.askopenfilename()
        if file_path: 
            self.popover = Popover(master=self)
            self.popover.loadStart('Processing Data...')
            thread = threading.Thread(target=self.process_file, args=(file_path,))
            thread.start()
        
    def process_file(self, file_path):
        if 'Contacts' in file_path:
            try:
                print('Processing Contact Data...')
                contact_converter = ContactConverter(csv_file=file_path, json_file='StockContacts.json')
                contact_converter.run()  # Run the conversion process
                message = 'Contacts updated processed successfully'
                print(message)
                self.after(1000, self.popover.message(message)) 
            except Exception as e:
                message = f'Error: {e}'
                print(message)
                return message
        if 'Companies' in file_path:
            try:
                company_converter = CompanyConverter(csv_file=file_path, json_file='StockCompanies.json')
                company_converter.run()  # Run the conversion process
                message = 'Companies updated processed successfully'
                print(message)
                self.after(1000, self.popover.message(message)) 
            except Exception as e:
                message = f'Error: {e}'
                print(message)
                return message            
            
    
    def on_close(self):
        self.quit()
        self.destroy()


class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        # Configure Window
        self.title("Stock & Associates - Contact Management Application")
        self.geometry("450x250")
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure((0, 1), weight=1)
    
        self.text_frame = Header(master=self)
        self.text_frame.grid(padx=20, pady=20, sticky="ew")
        
        self.button_frame = Body(master=self)
        self.button_frame.grid(padx=20, pady=20, sticky="ew")
        



if __name__ == "__main__":
    app = App()
    app.mainloop()