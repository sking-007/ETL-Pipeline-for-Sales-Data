import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from config.config import DB_CONFIG  

# Function to extract data from PostgreSQL
def extract_from_postgres(table_name, output_csv):
    try:
        # Create connection
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        query = f"SELECT * FROM {table_name}"
        
        # Read into DataFrame
        df = pd.read_sql(query, engine)
        
        # Save as CSV
        df.to_csv(output_csv, index=False)
        print(f" Data extracted and saved to {output_csv}")
        
        return df
    except Exception as e:
        print(f" Error extracting data: {e}")
        return None

if __name__ == "__main__":
    # Define table and output path
    table_name = "sales_data"
    output_csv = "data/raw_sales_data.csv"
    
    # Run extraction
    extract_from_postgres(table_name, output_csv)
