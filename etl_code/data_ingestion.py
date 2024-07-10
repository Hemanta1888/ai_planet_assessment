import csv
from db_connection import connect_to_db



# CSV file path
csv_file_path = 'AB_NYC_2019.csv'

# Table creation SQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS airbnb_listings (
    id SERIAL PRIMARY KEY,
    name TEXT,
    host_id INTEGER,
    host_name TEXT,
    neighbourhood_group TEXT,
    neighbourhood TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    room_type TEXT,
    price INTEGER,
    minimum_nights INTEGER,
    number_of_reviews INTEGER,
    last_review DATE,
    reviews_per_month DOUBLE PRECISION,
    calculated_host_listings_count INTEGER,
    availability_365 INTEGER
);
"""

# Function to create the table
def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()

# Function to load data from CSV to PostgreSQL
def load_data(conn, csv_file_path):
    with open(csv_file_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row
        with conn.cursor() as cursor:
            for row in reader:
                # Replace empty strings with None for last_review and reviews_per_month
                row = [None if field == '' else field for field in row]
                
                cursor.execute(
                    """
                    INSERT INTO airbnb_listings (
                        id, name, host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews, last_review, reviews_per_month, calculated_host_listings_count, availability_365
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, row
                )
    conn.commit()

# Main function
def main():
    try:
        # Connect to the PostgreSQL database
        conn = connect_to_db()
        # Create the table
        create_table(conn)
        
        # Load the data
        load_data(conn, csv_file_path)
        
        print("Data loaded successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
