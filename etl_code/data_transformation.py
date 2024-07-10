import psycopg2
import os
from db_connection import connect_to_db

# Function to normalize last_review into date and time columns and insert into a new table
def normalize_last_review(conn):
    try:
        with conn.cursor() as cursor:
            # Create new table airbnb_listings_normalized if not exists
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS airbnb_listings_normalized (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    host_id INTEGER,
                    host_name TEXT,
                    neighbourhood_group TEXT,
                    neighbourhood TEXT,
                    latitude NUMERIC,
                    longitude NUMERIC,
                    room_type TEXT,
                    price NUMERIC,
                    minimum_nights INTEGER,
                    number_of_reviews INTEGER,
                    last_review_date DATE,
                    last_review_time TIME,
                    reviews_per_month NUMERIC,
                    calculated_host_listings_count INTEGER,
                    availability_365 INTEGER
                )
                """
            )
            conn.commit()

            # Insert normalized data into airbnb_listings_normalized
            cursor.execute(
                """
                INSERT INTO airbnb_listings_normalized (
                    id, name, host_id, host_name, neighbourhood_group, neighbourhood,
                    latitude, longitude, room_type, price, minimum_nights,
                    number_of_reviews, last_review_date, last_review_time,
                    reviews_per_month, calculated_host_listings_count, availability_365
                )
                SELECT
                    id, name, host_id, host_name, neighbourhood_group, neighbourhood,
                    latitude, longitude, room_type, price, minimum_nights,
                    number_of_reviews, DATE(last_review) AS last_review_date,
                    last_review::timestamp::time AS last_review_time, COALESCE(reviews_per_month, 0.0) AS reviews_per_month,
                    calculated_host_listings_count, availability_365
                FROM airbnb_listings
                """
            )
            conn.commit()
        
        print("Normalization completed and inserted into new table 'airbnb_listings_normalized'!")
    
    except psycopg2.Error as e:
        print(f"Error normalizing data: {e}")

# Function to calculate average price per neighbourhood and store in a new table
def calculate_average_price_per_neighbourhood(conn):
    try:
        with conn.cursor() as cursor:
            # Create neighbourhood_avg_price table if not exists
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS neighbourhood_avg_price (
                    neighbourhood TEXT PRIMARY KEY,
                    avg_price NUMERIC
                )
                """
            )
            conn.commit()

            # Calculate average price per neighbourhood and insert/update in neighbourhood_avg_price
            cursor.execute(
                """
                INSERT INTO neighbourhood_avg_price (neighbourhood, avg_price)
                SELECT neighbourhood, ROUND(AVG(price), 3) AS avg_price
                FROM airbnb_listings
                GROUP BY neighbourhood
                ON CONFLICT (neighbourhood) DO UPDATE
                SET avg_price = EXCLUDED.avg_price
                """
            )
            conn.commit()

            print("Average price per neighbourhood calculated and updated!")
    
    except psycopg2.Error as e:
        print(f"Error calculating average price per neighbourhood: {e}")

# Function to handle missing values
def handle_missing_values(conn):
    try:
        with conn.cursor() as cursor:
            # Handle missing values in reviews_per_month
            cursor.execute(
                """
                UPDATE airbnb_listings
                SET reviews_per_month = COALESCE(reviews_per_month, 0.0)
                WHERE reviews_per_month IS NULL
                """
            )
            conn.commit()
        
        print("Missing values handled!")
    
    except psycopg2.Error as e:
        print(f"Error handling missing values: {e}")

# Main function
def main():
    try:
        # Connect to PostgreSQL
        conn = connect_to_db()
        if conn is None:
            return
        
        # Perform data transformations into a new table
        normalize_last_review(conn)
        calculate_average_price_per_neighbourhood(conn)
        handle_missing_values(conn)
        
        print("Data transformation completed!")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
