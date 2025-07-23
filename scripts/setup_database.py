import duckdb
import os
from pathlib import Path

def setup_database():
    """Initialize DuckDB with required schema"""
    # Use the path that will work inside Docker container
    db_path = "/app/data/ecommerce.duckdb"
    
    # Ensure data directory exists
    Path("/app/data").mkdir(parents=True, exist_ok=True)
    
    print(f"Initializing database at: {db_path}")
    
    try:
        conn = duckdb.connect(db_path)
        
        # Products table - main data store
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_products (
                id VARCHAR PRIMARY KEY,
                source VARCHAR NOT NULL,
                title VARCHAR NOT NULL, 
                price DECIMAL(10,2),
                rating DECIMAL(3,2),
                review_count INTEGER,
                category VARCHAR,
                url VARCHAR,
                scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print("✅ Database schema created successfully!")
        conn.close()
        
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        raise

if __name__ == "__main__":
    setup_database()