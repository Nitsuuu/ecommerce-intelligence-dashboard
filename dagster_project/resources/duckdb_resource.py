"""
DuckDB Resource Configuration for E-commerce Intelligence Dashboard
Database connection and initialization for the data pipeline
"""
import os
from pathlib import Path
from dagster import ConfigurableResource
from dagster_duckdb import DuckDBResource
import duckdb
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class EcommerceDuckDBResource(ConfigurableResource):
    """
    Custom DuckDB resource for e-commerce intelligence dashboard
    
    This resource handles database initialization, schema creation,
    and provides connection management for the data pipeline.
    """
    
    database_path: str = "/app/data/ecommerce_intelligence.duckdb"
    schema_name: str = "ecommerce"
    
    def setup_for_execution(self, context) -> None:
        """Initialize database and create required schemas/tables"""
        self._ensure_database_directory()
        self._initialize_database()
        
    def _ensure_database_directory(self) -> None:
        """Ensure the database directory exists"""
        db_dir = Path(self.database_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Database directory ensured: {db_dir}")
    
    def _initialize_database(self) -> None:
        """Initialize database with required schemas and tables"""
        try:
            with duckdb.connect(self.database_path) as conn:
                # Create schema
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
                conn.execute(f"USE {self.schema_name}")
                
                # Create core tables
                self._create_core_tables(conn)
                
                # Create indexes for performance
                self._create_indexes(conn)
                
                # Create views for common queries
                self._create_views(conn)
                
                logger.info("Database initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _create_core_tables(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create core database tables"""
        
        # Raw products table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_products (
                item_id VARCHAR PRIMARY KEY,
                title VARCHAR NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                currency VARCHAR(3) DEFAULT 'USD',
                condition VARCHAR(50),
                category VARCHAR(100),
                seller VARCHAR(100),
                location VARCHAR(100),
                shipping_cost DECIMAL(8,2),
                listing_type VARCHAR(50),
                end_time TIMESTAMP,
                image_url TEXT,
                description TEXT,
                view_count INTEGER,
                watch_count INTEGER,
                collected_at TIMESTAMP NOT NULL,
                partition_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Validated products table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS validated_products (
                item_id VARCHAR PRIMARY KEY,
                title VARCHAR NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                currency VARCHAR(3) DEFAULT 'USD',
                condition VARCHAR(50),
                category VARCHAR(100),
                seller VARCHAR(100),
                location VARCHAR(100),
                shipping_cost DECIMAL(8,2),
                listing_type VARCHAR(50),
                end_time TIMESTAMP,
                image_url TEXT,
                description TEXT,
                view_count INTEGER,
                watch_count INTEGER,
                collected_at TIMESTAMP NOT NULL,
                partition_date DATE NOT NULL,
                validation_score DECIMAL(5,2),
                validation_timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Raw stock data table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_stock_data (
                id INTEGER PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                company_name VARCHAR(200),
                current_price DECIMAL(12,4) NOT NULL,
                currency VARCHAR(3) DEFAULT 'USD',
                market_cap BIGINT,
                pe_ratio DECIMAL(8,2),
                day_change DECIMAL(12,4),
                day_change_percent DECIMAL(8,4),
                volume BIGINT,
                avg_volume BIGINT,
                fifty_two_week_high DECIMAL(12,4),
                fifty_two_week_low DECIMAL(12,4),
                dividend_yield DECIMAL(6,4),
                beta DECIMAL(6,4),
                sector VARCHAR(100),
                industry VARCHAR(100),
                collected_at TIMESTAMP NOT NULL,
                partition_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, partition_date, collected_at)
            )
        """)
        
        # Validated stock data table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS validated_stock_data (
                id INTEGER PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                company_name VARCHAR(200),
                current_price DECIMAL(12,4) NOT NULL,
                currency VARCHAR(3) DEFAULT 'USD',
                market_cap BIGINT,
                pe_ratio DECIMAL(8,2),
                day_change DECIMAL(12,4),
                day_change_percent DECIMAL(8,4),
                volume BIGINT,
                avg_volume BIGINT,
                fifty_two_week_high DECIMAL(12,4),
                fifty_two_week_low DECIMAL(12,4),
                dividend_yield DECIMAL(6,4),
                beta DECIMAL(6,4),
                sector VARCHAR(100),
                industry VARCHAR(100),
                collected_at TIMESTAMP NOT NULL,
                partition_date DATE NOT NULL,
                validation_score DECIMAL(5,2),
                validation_timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, partition_date, collected_at)
            )
        """)
        
        # Data quality metrics table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                id INTEGER PRIMARY KEY,
                source VARCHAR(50) NOT NULL,
                partition_date DATE NOT NULL,
                total_records INTEGER NOT NULL,
                valid_records INTEGER NOT NULL,
                invalid_records INTEGER NOT NULL,
                quality_score DECIMAL(5,2) NOT NULL,
                critical_issues INTEGER DEFAULT 0,
                error_issues INTEGER DEFAULT 0,
                warning_issues INTEGER DEFAULT 0,
                info_issues INTEGER DEFAULT 0,
                generated_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Market summary table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_summary (
                id INTEGER PRIMARY KEY,
                partition_date DATE NOT NULL,
                total_companies INTEGER,
                avg_day_change_percent DECIMAL(8,4),
                positive_movers INTEGER,
                negative_movers INTEGER,
                highest_gainer_symbol VARCHAR(10),
                highest_gainer_change DECIMAL(8,4),
                biggest_loser_symbol VARCHAR(10),
                biggest_loser_change DECIMAL(8,4),
                total_volume BIGINT,
                collected_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(partition_date)
            )
        """)
        
        # Daily summaries table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_summaries (
                id INTEGER PRIMARY KEY,
                partition_date DATE NOT NULL,
                products_collected INTEGER,
                stocks_collected INTEGER,
                avg_product_price DECIMAL(10,2),
                avg_stock_price DECIMAL(12,4),
                avg_stock_change_percent DECIMAL(8,4),
                positive_stock_movers INTEGER,
                negative_stock_movers INTEGER,
                product_quality_score DECIMAL(5,2),
                stock_quality_score DECIMAL(5,2),
                generated_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(partition_date)
            )
        """)
        
        # Price history table for trend analysis
        conn.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY,
                item_id VARCHAR,
                symbol VARCHAR(10),
                price DECIMAL(12,4) NOT NULL,
                currency VARCHAR(3) DEFAULT 'USD',
                price_type VARCHAR(20), -- 'product' or 'stock'
                collected_at TIMESTAMP NOT NULL,
                partition_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Competitors table for competitive analysis
        conn.execute("""
            CREATE TABLE IF NOT EXISTS competitors (
                id INTEGER PRIMARY KEY,
                company_name VARCHAR(200) NOT NULL,
                symbol VARCHAR(10),
                category VARCHAR(100),
                market_cap_bucket VARCHAR(20), -- 'large', 'medium', 'small'
                is_direct_competitor BOOLEAN DEFAULT FALSE,
                competitor_type VARCHAR(50), -- 'ecommerce', 'tech', 'retail'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(company_name, symbol)
            )
        """)
        
        logger.info("Core tables created successfully")
    
    def _create_indexes(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create database indexes for performance optimization"""
        
        # Product indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_products_partition_date ON raw_products(partition_date)",
            "CREATE INDEX IF NOT EXISTS idx_products_category ON raw_products(category)",
            "CREATE INDEX IF NOT EXISTS idx_products_price ON raw_products(price)",
            "CREATE INDEX IF NOT EXISTS idx_products_collected_at ON raw_products(collected_at)",
            
            "CREATE INDEX IF NOT EXISTS idx_validated_products_partition_date ON validated_products(partition_date)",
            "CREATE INDEX IF NOT EXISTS idx_validated_products_category ON validated_products(category)",
            "CREATE INDEX IF NOT EXISTS idx_validated_products_price ON validated_products(price)",
            
            # Stock indexes
            "CREATE INDEX IF NOT EXISTS idx_stock_symbol ON raw_stock_data(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_stock_partition_date ON raw_stock_data(partition_date)",
            "CREATE INDEX IF NOT EXISTS idx_stock_sector ON raw_stock_data(sector)",
            "CREATE INDEX IF NOT EXISTS idx_stock_collected_at ON raw_stock_data(collected_at)",
            
            "CREATE INDEX IF NOT EXISTS idx_validated_stock_symbol ON validated_stock_data(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_validated_stock_partition_date ON validated_stock_data(partition_date)",
            
            # Quality metrics indexes
            "CREATE INDEX IF NOT EXISTS idx_quality_source_date ON data_quality_metrics(source, partition_date)",
            "CREATE INDEX IF NOT EXISTS idx_quality_score ON data_quality_metrics(quality_score)",
            
            # Price history indexes
            "CREATE INDEX IF NOT EXISTS idx_price_history_item_date ON price_history(item_id, partition_date)",
            "CREATE INDEX IF NOT EXISTS idx_price_history_symbol_date ON price_history(symbol, partition_date)",
            "CREATE INDEX IF NOT EXISTS idx_price_history_type ON price_history(price_type)",
        ]
        
        for index_sql in indexes:
            try:
                conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Failed to create index: {e}")
        
        logger.info("Database indexes created successfully")
    
    def _create_views(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create database views for common analytics queries"""
        
        # Latest products view
        conn.execute("""
            CREATE OR REPLACE VIEW latest_products AS
            SELECT 
                p.*,
                ROW_NUMBER() OVER (PARTITION BY p.item_id ORDER BY p.collected_at DESC) as rn
            FROM validated_products p
            QUALIFY rn = 1
        """)
        
        # Latest stock data view
        conn.execute("""
            CREATE OR REPLACE VIEW latest_stocks AS
            SELECT 
                s.*,
                ROW_NUMBER() OVER (PARTITION BY s.symbol ORDER BY s.collected_at DESC) as rn
            FROM validated_stock_data s
            QUALIFY rn = 1
        """)
        
        # Product category summary view
        conn.execute("""
            CREATE OR REPLACE VIEW product_category_summary AS
            SELECT 
                category,
                COUNT(*) as product_count,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                COUNT(DISTINCT seller) as unique_sellers,
                MAX(collected_at) as last_updated
            FROM latest_products
            WHERE category IS NOT NULL
            GROUP BY category
            ORDER BY product_count DESC
        """)
        
        # Stock sector performance view
        conn.execute("""
            CREATE OR REPLACE VIEW sector_performance AS
            SELECT 
                sector,
                COUNT(*) as company_count,
                AVG(day_change_percent) as avg_change_percent,
                SUM(CASE WHEN day_change_percent > 0 THEN 1 ELSE 0 END) as positive_movers,
                SUM(CASE WHEN day_change_percent < 0 THEN 1 ELSE 0 END) as negative_movers,
                AVG(current_price) as avg_price,
                SUM(market_cap) as total_market_cap,
                MAX(collected_at) as last_updated
            FROM latest_stocks
            WHERE sector IS NOT NULL
            GROUP BY sector
            ORDER BY avg_change_percent DESC
        """)
        
        # Data quality trend view
        conn.execute("""
            CREATE OR REPLACE VIEW quality_trends AS
            SELECT 
                partition_date,
                source,
                quality_score,
                total_records,
                valid_records,
                critical_issues + error_issues as serious_issues,
                warning_issues + info_issues as minor_issues,
                LAG(quality_score) OVER (PARTITION BY source ORDER BY partition_date) as prev_quality_score,
                quality_score - LAG(quality_score) OVER (PARTITION BY source ORDER BY partition_date) as quality_change
            FROM data_quality_metrics
            ORDER BY partition_date DESC, source
        """)
        
        # Price comparison view (for competitive analysis)
        conn.execute("""
            CREATE OR REPLACE VIEW price_comparison AS
            SELECT 
                p1.category,
                p1.title as product1,
                p1.price as price1,
                p1.seller as seller1,
                p2.title as product2,
                p2.price as price2,
                p2.seller as seller2,
                ABS(p1.price - p2.price) as price_diff,
                ROUND(((p1.price - p2.price) / p2.price) * 100, 2) as price_diff_percent
            FROM latest_products p1
            JOIN latest_products p2 ON p1.category = p2.category
            WHERE p1.item_id != p2.item_id
                AND p1.seller != p2.seller
                AND SIMILARITY(p1.title, p2.title) > 0.7  -- Similar products
            ORDER BY p1.category, price_diff_percent DESC
        """)
        
        # Market overview view
        conn.execute("""
            CREATE OR REPLACE VIEW market_overview AS
            SELECT 
                'Products' as data_type,
                COUNT(*) as total_records,
                COUNT(DISTINCT category) as categories,
                AVG(price) as avg_price,
                MAX(collected_at) as last_updated
            FROM latest_products
            
            UNION ALL
            
            SELECT 
                'Stocks' as data_type,
                COUNT(*) as total_records,
                COUNT(DISTINCT sector) as categories,
                AVG(current_price) as avg_price,
                MAX(collected_at) as last_updated
            FROM latest_stocks
        """)
        
        logger.info("Database views created successfully")
    
    def get_connection(self):
        """Get a database connection"""
        return duckdb.connect(self.database_path)
    
    def execute_query(self, query: str, params: Optional[list] = None):
        """Execute a query and return results"""
        with self.get_connection() as conn:
            conn.execute(f"USE {self.schema_name}")
            if params:
                return conn.execute(query, params).fetchall()
            else:
                return conn.execute(query).fetchall()
    
    def get_table_info(self, table_name: str) -> dict:
        """Get information about a table"""
        with self.get_connection() as conn:
            conn.execute(f"USE {self.schema_name}")
            
            # Get table schema
            schema_info = conn.execute(f"DESCRIBE {table_name}").fetchall()
            
            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            # Get latest update
            try:
                latest_update = conn.execute(
                    f"SELECT MAX(created_at) FROM {table_name}"
                ).fetchone()[0]
            except:
                latest_update = None
            
            return {
                'table_name': table_name,
                'schema': schema_info,
                'row_count': row_count,
                'latest_update': latest_update
            }
    
    def get_database_stats(self) -> dict:
        """Get overall database statistics"""
        with self.get_connection() as conn:
            conn.execute(f"USE {self.schema_name}")
            
            # Get all tables
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                [self.schema_name]
            ).fetchall()
            
            stats = {
                'database_path': self.database_path,
                'schema_name': self.schema_name,
                'total_tables': len(tables),
                'tables': {}
            }
            
            # Get stats for each table
            for (table_name,) in tables:
                try:
                    table_info = self.get_table_info(table_name)
                    stats['tables'][table_name] = {
                        'row_count': table_info['row_count'],
                        'latest_update': table_info['latest_update']
                    }
                except Exception as e:
                    logger.warning(f"Failed to get stats for table {table_name}: {e}")
                    stats['tables'][table_name] = {'error': str(e)}
            
            return stats
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """Clean up old data beyond retention period"""
        cutoff_date = f"CURRENT_DATE - INTERVAL '{days_to_keep} days'"
        
        cleanup_queries = [
            f"DELETE FROM raw_products WHERE partition_date < {cutoff_date}",
            f"DELETE FROM raw_stock_data WHERE partition_date < {cutoff_date}",
            f"DELETE FROM data_quality_metrics WHERE partition_date < {cutoff_date}",
            f"DELETE FROM daily_summaries WHERE partition_date < {cutoff_date}",
            f"DELETE FROM price_history WHERE partition_date < {cutoff_date}"
        ]
        
        with self.get_connection() as conn:
            conn.execute(f"USE {self.schema_name}")
            
            total_deleted = 0
            for query in cleanup_queries:
                try:
                    result = conn.execute(query)
                    deleted_rows = result.rowcount if hasattr(result, 'rowcount') else 0
                    total_deleted += deleted_rows
                    logger.info(f"Deleted {deleted_rows} old records from table")
                except Exception as e:
                    logger.error(f"Failed to cleanup table: {e}")
            
            # Vacuum to reclaim space
            conn.execute("VACUUM")
            
            logger.info(f"Cleanup completed. Total records deleted: {total_deleted}")
            return total_deleted

# Factory function to create DuckDB resource
def create_duckdb_resource(database_path: str = None) -> DuckDBResource:
    """
    Create a configured DuckDB resource for the ecommerce intelligence dashboard
    
    Args:
        database_path: Path to the database file, defaults to environment variable or default path
    
    Returns:
        Configured DuckDBResource
    """
    if database_path is None:
        database_path = os.getenv('DUCKDB_PATH', '/app/data/ecommerce_intelligence.duckdb')
    
    # Ensure directory exists
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    
    return DuckDBResource(database=database_path)

# Configuration for different environments
def get_duckdb_config(environment: str = 'development') -> dict:
    """
    Get DuckDB configuration for different environments
    
    Args:
        environment: 'development', 'staging', or 'production'
    
    Returns:
        Configuration dictionary
    """
    base_config = {
        'schema_name': 'ecommerce',
        'retention_days': 30
    }
    
    if environment == 'development':
        base_config.update({
            'database_path': '/app/data/dev_ecommerce_intelligence.duckdb',
            'log_level': 'DEBUG'
        })
    elif environment == 'staging':
        base_config.update({
            'database_path': '/app/data/staging_ecommerce_intelligence.duckdb',
            'log_level': 'INFO'
        })
    elif environment == 'production':
        base_config.update({
            'database_path': '/app/data/prod_ecommerce_intelligence.duckdb',
            'log_level': 'WARNING',
            'retention_days': 90
        })
    
    return base_config

# Usage example and testing
if __name__ == "__main__":
    # Test the DuckDB resource
    resource = EcommerceDuckDBResource(
        database_path="/tmp/test_ecommerce.duckdb",
        schema_name="test_ecommerce"
    )
    
    try:
        # Initialize the database
        resource._initialize_database()
        
        # Get database stats
        stats = resource.get_database_stats()
        print("Database Statistics:")
        print(f"- Total tables: {stats['total_tables']}")
        print(f"- Tables: {list(stats['tables'].keys())}")
        
        # Test a simple query
        result = resource.execute_query("SELECT 1 as test_value")
        print(f"- Test query result: {result}")
        
        print("DuckDB resource test completed successfully!")
        
    except Exception as e:
        print(f"DuckDB resource test failed: {e}")