"""
Main Dagster Definitions for E-commerce Intelligence Dashboard
Central configuration and orchestration for the data pipeline
"""
import os
from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_duckdb import DuckDBResource

# Import assets and resources
from dagster_project.assets import ingestion
from dagster_project.resources.duckdb_resource import create_duckdb_resource, EcommerceDuckDBResource
from dagster_project.assets.ingestion import (
    raw_ebay_products,
    raw_stock_data,
    validated_products,
    validated_stock_data,
    daily_collection_summary,
    data_quality_sensor,
    eBayConfig,
    DataCollectionConfig
)

# Asset groups for organization
from dagster import AssetGroup

# Load all assets
all_assets = [
    raw_ebay_products,
    raw_stock_data,
    validated_products,
    validated_stock_data,
    daily_collection_summary
]

# Resource configuration
resources = {
    "duckdb": create_duckdb_resource(),
    "ecommerce_duckdb": EcommerceDuckDBResource(
        database_path=EnvVar("DUCKDB_PATH").get_value("/app/data/ecommerce_intelligence.duckdb"),
        schema_name="ecommerce"
    )
}

# Configuration values
config_values = {
    "ebay_config": eBayConfig(
        app_id=EnvVar("EBAY_APP_ID").get_value("your_app_id"),
        cert_id=EnvVar("EBAY_CERT_ID").get_value("your_cert_id"),
        dev_id=EnvVar("EBAY_DEV_ID").get_value("your_dev_id"),
        environment=EnvVar("EBAY_ENVIRONMENT").get_value("sandbox"),
        categories=["Electronics", "Computers", "Cell Phones", "Video Games"],
        products_per_category=100
    ),
    "data_config": DataCollectionConfig(
        max_retries=3,
        batch_size=1000,
        quality_threshold=95.0
    )
}

# Jobs for scheduled execution
from dagster import job, op, DefaultSensorStatus
from dagster import schedule, ScheduleDefinition

@job(
    name="daily_data_collection",
    description="Daily job to collect product and stock data",
    resource_defs=resources,
    config=config_values
)
def daily_data_collection_job():
    """Daily data collection job"""
    products = raw_ebay_products()
    stocks = raw_stock_data()
    validated_prods = validated_products(products)
    validated_stocks = validated_stock_data(stocks)
    daily_collection_summary(validated_prods, validated_stocks)

# Schedule for daily execution
daily_schedule = ScheduleDefinition(
    job=daily_data_collection_job,
    cron_schedule="0 6 * * *",  # Run at 6 AM daily
    name="daily_data_collection_schedule",
    description="Schedule for daily data collection at 6 AM"
)

# Sensors
sensors = [
    data_quality_sensor.configured(
        {
            "minimum_interval_seconds": 300,  # Check every 5 minutes
            "default_status": DefaultSensorStatus.RUNNING
        },
        name="data_quality_monitor"
    )
]

# Asset groups for better organization
ingestion_assets = AssetGroup(
    assets=[raw_ebay_products, raw_stock_data],
    resource_defs=resources
)

validation_assets = AssetGroup(
    assets=[validated_products, validated_stock_data],
    resource_defs=resources
)

summary_assets = AssetGroup(
    assets=[daily_collection_summary],
    resource_defs=resources
)

# Main definitions
defs = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[daily_data_collection_job],
    schedules=[daily_schedule],
    sensors=sensors,
    asset_groups=[ingestion_assets, validation_assets, summary_assets]
)

# Development helper functions
def get_asset_lineage():
    """Get asset lineage for visualization"""
    lineage = {
        "raw_ebay_products": {
            "dependencies": [],
            "dependents": ["validated_products"]
        },
        "raw_stock_data": {
            "dependencies": [],
            "dependents": ["validated_stock_data"]
        },
        "validated_products": {
            "dependencies": ["raw_ebay_products"],
            "dependents": ["daily_collection_summary"]
        },
        "validated_stock_data": {
            "dependencies": ["raw_stock_data"],
            "dependents": ["daily_collection_summary"]
        },
        "daily_collection_summary": {
            "dependencies": ["validated_products", "validated_stock_data"],
            "dependents": []
        }
    }
    return lineage

def get_pipeline_status():
    """Get current pipeline status"""
    try:
        from dagster_project.resources.duckdb_resource import EcommerceDuckDBResource
        
        db_resource = EcommerceDuckDBResource()
        stats = db_resource.get_database_stats()
        
        status = {
            "database": {
                "path": stats.get("database_path"),
                "total_tables": stats.get("total_tables", 0),
                "status": "healthy" if stats.get("total_tables", 0) > 0 else "initializing"
            },
            "data_freshness": {},
            "quality_metrics": {}
        }
        
        # Check data freshness
        for table_name, table_info in stats.get("tables", {}).items():
            if "latest_update" in table_info and table_info["latest_update"]:
                from datetime import datetime, timedelta
                latest = table_info["latest_update"]
                if isinstance(latest, str):
                    latest = datetime.fromisoformat(latest)
                
                hours_ago = (datetime.now() - latest).total_seconds() / 3600
                status["data_freshness"][table_name] = {
                    "last_update": latest.isoformat(),
                    "hours_ago": round(hours_ago, 2),
                    "is_fresh": hours_ago < 24
                }
        
        return status
        
    except Exception as e:
        return {"error": f"Failed to get pipeline status: {str(e)}"}

# Configuration validation
def validate_configuration():
    """Validate that all required configuration is present"""
    required_env_vars = [
        "EBAY_APP_ID",
        "EBAY_CERT_ID", 
        "EBAY_DEV_ID"
    ]
    
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"⚠️  Missing environment variables: {', '.join(missing_vars)}")
        print("Please set these variables in your .env file or environment")
        return False
    
    print("✅ Configuration validation passed")
    return True

# Development utilities
class DevelopmentUtils:
    """Utilities for development and debugging"""
    
    @staticmethod
    def reset_database():
        """Reset the development database"""
        try:
            db_resource = EcommerceDuckDBResource(
                database_path="/app/data/dev_ecommerce_intelligence.duckdb"
            )
            
            # Delete the database file if it exists
            import os
            if os.path.exists(db_resource.database_path):
                os.remove(db_resource.database_path)
                print(f"✅ Deleted database: {db_resource.database_path}")
            
            # Reinitialize
            db_resource._initialize_database()
            print("✅ Database reset and reinitialized")
            
        except Exception as e:
            print(f"❌ Failed to reset database: {e}")
    
    @staticmethod
    def test_api_connections():
        """Test API connections"""
        results = {}
        
        # Test eBay API (mock test since we need credentials)
        try:
            print("🔍 Testing eBay API connection...")
            # This would normally test with real credentials
            results["ebay"] = {"status": "configured", "note": "Requires real credentials"}
            print("⚠️  eBay API: Configured but requires real credentials")
        except Exception as e:
            results["ebay"] = {"status": "error", "error": str(e)}
            print(f"❌ eBay API failed: {e}")
        
        # Test Yahoo Finance API
        try:
            print("🔍 Testing Yahoo Finance API connection...")
            from dagster_project.utils.finance_client import YahooFinanceClient
            
            client = YahooFinanceClient()
            test_data = client.get_current_stock_data(['AAPL'])  # Test with Apple
            
            if test_data and len(test_data) > 0:
                results["yahoo_finance"] = {"status": "success", "test_data": len(test_data)}
                print(f"✅ Yahoo Finance API: Successfully fetched data for {len(test_data)} stocks")
            else:
                results["yahoo_finance"] = {"status": "warning", "note": "No data returned"}
                print("⚠️  Yahoo Finance API: Connected but no data returned")
                
        except Exception as e:
            results["yahoo_finance"] = {"status": "error", "error": str(e)}
            print(f"❌ Yahoo Finance API failed: {e}")
        
        return results
    
    @staticmethod
    def generate_sample_data():
        """Generate sample data for testing"""
        try:
            from datetime import datetime
            import random
            
            # Sample product data
            sample_products = []
            categories = ["Electronics", "Computers", "Cell Phones"]
            
            for i in range(50):
                sample_products.append({
                    'item_id': f'SAMPLE_{i:03d}',
                    'title': f'Sample Product {i}',
                    'price': round(random.uniform(10, 1000), 2),
                    'currency': 'USD',
                    'category': random.choice(categories),
                    'seller': f'Seller_{random.randint(1, 10)}',
                    'location': 'USA',
                    'collected_at': datetime.now(),
                    'partition_date': datetime.now().date()
                })
            
            # Sample stock data
            sample_stocks = []
            symbols = ['AAPL', 'GOOGL', 'AMZN', 'MSFT', 'TSLA']
            
            for symbol in symbols:
                sample_stocks.append({
                    'symbol': symbol,
                    'company_name': f'{symbol} Inc',
                    'current_price': round(random.uniform(100, 500), 2),
                    'currency': 'USD',
                    'day_change_percent': round(random.uniform(-5, 5), 2),
                    'volume': random.randint(1000000, 10000000),
                    'collected_at': datetime.now(),
                    'partition_date': datetime.now().date()
                })
            
            # Store in database
            db_resource = EcommerceDuckDBResource()
            
            import pandas as pd
            products_df = pd.DataFrame(sample_products)
            stocks_df = pd.DataFrame(sample_stocks)
            
            with db_resource.get_connection() as conn:
                conn.execute(f"USE {db_resource.schema_name}")
                
                # Insert sample products
                conn.register('sample_products', products_df)
                conn.execute("""
                    INSERT INTO raw_products (item_id, title, price, currency, category, 
                                            seller, location, collected_at, partition_date)
                    SELECT item_id, title, price, currency, category, 
                           seller, location, collected_at, partition_date
                    FROM sample_products
                """)
                
                # Insert sample stocks
                conn.register('sample_stocks', stocks_df)
                conn.execute("""
                    INSERT INTO raw_stock_data (symbol, company_name, current_price, currency,
                                              day_change_percent, volume, collected_at, partition_date)
                    SELECT symbol, company_name, current_price, currency,
                           day_change_percent, volume, collected_at, partition_date
                    FROM sample_stocks
                """)
            
            print(f"✅ Generated {len(sample_products)} sample products and {len(sample_stocks)} sample stocks")
            return {"products": len(sample_products), "stocks": len(sample_stocks)}
            
        except Exception as e:
            print(f"❌ Failed to generate sample data: {e}")
            return {"error": str(e)}

# Export for easy importing
__all__ = [
    'defs',
    'daily_data_collection_job',
    'daily_schedule',
    'get_asset_lineage',
    'get_pipeline_status',
    'validate_configuration',
    'DevelopmentUtils'
]

# Initialize on import if in development mode
if os.getenv('DAGSTER_ENV', 'development') == 'development':
    print("🚀 E-commerce Intelligence Dashboard - Day 2 Implementation")
    print("=" * 60)
    
    # Validate configuration
    config_ok = validate_configuration()
    
    # Show pipeline status
    try:
        status = get_pipeline_status()
        if "error" not in status:
            print(f"📊 Database Status: {status['database']['status']}")
            print(f"📁 Total Tables: {status['database']['total_tables']}")
        else:
            print(f"⚠️  Pipeline Status: {status['error']}")
    except Exception as e:
        print(f"⚠️  Could not get pipeline status: {e}")
    
    print("\n🎯 Available Commands:")
    print("- docker-compose up: Start the development environment")
    print("- http://localhost:3000: Access Dagster UI") 
    print("- DevelopmentUtils.reset_database(): Reset development database")
    print("- DevelopmentUtils.test_api_connections(): Test API connections")
    print("- DevelopmentUtils.generate_sample_data(): Generate sample data")
    print("\n🔧 Next Steps:")
    print("1. Set up eBay API credentials in .env file")
    print("2. Run the daily_data_collection job in Dagster UI")
    print("3. Monitor data quality metrics")
    print("4. Proceed to Day 3: Advanced Data Processing")