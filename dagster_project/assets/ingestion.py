"""
Dagster Assets for E-commerce Intelligence Dashboard
Data ingestion and processing assets for product and financial data
"""
from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition, Config
from dagster_duckdb import DuckDBResource
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging

# Import our custom clients (these would be in the utils/ directory)
from dagster_project.utils.ebay_client import eBayAPIClient, Product
from dagster_project.utils.finance_client import YahooFinanceClient, StockData
from dagster_project.utils.validators import DataQualityManager, DataQualityReport

logger = logging.getLogger(__name__)

# Partition definitions for daily data collection
daily_partitions = DailyPartitionsDefinition(start_date="2025-07-23")

class eBayConfig(Config):
    """Configuration for eBay API client"""
    app_id: str
    cert_id: str
    dev_id: str
    environment: str = "sandbox"
    categories: List[str] = ["Electronics", "Computers", "Cell Phones"]
    products_per_category: int = 100

class DataCollectionConfig(Config):
    """Configuration for data collection settings"""
    max_retries: int = 3
    batch_size: int = 1000
    quality_threshold: float = 95.0

@asset(
    partitions_def=daily_partitions,
    description="Raw product data collected from eBay API"
)
def raw_ebay_products(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    ebay_config: eBayConfig
) -> pd.DataFrame:
    """
    Collect raw product data from eBay API
    
    This asset fetches product listings across multiple categories
    and stores them in their raw format for further processing.
    """
    partition_date = context.partition_key
    context.log.info(f"Collecting eBay products for {partition_date}")
    
    # Initialize eBay client
    ebay_client = eBayAPIClient(
        app_id=ebay_config.app_id,
        cert_id=ebay_config.cert_id,
        dev_id=ebay_config.dev_id,
        environment=ebay_config.environment
    )
    
    all_products = []
    
    try:
        # Collect products from each category
        for category in ebay_config.categories:
            context.log.info(f"Collecting products from category: {category}")
            
            products = ebay_client.collect_category_data(
                categories=[category],
                products_per_category=ebay_config.products_per_category
            )
            
            # Convert Product objects to dictionaries
            for product in products:
                product_dict = {
                    'item_id': product.item_id,
                    'title': product.title,
                    'price': product.price,
                    'currency': product.currency,
                    'condition': product.condition,
                    'category': product.category,
                    'seller': product.seller,
                    'location': product.location,
                    'shipping_cost': product.shipping_cost,
                    'listing_type': product.listing_type,
                    'end_time': product.end_time,
                    'image_url': product.image_url,
                    'description': product.description,
                    'view_count': product.view_count,
                    'watch_count': product.watch_count,
                    'collected_at': product.collected_at,
                    'partition_date': partition_date
                }
                all_products.append(product_dict)
        
        context.log.info(f"Successfully collected {len(all_products)} products")
        
        # Convert to DataFrame
        df = pd.DataFrame(all_products)
        
        # Store in DuckDB
        with duckdb.get_connection() as conn:
            # Create table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_products (
                    item_id VARCHAR,
                    title VARCHAR,
                    price DECIMAL,
                    currency VARCHAR,
                    condition VARCHAR,
                    category VARCHAR,
                    seller VARCHAR,
                    location VARCHAR,
                    shipping_cost DECIMAL,
                    listing_type VARCHAR,
                    end_time TIMESTAMP,
                    image_url VARCHAR,
                    description TEXT,
                    view_count INTEGER,
                    watch_count INTEGER,
                    collected_at TIMESTAMP,
                    partition_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert data
            conn.register('products_df', df)
            conn.execute("""
                INSERT INTO raw_products 
                SELECT * FROM products_df
            """)
        
        context.add_output_metadata({
            "num_products": len(df),
            "categories": ebay_config.categories,
            "partition_date": partition_date,
            "avg_price": df['price'].mean() if not df.empty else 0
        })
        
        return df
        
    except Exception as e:
        context.log.error(f"Failed to collect eBay products: {str(e)}")
        raise

@asset(
    partitions_def=daily_partitions,
    description="Raw stock data collected from Yahoo Finance"
)
def raw_stock_data(
    context: AssetExecutionContext,
    duckdb: DuckDBResource
) -> pd.DataFrame:
    """
    Collect raw stock data from Yahoo Finance
    
    This asset fetches current stock prices and financial metrics
    for tracked e-commerce and tech companies.
    """
    partition_date = context.partition_key
    context.log.info(f"Collecting stock data for {partition_date}")
    
    # Initialize Yahoo Finance client
    finance_client = YahooFinanceClient()
    
    try:
        # Collect comprehensive stock data
        comprehensive_data = finance_client.collect_comprehensive_data()
        
        # Extract current stock data
        stock_data_list = comprehensive_data['current_stock_data']
        
        # Convert StockData objects to dictionaries
        stock_records = []
        for stock in stock_data_list:
            stock_dict = {
                'symbol': stock.symbol,
                'company_name': stock.company_name,
                'current_price': stock.current_price,
                'currency': stock.currency,
                'market_cap': stock.market_cap,
                'pe_ratio': stock.pe_ratio,
                'day_change': stock.day_change,
                'day_change_percent': stock.day_change_percent,
                'volume': stock.volume,
                'avg_volume': stock.avg_volume,
                'fifty_two_week_high': stock.fifty_two_week_high,
                'fifty_two_week_low': stock.fifty_two_week_low,
                'dividend_yield': stock.dividend_yield,
                'beta': stock.beta,
                'sector': stock.sector,
                'industry': stock.industry,
                'collected_at': stock.collected_at,
                'partition_date': partition_date
            }
            stock_records.append(stock_dict)
        
        context.log.info(f"Successfully collected data for {len(stock_records)} stocks")
        
        # Convert to DataFrame
        df = pd.DataFrame(stock_records)
        
        # Store in DuckDB
        with duckdb.get_connection() as conn:
            # Create table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_stock_data (
                    symbol VARCHAR,
                    company_name VARCHAR,
                    current_price DECIMAL,
                    currency VARCHAR,
                    market_cap BIGINT,
                    pe_ratio DECIMAL,
                    day_change DECIMAL,
                    day_change_percent DECIMAL,
                    volume BIGINT,
                    avg_volume BIGINT,
                    fifty_two_week_high DECIMAL,
                    fifty_two_week_low DECIMAL,
                    dividend_yield DECIMAL,
                    beta DECIMAL,
                    sector VARCHAR,
                    industry VARCHAR,
                    collected_at TIMESTAMP,
                    partition_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert data
            conn.register('stock_df', df)
            conn.execute("""
                INSERT INTO raw_stock_data 
                SELECT * FROM stock_df
            """)
        
        # Store market summary separately
        market_summary = comprehensive_data.get('market_summary', {})
        if market_summary:
            summary_df = pd.DataFrame([{
                'partition_date': partition_date,
                'total_companies': market_summary.get('total_companies', 0),
                'avg_day_change_percent': market_summary.get('market_summary', {}).get('avg_day_change_percent', 0),
                'positive_movers': market_summary.get('market_summary', {}).get('positive_movers', 0),
                'negative_movers': market_summary.get('market_summary', {}).get('negative_movers', 0),
                'collected_at': datetime.now()
            }])
            
            with duckdb.get_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS market_summary (
                        partition_date DATE,
                        total_companies INTEGER,
                        avg_day_change_percent DECIMAL,
                        positive_movers INTEGER,
                        negative_movers INTEGER,
                        collected_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.register('summary_df', summary_df)
                conn.execute("""
                    INSERT INTO market_summary 
                    SELECT * FROM summary_df
                """)
        
        context.add_output_metadata({
            "num_stocks": len(df),
            "avg_price": df['current_price'].mean() if not df.empty else 0,
            "avg_change_percent": df['day_change_percent'].mean() if not df.empty else 0,
            "partition_date": partition_date
        })
        
        return df
        
    except Exception as e:
        context.log.error(f"Failed to collect stock data: {str(e)}")
        raise

@asset(
    deps=[raw_ebay_products],
    description="Validated and cleaned product data with quality metrics"
)
def validated_products(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_ebay_products: pd.DataFrame,
    data_config: DataCollectionConfig
) -> pd.DataFrame:
    """
    Validate and clean raw product data
    
    This asset applies data quality rules to raw product data,
    filters out invalid records, and generates quality metrics.
    """
    context.log.info(f"Validating {len(raw_ebay_products)} product records")
    
    # Initialize data quality manager
    quality_manager = DataQualityManager()
    
    try:
        # Convert DataFrame to list of dictionaries for validation
        product_dicts = raw_ebay_products.to_dict('records')
        
        # Validate the batch
        quality_report = quality_manager.validate_data_batch(product_dicts, 'product')
        
        context.log.info(f"Data quality score: {quality_report.quality_score:.1f}%")
        
        # Log quality report details
        context.log.info(quality_manager.export_quality_report(quality_report))
        
        # Filter out invalid records based on quality threshold
        if quality_report.quality_score < data_config.quality_threshold:
            context.log.warning(
                f"Quality score {quality_report.quality_score:.1f}% is below threshold "
                f"{data_config.quality_threshold}%"
            )
        
        # Create a set of valid record indices
        invalid_indices = set()
        for result in quality_report.validation_results:
            if not result.passed and hasattr(result, 'record_index'):
                if result.severity.value in ['critical', 'error']:
                    invalid_indices.add(result.record_index)
        
        # Filter out invalid records
        valid_mask = ~raw_ebay_products.index.isin(invalid_indices)
        validated_df = raw_ebay_products[valid_mask].copy()
        
        context.log.info(f"Filtered out {len(invalid_indices)} invalid records")
        context.log.info(f"Retained {len(validated_df)} valid records")
        
        # Add validation metadata
        validated_df['validation_score'] = quality_report.quality_score
        validated_df['validation_timestamp'] = datetime.now()
        
        # Store quality metrics in DuckDB
        with duckdb.get_connection() as conn:
            # Create quality metrics table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_metrics (
                    source VARCHAR,
                    partition_date DATE,
                    total_records INTEGER,
                    valid_records INTEGER,
                    invalid_records INTEGER,
                    quality_score DECIMAL,
                    critical_issues INTEGER,
                    error_issues INTEGER,
                    warning_issues INTEGER,
                    info_issues INTEGER,
                    generated_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Count issues by severity
            issue_counts = {
                'critical': len([r for r in quality_report.validation_results 
                               if r.severity.value == 'critical' and not r.passed]),
                'error': len([r for r in quality_report.validation_results 
                             if r.severity.value == 'error' and not r.passed]),
                'warning': len([r for r in quality_report.validation_results 
                               if r.severity.value == 'warning' and not r.passed]),
                'info': len([r for r in quality_report.validation_results 
                            if r.severity.value == 'info' and not r.passed])
            }
            
            # Insert quality metrics
            conn.execute("""
                INSERT INTO data_quality_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                quality_report.source,
                context.partition_key,
                quality_report.total_records,
                quality_report.valid_records,
                quality_report.invalid_records,
                quality_report.quality_score,
                issue_counts['critical'],
                issue_counts['error'],
                issue_counts['warning'],
                issue_counts['info'],
                quality_report.generated_at
            ])
            
            # Store validated products
            conn.execute("""
                CREATE TABLE IF NOT EXISTS validated_products (
                    item_id VARCHAR,
                    title VARCHAR,
                    price DECIMAL,
                    currency VARCHAR,
                    condition VARCHAR,
                    category VARCHAR,
                    seller VARCHAR,
                    location VARCHAR,
                    shipping_cost DECIMAL,
                    listing_type VARCHAR,
                    end_time TIMESTAMP,
                    image_url VARCHAR,
                    description TEXT,
                    view_count INTEGER,
                    watch_count INTEGER,
                    collected_at TIMESTAMP,
                    partition_date DATE,
                    validation_score DECIMAL,
                    validation_timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.register('validated_df', validated_df)
            conn.execute("""
                INSERT INTO validated_products 
                SELECT * FROM validated_df
            """)
        
        context.add_output_metadata({
            "quality_score": quality_report.quality_score,
            "total_records": quality_report.total_records,
            "valid_records": quality_report.valid_records,
            "invalid_records": quality_report.invalid_records,
            "critical_issues": issue_counts['critical'],
            "error_issues": issue_counts['error'],
            "warning_issues": issue_counts['warning']
        })
        
        return validated_df
        
    except Exception as e:
        context.log.error(f"Failed to validate product data: {str(e)}")
        raise

@asset(
    deps=[raw_stock_data],
    description="Validated and cleaned stock data with quality metrics"
)
def validated_stock_data(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_stock_data: pd.DataFrame,
    data_config: DataCollectionConfig
) -> pd.DataFrame:
    """
    Validate and clean raw stock data
    
    This asset applies data quality rules to raw stock data,
    filters out invalid records, and generates quality metrics.
    """
    context.log.info(f"Validating {len(raw_stock_data)} stock records")
    
    # Initialize data quality manager
    quality_manager = DataQualityManager()
    
    try:
        # Convert DataFrame to list of dictionaries for validation
        stock_dicts = raw_stock_data.to_dict('records')
        
        # Validate the batch
        quality_report = quality_manager.validate_data_batch(stock_dicts, 'stock')
        
        context.log.info(f"Data quality score: {quality_report.quality_score:.1f}%")
        
        # Log quality report details
        context.log.info(quality_manager.export_quality_report(quality_report))
        
        # Filter out invalid records based on quality threshold
        if quality_report.quality_score < data_config.quality_threshold:
            context.log.warning(
                f"Quality score {quality_report.quality_score:.1f}% is below threshold "
                f"{data_config.quality_threshold}%"
            )
        
        # Create a set of valid record indices
        invalid_indices = set()
        for result in quality_report.validation_results:
            if not result.passed and hasattr(result, 'record_index'):
                if result.severity.value in ['critical', 'error']:
                    invalid_indices.add(result.record_index)
        
        # Filter out invalid records
        valid_mask = ~raw_stock_data.index.isin(invalid_indices)
        validated_df = raw_stock_data[valid_mask].copy()
        
        context.log.info(f"Filtered out {len(invalid_indices)} invalid records")
        context.log.info(f"Retained {len(validated_df)} valid records")
        
        # Add validation metadata
        validated_df['validation_score'] = quality_report.quality_score
        validated_df['validation_timestamp'] = datetime.now()
        
        # Store validated stock data
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS validated_stock_data (
                    symbol VARCHAR,
                    company_name VARCHAR,
                    current_price DECIMAL,
                    currency VARCHAR,
                    market_cap BIGINT,
                    pe_ratio DECIMAL,
                    day_change DECIMAL,
                    day_change_percent DECIMAL,
                    volume BIGINT,
                    avg_volume BIGINT,
                    fifty_two_week_high DECIMAL,
                    fifty_two_week_low DECIMAL,
                    dividend_yield DECIMAL,
                    beta DECIMAL,
                    sector VARCHAR,
                    industry VARCHAR,
                    collected_at TIMESTAMP,
                    partition_date DATE,
                    validation_score DECIMAL,
                    validation_timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.register('validated_stock_df', validated_df)
            conn.execute("""
                INSERT INTO validated_stock_data 
                SELECT * FROM validated_stock_df
            """)
        
        context.add_output_metadata({
            "quality_score": quality_report.quality_score,
            "total_records": quality_report.total_records,
            "valid_records": quality_report.valid_records,
            "invalid_records": quality_report.invalid_records
        })
        
        return validated_df
        
    except Exception as e:
        context.log.error(f"Failed to validate stock data: {str(e)}")
        raise

@asset(
    deps=[validated_products, validated_stock_data],
    description="Daily data collection summary and metrics"
)
def daily_collection_summary(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    validated_products: pd.DataFrame,
    validated_stock_data: pd.DataFrame
) -> Dict[str, Any]:
    """
    Generate daily summary of data collection activities
    
    This asset creates a summary report of all data collected,
    quality metrics, and key insights for the day.
    """
    partition_date = context.partition_key
    context.log.info(f"Generating daily summary for {partition_date}")
    
    try:
        # Calculate summary statistics
        summary = {
            'partition_date': partition_date,
            'products': {
                'total_collected': len(validated_products),
                'avg_price': validated_products['price'].mean() if not validated_products.empty else 0,
                'price_range': {
                    'min': validated_products['price'].min() if not validated_products.empty else 0,
                    'max': validated_products['price'].max() if not validated_products.empty else 0
                },
                'top_categories': validated_products['category'].value_counts().head(5).to_dict() if not validated_products.empty else {},
                'top_sellers': validated_products['seller'].value_counts().head(5).to_dict() if not validated_products.empty else {}
            },
            'stocks': {
                'total_collected': len(validated_stock_data),
                'avg_price': validated_stock_data['current_price'].mean() if not validated_stock_data.empty else 0,
                'avg_change_percent': validated_stock_data['day_change_percent'].mean() if not validated_stock_data.empty else 0,
                'positive_movers': len(validated_stock_data[validated_stock_data['day_change_percent'] > 0]) if not validated_stock_data.empty else 0,
                'negative_movers': len(validated_stock_data[validated_stock_data['day_change_percent'] < 0]) if not validated_stock_data.empty else 0,
                'top_gainers': validated_stock_data.nlargest(3, 'day_change_percent')[['symbol', 'day_change_percent']].to_dict('records') if not validated_stock_data.empty else [],
                'top_losers': validated_stock_data.nsmallest(3, 'day_change_percent')[['symbol', 'day_change_percent']].to_dict('records') if not validated_stock_data.empty else []
            },
            'data_quality': {
                'product_quality_score': validated_products['validation_score'].iloc[0] if not validated_products.empty else 0,
                'stock_quality_score': validated_stock_data['validation_score'].iloc[0] if not validated_stock_data.empty else 0
            },
            'generated_at': datetime.now()
        }
        
        # Store summary in DuckDB
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_summaries (
                    partition_date DATE,
                    products_collected INTEGER,
                    stocks_collected INTEGER,
                    avg_product_price DECIMAL,
                    avg_stock_price DECIMAL,
                    avg_stock_change_percent DECIMAL,
                    positive_stock_movers INTEGER,
                    negative_stock_movers INTEGER,
                    product_quality_score DECIMAL,
                    stock_quality_score DECIMAL,
                    generated_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                INSERT INTO daily_summaries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                partition_date,
                summary['products']['total_collected'],
                summary['stocks']['total_collected'],
                summary['products']['avg_price'],
                summary['stocks']['avg_price'],
                summary['stocks']['avg_change_percent'],
                summary['stocks']['positive_movers'],
                summary['stocks']['negative_movers'],
                summary['data_quality']['product_quality_score'],
                summary['data_quality']['stock_quality_score'],
                summary['generated_at']
            ])
        
        context.add_output_metadata({
            "products_collected": summary['products']['total_collected'],
            "stocks_collected": summary['stocks']['total_collected'],
            "avg_product_price": summary['products']['avg_price'],
            "avg_stock_change": summary['stocks']['avg_change_percent'],
            "data_quality_overall": (summary['data_quality']['product_quality_score'] + 
                                   summary['data_quality']['stock_quality_score']) / 2
        })
        
        context.log.info(f"Daily summary generated successfully")
        context.log.info(f"Products collected: {summary['products']['total_collected']}")
        context.log.info(f"Stocks collected: {summary['stocks']['total_collected']}")
        context.log.info(f"Average product price: ${summary['products']['avg_price']:.2f}")
        context.log.info(f"Average stock change: {summary['stocks']['avg_change_percent']:.2f}%")
        
        return summary
        
    except Exception as e:
        context.log.error(f"Failed to generate daily summary: {str(e)}")
        raise

# Sensor for monitoring data quality
from dagster import sensor, RunRequest, SkipReason, SensorEvaluationContext

@sensor(
    asset_selection=[validated_products, validated_stock_data],
    description="Monitor data quality and trigger alerts if quality drops below threshold"
)
def data_quality_sensor(context: SensorEvaluationContext, duckdb: DuckDBResource):
    """
    Sensor to monitor data quality and trigger alerts
    
    This sensor checks the latest data quality metrics and
    can trigger remediation jobs or send alerts if quality drops.
    """
    try:
        with duckdb.get_connection() as conn:
            # Get latest quality metrics
            result = conn.execute("""
                SELECT 
                    source,
                    quality_score,
                    total_records,
                    critical_issues,
                    error_issues,
                    generated_at
                FROM data_quality_metrics 
                WHERE generated_at > NOW() - INTERVAL '1 day'
                ORDER BY generated_at DESC
                LIMIT 10
            """).fetchall()
            
            if not result:
                return SkipReason("No recent quality metrics found")
            
            # Check for quality issues
            quality_issues = []
            for row in result:
                source, quality_score, total_records, critical_issues, error_issues, generated_at = row
                
                if quality_score < 90:  # Quality threshold
                    quality_issues.append({
                        'source': source,
                        'quality_score': quality_score,
                        'critical_issues': critical_issues,
                        'error_issues': error_issues
                    })
            
            if quality_issues:
                context.log.warning(f"Data quality issues detected: {quality_issues}")
                # In a real implementation, this could trigger alerts, emails, etc.
                return SkipReason(f"Quality issues detected but no remediation job configured")
            
            return SkipReason("Data quality is within acceptable range")
            
    except Exception as e:
        context.log.error(f"Data quality sensor failed: {str(e)}")
        return SkipReason(f"Sensor error: {str(e)}")