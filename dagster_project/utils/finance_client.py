"""
Yahoo Finance Client for E-commerce Intelligence Dashboard
Collects stock data for tech companies and e-commerce platforms
"""
import yfinance as yf
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class StockData:
    """Stock data structure"""
    symbol: str
    company_name: str
    current_price: float
    currency: str
    market_cap: Optional[float]
    pe_ratio: Optional[float]
    day_change: float
    day_change_percent: float
    volume: int
    avg_volume: Optional[int]
    fifty_two_week_high: Optional[float]
    fifty_two_week_low: Optional[float]
    dividend_yield: Optional[float]
    beta: Optional[float]
    sector: Optional[str]
    industry: Optional[str]
    collected_at: datetime

@dataclass
class HistoricalData:
    """Historical stock data structure"""
    symbol: str
    date: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    adj_close: float
    volume: int

class YahooFinanceClient:
    """
    Yahoo Finance client for collecting stock and financial data
    Uses yfinance library (free, no API key required)
    """
    
    def __init__(self):
        # E-commerce and tech companies to track
        self.ecommerce_symbols = {
            'AMZN': 'Amazon.com Inc',
            'EBAY': 'eBay Inc',
            'ETSY': 'Etsy Inc',
            'SHOP': 'Shopify Inc',
            'WMT': 'Walmart Inc',
            'TGT': 'Target Corporation',
            'COST': 'Costco Wholesale Corporation',
            'HD': 'The Home Depot Inc',
            'LOW': "Lowe's Companies Inc"
        }
        
        self.tech_symbols = {
            'AAPL': 'Apple Inc',
            'GOOGL': 'Alphabet Inc',
            'MSFT': 'Microsoft Corporation',
            'META': 'Meta Platforms Inc',
            'NFLX': 'Netflix Inc',
            'NVDA': 'NVIDIA Corporation',
            'TSLA': 'Tesla Inc',
            'CRM': 'Salesforce Inc',
            'ORCL': 'Oracle Corporation',
            'IBM': 'International Business Machines Corporation'
        }
        
        self.all_symbols = {**self.ecommerce_symbols, **self.tech_symbols}
    
    def get_current_stock_data(self, symbols: List[str] = None) -> List[StockData]:
        """
        Get current stock data for specified symbols
        
        Args:
            symbols: List of stock symbols, defaults to all tracked symbols
        
        Returns:
            List of StockData objects
        """
        if symbols is None:
            symbols = list(self.all_symbols.keys())
        
        stock_data_list = []
        
        for symbol in symbols:
            try:
                logger.info(f"Fetching current data for {symbol}")
                
                ticker = yf.Ticker(symbol)
                info = ticker.info
                hist = ticker.history(period="2d")  # Get last 2 days for change calculation
                
                if hist.empty:
                    logger.warning(f"No historical data available for {symbol}")
                    continue
                
                current_price = hist['Close'].iloc[-1]
                previous_close = info.get('previousClose', hist['Close'].iloc[-2] if len(hist) > 1 else current_price)
                
                day_change = current_price - previous_close
                day_change_percent = (day_change / previous_close) * 100 if previous_close != 0 else 0
                
                stock_data = StockData(
                    symbol=symbol,
                    company_name=self.all_symbols.get(symbol, info.get('longName', symbol)),
                    current_price=float(current_price),
                    currency=info.get('currency', 'USD'),
                    market_cap=info.get('marketCap'),
                    pe_ratio=info.get('trailingPE'),
                    day_change=float(day_change),
                    day_change_percent=float(day_change_percent),
                    volume=int(hist['Volume'].iloc[-1]),
                    avg_volume=info.get('averageVolume'),
                    fifty_two_week_high=info.get('fiftyTwoWeekHigh'),
                    fifty_two_week_low=info.get('fiftyTwoWeekLow'),
                    dividend_yield=info.get('dividendYield'),
                    beta=info.get('beta'),
                    sector=info.get('sector'),
                    industry=info.get('industry'),
                    collected_at=datetime.now()
                )
                
                stock_data_list.append(stock_data)
                
            except Exception as e:
                logger.error(f"Failed to fetch data for {symbol}: {e}")
                continue
        
        logger.info(f"Successfully collected data for {len(stock_data_list)} stocks")
        return stock_data_list
    
    def get_historical_data(self, 
                          symbols: List[str], 
                          period: str = "1mo",
                          interval: str = "1d") -> List[HistoricalData]:
        """
        Get historical stock data
        
        Args:
            symbols: List of stock symbols
            period: Data period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
        
        Returns:
            List of HistoricalData objects
        """
        historical_data_list = []
        
        for symbol in symbols:
            try:
                logger.info(f"Fetching historical data for {symbol}")
                
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period=period, interval=interval)
                
                if hist.empty:
                    logger.warning(f"No historical data available for {symbol}")
                    continue
                
                for date, row in hist.iterrows():
                    historical_data = HistoricalData(
                        symbol=symbol,
                        date=date.to_pydatetime(),
                        open_price=float(row['Open']),
                        high_price=float(row['High']),
                        low_price=float(row['Low']),
                        close_price=float(row['Close']),
                        adj_close=float(row['Adj Close']),
                        volume=int(row['Volume'])
                    )
                    historical_data_list.append(historical_data)
                
            except Exception as e:
                logger.error(f"Failed to fetch historical data for {symbol}: {e}")
                continue
        
        logger.info(f"Successfully collected {len(historical_data_list)} historical data points")
        return historical_data_list
    
    def get_market_summary(self) -> Dict[str, Any]:
        """
        Get market summary for tracked companies
        
        Returns:
            Dictionary with market summary statistics
        """
        try:
            current_data = self.get_current_stock_data()
            
            if not current_data:
                return {}
            
            # Calculate summary statistics
            ecommerce_stocks = [stock for stock in current_data if stock.symbol in self.ecommerce_symbols]
            tech_stocks = [stock for stock in current_data if stock.symbol in self.tech_symbols]
            
            summary = {
                'total_companies': len(current_data),
                'ecommerce_companies': len(ecommerce_stocks),
                'tech_companies': len(tech_stocks),
                'market_summary': {
                    'avg_day_change_percent': sum(stock.day_change_percent for stock in current_data) / len(current_data),
                    'positive_movers': len([s for s in current_data if s.day_change_percent > 0]),
                    'negative_movers': len([s for s in current_data if s.day_change_percent < 0]),
                    'highest_gainer': max(current_data, key=lambda x: x.day_change_percent),
                    'biggest_loser': min(current_data, key=lambda x: x.day_change_percent),
                    'most_active': max(current_data, key=lambda x: x.volume),
                },
                'sector_performance': self._calculate_sector_performance(current_data),
                'collected_at': datetime.now()
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to generate market summary: {e}")
            return {}
    
    def _calculate_sector_performance(self, stock_data: List[StockData]) -> Dict[str, Dict]:
        """Calculate performance by sector"""
        sectors = {}
        
        for stock in stock_data:
            if stock.sector:
                if stock.sector not in sectors:
                    sectors[stock.sector] = {
                        'companies': [],
                        'avg_change': 0,
                        'total_market_cap': 0
                    }
                
                sectors[stock.sector]['companies'].append(stock.symbol)
                if stock.market_cap:
                    sectors[stock.sector]['total_market_cap'] += stock.market_cap
        
        # Calculate averages
        for sector, data in sectors.items():
            sector_stocks = [s for s in stock_data if s.sector == sector]
            data['avg_change'] = sum(s.day_change_percent for s in sector_stocks) / len(sector_stocks)
            data['company_count'] = len(sector_stocks)
        
        return sectors
    
    def get_earnings_calendar(self, symbols: List[str] = None) -> List[Dict]:
        """
        Get upcoming earnings dates for tracked companies
        
        Args:
            symbols: List of stock symbols, defaults to all tracked symbols
        
        Returns:
            List of earnings information
        """
        if symbols is None:
            symbols = list(self.all_symbols.keys())
        
        earnings_data = []
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                calendar = ticker.calendar
                
                if calendar is not None and not calendar.empty:
                    earnings_info = {
                        'symbol': symbol,
                        'company_name': self.all_symbols.get(symbol, symbol),
                        'earnings_date': calendar.index[0],
                        'eps_estimate': calendar.iloc[0, 0] if len(calendar.columns) > 0 else None,
                        'revenue_estimate': calendar.iloc[0, 1] if len(calendar.columns) > 1 else None
                    }
                    earnings_data.append(earnings_info)
                
            except Exception as e:
                logger.warning(f"Failed to get earnings data for {symbol}: {e}")
                continue
        
        return earnings_data
    
    def get_analyst_recommendations(self, symbols: List[str] = None) -> List[Dict]:
        """
        Get analyst recommendations for tracked companies
        
        Args:
            symbols: List of stock symbols, defaults to all tracked symbols
        
        Returns:
            List of analyst recommendation data
        """
        if symbols is None:
            symbols = list(self.all_symbols.keys())
        
        recommendations = []
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                recs = ticker.recommendations
                
                if recs is not None and not recs.empty:
                    latest_rec = recs.iloc[-1]  # Most recent recommendation
                    
                    rec_info = {
                        'symbol': symbol,
                        'company_name': self.all_symbols.get(symbol, symbol),
                        'date': latest_rec.name,
                        'firm': latest_rec.get('To Grade', 'Unknown'),
                        'action': latest_rec.get('Action', 'Unknown'),
                        'from_grade': latest_rec.get('From Grade', ''),
                        'to_grade': latest_rec.get('To Grade', ''),
                        'collected_at': datetime.now()
                    }
                    recommendations.append(rec_info)
                
            except Exception as e:
                logger.warning(f"Failed to get recommendations for {symbol}: {e}")
                continue
        
        return recommendations
    
    def get_financial_metrics(self, symbol: str) -> Dict[str, Any]:
        """
        Get comprehensive financial metrics for a specific company
        
        Args:
            symbol: Stock symbol
        
        Returns:
            Dictionary with financial metrics
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # Get financial statements
            financials = ticker.financials
            balance_sheet = ticker.balance_sheet
            cashflow = ticker.cashflow
            
            metrics = {
                'symbol': symbol,
                'company_name': self.all_symbols.get(symbol, info.get('longName', symbol)),
                'valuation_metrics': {
                    'market_cap': info.get('marketCap'),
                    'enterprise_value': info.get('enterpriseValue'),
                    'pe_ratio': info.get('trailingPE'),
                    'forward_pe': info.get('forwardPE'),
                    'peg_ratio': info.get('pegRatio'),
                    'price_to_book': info.get('priceToBook'),
                    'price_to_sales': info.get('priceToSalesTrailing12Months')
                },
                'profitability_metrics': {
                    'profit_margin': info.get('profitMargins'),
                    'operating_margin': info.get('operatingMargins'),
                    'return_on_equity': info.get('returnOnEquity'),
                    'return_on_assets': info.get('returnOnAssets')
                },
                'growth_metrics': {
                    'revenue_growth': info.get('revenueGrowth'),
                    'earnings_growth': info.get('earningsGrowth'),
                    'revenue_per_share': info.get('revenuePerShare'),
                    'book_value_per_share': info.get('bookValue')
                },
                'financial_health': {
                    'total_cash': info.get('totalCash'),
                    'total_debt': info.get('totalDebt'),
                    'debt_to_equity': info.get('debtToEquity'),
                    'current_ratio': info.get('currentRatio'),
                    'quick_ratio': info.get('quickRatio')
                },
                'dividend_info': {
                    'dividend_yield': info.get('dividendYield'),
                    'payout_ratio': info.get('payoutRatio'),
                    'dividend_rate': info.get('dividendRate'),
                    'ex_dividend_date': info.get('exDividendDate')
                },
                'collected_at': datetime.now()
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get financial metrics for {symbol}: {e}")
            return {}
    
    def collect_comprehensive_data(self) -> Dict[str, Any]:
        """
        Collect comprehensive financial data for all tracked companies
        
        Returns:
            Dictionary with all collected data
        """
        logger.info("Starting comprehensive data collection")
        
        comprehensive_data = {
            'current_stock_data': self.get_current_stock_data(),
            'market_summary': self.get_market_summary(),
            'earnings_calendar': self.get_earnings_calendar(),
            'analyst_recommendations': self.get_analyst_recommendations(),
            'collection_timestamp': datetime.now()
        }
        
        # Add historical data for key metrics (last 30 days)
        key_symbols = ['AMZN', 'EBAY', 'AAPL', 'GOOGL', 'MSFT']
        comprehensive_data['historical_data'] = self.get_historical_data(
            symbols=key_symbols,
            period="1mo"
        )
        
        logger.info("Comprehensive data collection completed")
        return comprehensive_data

# Usage example and testing
if __name__ == "__main__":
    client = YahooFinanceClient()
    
    # Test current stock data
    try:
        print("Testing Yahoo Finance client...")
        
        # Get data for a few key stocks
        test_symbols = ['AMZN', 'EBAY', 'AAPL']
        stock_data = client.get_current_stock_data(test_symbols)
        
        print(f"\nFound data for {len(stock_data)} stocks:")
        for stock in stock_data:
            print(f"- {stock.symbol} ({stock.company_name}): ${stock.current_price:.2f} "
                  f"({stock.day_change_percent:+.2f}%)")
        
        # Test market summary
        summary = client.get_market_summary()
        if summary:
            print(f"\nMarket Summary:")
            print(f"- Average daily change: {summary['market_summary']['avg_day_change_percent']:.2f}%")
            print(f"- Positive movers: {summary['market_summary']['positive_movers']}")
            print(f"- Negative movers: {summary['market_summary']['negative_movers']}")
        
    except Exception as e:
        print(f"Test failed: {e}")