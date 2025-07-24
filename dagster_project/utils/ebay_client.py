"""
eBay API Client for E-commerce Intelligence Dashboard
Handles product data collection from eBay's free tier API (5,000 calls/day)
"""
import requests
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class Product:
    """Product data structure"""
    item_id: str
    title: str
    price: float
    currency: str
    condition: str
    category: str
    seller: str
    location: str
    shipping_cost: Optional[float]
    listing_type: str
    end_time: Optional[str]
    image_url: Optional[str]
    description: Optional[str]
    view_count: Optional[int]
    watch_count: Optional[int]
    collected_at: datetime

class eBayAPIClient:
    """
    eBay API client for collecting product data
    Uses eBay's Browse API (free tier)
    """
    
    def __init__(self, app_id: str, cert_id: str, dev_id: str, environment: str = "sandbox"):
        self.app_id = app_id
        self.cert_id = cert_id
        self.dev_id = dev_id
        self.environment = environment
        self.access_token = None
        self.token_expires_at = None
        
        # API endpoints
        if environment == "production":
            self.base_url = "https://api.ebay.com"
            self.auth_url = "https://api.ebay.com/identity/v1/oauth2/token"
        else:
            self.base_url = "https://api.sandbox.ebay.com"
            self.auth_url = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
        
        # Rate limiting
        self.calls_made = 0
        self.daily_limit = 5000
        self.last_reset = datetime.now().date()
        
    def _get_access_token(self) -> str:
        """Get OAuth access token for API calls"""
        if self.access_token and self.token_expires_at and datetime.now() < self.token_expires_at:
            return self.access_token
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {self._encode_credentials()}'
        }
        
        data = {
            'grant_type': 'client_credentials',
            'scope': 'https://api.ebay.com/oauth/api_scope'
        }
        
        try:
            response = requests.post(self.auth_url, headers=headers, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            expires_in = token_data.get('expires_in', 3600)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            
            logger.info("Successfully obtained eBay access token")
            return self.access_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get eBay access token: {e}")
            raise
    
    def _encode_credentials(self) -> str:
        """Encode app credentials for OAuth"""
        import base64
        credentials = f"{self.app_id}:{self.cert_id}"
        return base64.b64encode(credentials.encode()).decode()
    
    def _check_rate_limit(self):
        """Check and enforce rate limiting"""
        current_date = datetime.now().date()
        
        # Reset counter if it's a new day
        if current_date > self.last_reset:
            self.calls_made = 0
            self.last_reset = current_date
        
        if self.calls_made >= self.daily_limit:
            raise Exception(f"Daily API limit of {self.daily_limit} calls reached")
        
        self.calls_made += 1
    
    def search_products(self, 
                       category: str = "Electronics", 
                       keywords: str = "",
                       limit: int = 50,
                       condition: str = "New") -> List[Product]:
        """
        Search for products on eBay
        
        Args:
            category: Product category to search
            keywords: Search keywords
            limit: Number of products to return (max 200 per call)
            condition: Product condition filter
        
        Returns:
            List of Product objects
        """
        self._check_rate_limit()
        
        # Build search parameters
        params = {
            'q': keywords,
            'category_ids': self._get_category_id(category),
            'filter': f'conditionIds:{{{self._get_condition_id(condition)}}}',
            'limit': min(limit, 200),  # eBay API limit
            'sort': 'newlyListed'
        }
        
        headers = {
            'Authorization': f'Bearer {self._get_access_token()}',
            'Content-Type': 'application/json',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }
        
        url = f"{self.base_url}/buy/browse/v1/item_summary/search"
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            products = []
            
            for item in data.get('itemSummaries', []):
                product = self._parse_product_data(item, category)
                if product:
                    products.append(product)
            
            logger.info(f"Successfully collected {len(products)} products from eBay")
            return products
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to search eBay products: {e}")
            raise
    
    def get_product_details(self, item_id: str) -> Optional[Product]:
        """Get detailed information for a specific product"""
        self._check_rate_limit()
        
        headers = {
            'Authorization': f'Bearer {self._get_access_token()}',
            'Content-Type': 'application/json'
        }
        
        url = f"{self.base_url}/buy/browse/v1/item/{item_id}"
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            item_data = response.json()
            return self._parse_detailed_product(item_data)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get product details for {item_id}: {e}")
            return None
    
    def _parse_product_data(self, item_data: Dict, category: str) -> Optional[Product]:
        """Parse eBay API response into Product object"""
        try:
            price_info = item_data.get('price', {})
            shipping_info = item_data.get('shippingOptions', [{}])[0]
            seller_info = item_data.get('seller', {})
            
            return Product(
                item_id=item_data.get('itemId', ''),
                title=item_data.get('title', ''),
                price=float(price_info.get('value', 0)),
                currency=price_info.get('currency', 'USD'),
                condition=item_data.get('condition', 'Unknown'),
                category=category,
                seller=seller_info.get('username', ''),
                location=item_data.get('itemLocation', {}).get('country', ''),
                shipping_cost=self._parse_shipping_cost(shipping_info),
                listing_type=item_data.get('buyingOptions', [''])[0],
                end_time=item_data.get('itemEndDate'),
                image_url=item_data.get('image', {}).get('imageUrl'),
                description=item_data.get('shortDescription'),
                view_count=None,  # Not available in summary
                watch_count=None,  # Not available in summary
                collected_at=datetime.now()
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Failed to parse product data: {e}")
            return None
    
    def _parse_detailed_product(self, item_data: Dict) -> Optional[Product]:
        """Parse detailed product data"""
        # Similar to _parse_product_data but with more fields
        # Implementation details...
        pass
    
    def _parse_shipping_cost(self, shipping_info: Dict) -> Optional[float]:
        """Extract shipping cost from shipping information"""
        try:
            shipping_cost = shipping_info.get('shippingCost', {})
            if shipping_cost:
                return float(shipping_cost.get('value', 0))
        except (ValueError, TypeError):
            pass
        return None
    
    def _get_category_id(self, category: str) -> str:
        """Map category names to eBay category IDs"""
        category_mapping = {
            'Electronics': '58058',
            'Computers': '58058',
            'Cell Phones': '15032',
            'Video Games': '1249',
            'Home & Garden': '11700',
            'Fashion': '11450',
            'Sports': '888',
            'Automotive': '6000'
        }
        return category_mapping.get(category, '58058')  # Default to Electronics
    
    def _get_condition_id(self, condition: str) -> str:
        """Map condition names to eBay condition IDs"""
        condition_mapping = {
            'New': '1000',
            'Used': '3000',
            'Refurbished': '2000',
            'For parts or not working': '7000'
        }
        return condition_mapping.get(condition, '1000')  # Default to New
    
    def get_trending_searches(self, category: str = "Electronics") -> List[str]:
        """Get trending search terms for a category"""
        # This would require additional API calls or web scraping
        # For now, return some common search terms
        trending_terms = {
            'Electronics': ['iPhone', 'Samsung Galaxy', 'MacBook', 'iPad', 'AirPods'],
            'Computers': ['laptop', 'gaming PC', 'monitor', 'SSD', 'graphics card'],
            'Cell Phones': ['iPhone 15', 'Samsung S24', 'Google Pixel', 'OnePlus'],
        }
        return trending_terms.get(category, ['popular items'])
    
    def collect_category_data(self, 
                            categories: List[str], 
                            products_per_category: int = 100) -> List[Product]:
        """
        Collect product data across multiple categories
        
        Args:
            categories: List of categories to collect from
            products_per_category: Number of products per category
        
        Returns:
            List of all collected products
        """
        all_products = []
        
        for category in categories:
            try:
                logger.info(f"Collecting products from category: {category}")
                
                # Get trending search terms for this category
                trending_terms = self.get_trending_searches(category)
                
                # Collect products for each trending term
                products_per_term = products_per_category // len(trending_terms)
                
                for term in trending_terms:
                    products = self.search_products(
                        category=category,
                        keywords=term,
                        limit=products_per_term
                    )
                    all_products.extend(products)
                    
                    # Rate limiting - sleep between requests
                    time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to collect data for category {category}: {e}")
                continue
        
        logger.info(f"Total products collected: {len(all_products)}")
        return all_products

# Usage example and testing
if __name__ == "__main__":
    # Test with sandbox credentials
    client = eBayAPIClient(
        app_id="your_app_id",
        cert_id="your_cert_id", 
        dev_id="your_dev_id",
        environment="sandbox"
    )
    
    # Test product search
    try:
        products = client.search_products(
            category="Electronics",
            keywords="iPhone",
            limit=10
        )
        
        print(f"Found {len(products)} products")
        for product in products[:3]:  # Show first 3
            print(f"- {product.title}: ${product.price}")
            
    except Exception as e:
        print(f"Test failed: {e}")