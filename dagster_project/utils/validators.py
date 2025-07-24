"""
Data Quality Validators for E-commerce Intelligence Dashboard
Ensures data integrity and quality across all data sources
"""
import re
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
from enum import Enum

logger = logging.getLogger(__name__)

class ValidationSeverity(Enum):
    """Validation issue severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class ValidationResult:
    """Validation result structure"""
    field: str
    rule: str
    severity: ValidationSeverity
    message: str
    value: Any
    passed: bool
    timestamp: datetime

@dataclass
class DataQualityReport:
    """Data quality assessment report"""
    source: str
    total_records: int
    valid_records: int
    invalid_records: int
    validation_results: List[ValidationResult]
    quality_score: float
    generated_at: datetime

class ProductDataValidator:
    """Validator for e-commerce product data"""
    
    def __init__(self):
        self.validation_rules = {
            'item_id': [
                self._validate_required,
                self._validate_string_length,
                self._validate_alphanumeric
            ],
            'title': [
                self._validate_required,
                self._validate_string_length,
                self._validate_no_html
            ],
            'price': [
                self._validate_required,
                self._validate_positive_number,
                self._validate_reasonable_price
            ],
            'currency': [
                self._validate_required,
                self._validate_currency_code
            ],
            'category': [
                self._validate_required,
                self._validate_known_category
            ],
            'seller': [
                self._validate_string_length
            ],
            'location': [
                self._validate_string_length
            ],
            'collected_at': [
                self._validate_required,
                self._validate_recent_timestamp
            ]
        }
        
        self.known_currencies = {'USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY', 'CNY'}
        self.known_categories = {
            'Electronics', 'Computers', 'Cell Phones', 'Video Games',
            'Home & Garden', 'Fashion', 'Sports', 'Automotive'
        }
    
    def validate_product(self, product_data: Dict) -> List[ValidationResult]:
        """
        Validate a single product record
        
        Args:
            product_data: Dictionary containing product data
        
        Returns:
            List of validation results
        """
        results = []
        
        for field, rules in self.validation_rules.items():
            value = product_data.get(field)
            
            for rule in rules:
                try:
                    result = rule(field, value, product_data)
                    if result:
                        results.append(result)
                except Exception as e:
                    results.append(ValidationResult(
                        field=field,
                        rule=rule.__name__,
                        severity=ValidationSeverity.ERROR,
                        message=f"Validation error: {str(e)}",
                        value=value,
                        passed=False,
                        timestamp=datetime.now()
                    ))
        
        return results
    
    def validate_product_batch(self, products: List[Dict]) -> DataQualityReport:
        """
        Validate a batch of product records
        
        Args:
            products: List of product dictionaries
        
        Returns:
            Data quality report
        """
        all_results = []
        valid_count = 0
        
        for i, product in enumerate(products):
            product_results = self.validate_product(product)
            
            # Add record index to results
            for result in product_results:
                result.record_index = i
            
            all_results.extend(product_results)
            
            # Count as valid if no critical or error issues
            has_critical_errors = any(
                r.severity in [ValidationSeverity.CRITICAL, ValidationSeverity.ERROR] 
                and not r.passed 
                for r in product_results
            )
            
            if not has_critical_errors:
                valid_count += 1
        
        # Calculate quality score
        quality_score = (valid_count / len(products)) * 100 if products else 0
        
        return DataQualityReport(
            source="product_data",
            total_records=len(products),
            valid_records=valid_count,
            invalid_records=len(products) - valid_count,
            validation_results=all_results,
            quality_score=quality_score,
            generated_at=datetime.now()
        )
    
    def _validate_required(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Check if required field is present and not empty"""
        if value is None or (isinstance(value, str) and not value.strip()):
            return ValidationResult(
                field=field,
                rule="required",
                severity=ValidationSeverity.CRITICAL,
                message=f"Required field '{field}' is missing or empty",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        return None
    
    def _validate_string_length(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate string length constraints"""
        if value is None:
            return None
        
        if not isinstance(value, str):
            return ValidationResult(
                field=field,
                rule="string_type",
                severity=ValidationSeverity.ERROR,
                message=f"Field '{field}' should be a string",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        # Field-specific length constraints
        length_limits = {
            'item_id': (1, 50),
            'title': (1, 200),
            'seller': (1, 80),
            'location': (0, 100)
        }
        
        if field in length_limits:
            min_len, max_len = length_limits[field]
            if len(value) < min_len or len(value) > max_len:
                return ValidationResult(
                    field=field,
                    rule="string_length",
                    severity=ValidationSeverity.WARNING,
                    message=f"Field '{field}' length ({len(value)}) outside range {min_len}-{max_len}",
                    value=value,
                    passed=False,
                    timestamp=datetime.now()
                )
        
        return None
    
    def _validate_alphanumeric(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate alphanumeric characters (for item IDs)"""
        if value is None or not isinstance(value, str):
            return None
        
        if not re.match(r'^[a-zA-Z0-9_-]+$', value):
            return ValidationResult(
                field=field,
                rule="alphanumeric",
                severity=ValidationSeverity.WARNING,
                message=f"Field '{field}' should contain only alphanumeric characters, hyphens, and underscores",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None
    
    def _validate_no_html(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Check for HTML tags in text fields"""
        if value is None or not isinstance(value, str):
            return None
        
        if re.search(r'<[^>]+>', value):
            return ValidationResult(
                field=field,
                rule="no_html",
                severity=ValidationSeverity.WARNING,
                message=f"Field '{field}' contains HTML tags",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None
    
    def _validate_positive_number(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate positive numeric values"""
        if value is None:
            return None
        
        try:
            num_value = float(value)
            if num_value <= 0:
                return ValidationResult(
                    field=field,
                    rule="positive_number",
                    severity=ValidationSeverity.ERROR,
                    message=f"Field '{field}' must be a positive number",
                    value=value,
                    passed=False,
                    timestamp=datetime.now()
                )
        except (ValueError, TypeError):
            return ValidationResult(
                field=field,
                rule="numeric_type",
                severity=ValidationSeverity.ERROR,
                message=f"Field '{field}' must be a valid number",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None
    
    def _validate_reasonable_price(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate price ranges are reasonable"""
        if value is None:
            return None
        
        try:
            price = float(value)
            
            # Check for unreasonably high or low prices
            if price > 100000:  # $100,000+
                return ValidationResult(
                    field=field,
                    rule="reasonable_price",
                    severity=ValidationSeverity.WARNING,
                    message=f"Price ${price:,.2f} seems unusually high",
                    value=value,
                    passed=False,
                    timestamp=datetime.now()
                )
            
            if price < 0.01:  # Less than 1 cent
                return ValidationResult(
                    field=field,
                    rule="reasonable_price",
                    severity=ValidationSeverity.WARNING,
                    message=f"Price ${price:.2f} seems unusually low",
                    value=value,
                    passed=False,
                    timestamp=datetime.now()
                )
        
        except (ValueError, TypeError):
            pass  # Already handled by numeric validation
        
        return None
    
    def _validate_currency_code(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate currency codes"""
        if value is None:
            return None
        
        if not isinstance(value, str) or value.upper() not in self.known_currencies:
            return ValidationResult(
                field=field,
                rule="currency_code",
                severity=ValidationSeverity.WARNING,
                message=f"Unknown currency code: {value}",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None
    
    def _validate_known_category(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate product categories"""
        if value is None:
            return None
        
        if not isinstance(value, str) or value not in self.known_categories:
            return ValidationResult(
                field=field,
                rule="known_category",
                severity=ValidationSeverity.INFO,
                message=f"Category '{value}' not in known categories list",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None
    
    def _validate_recent_timestamp(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate timestamps are recent"""
        if value is None:
            return None
        
        if not isinstance(value, datetime):
            return ValidationResult(
                field=field,
                rule="datetime_type",
                severity=ValidationSeverity.ERROR,
                message=f"Field '{field}' must be a datetime object",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        # Check if timestamp is more than 24 hours old
        if value < datetime.now() - timedelta(hours=24):
            return ValidationResult(
                field=field,
                rule="recent_timestamp",
                severity=ValidationSeverity.WARNING,
                message=f"Timestamp is more than 24 hours old",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        # Check if timestamp is in the future
        if value > datetime.now() + timedelta(minutes=5):  # Allow 5 minutes for clock skew
            return ValidationResult(
                field=field,
                rule="future_timestamp",
                severity=ValidationSeverity.WARNING,
                message=f"Timestamp is in the future",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None

class StockDataValidator:
    """Validator for stock/financial data"""
    
    def __init__(self):
        self.validation_rules = {
            'symbol': [self._validate_required, self._validate_stock_symbol],
            'current_price': [self._validate_required, self._validate_positive_number],
            'currency': [self._validate_required, self._validate_currency_code],
            'day_change_percent': [self._validate_reasonable_change],
            'volume': [self._validate_positive_integer],
            'market_cap': [self._validate_positive_number],
            'collected_at': [self._validate_required, self._validate_recent_timestamp]
        }
        
        self.known_currencies = {'USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY', 'CNY'}
    
    def validate_stock_data(self, stock_data: Dict) -> List[ValidationResult]:
        """Validate stock data record"""
        results = []
        
        for field, rules in self.validation_rules.items():
            value = stock_data.get(field)
            
            for rule in rules:
                try:
                    result = rule(field, value, stock_data)
                    if result:
                        results.append(result)
                except Exception as e:
                    results.append(ValidationResult(
                        field=field,
                        rule=rule.__name__,
                        severity=ValidationSeverity.ERROR,
                        message=f"Validation error: {str(e)}",
                        value=value,
                        passed=False,
                        timestamp=datetime.now()
                    ))
        
        return results
    
    def _validate_stock_symbol(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate stock symbol format"""
        if value is None or not isinstance(value, str):
            return None
        
        if not re.match(r'^[A-Z]{1,5}$', value):
            return ValidationResult(
                field=field,
                rule="stock_symbol",
                severity=ValidationSeverity.WARNING,
                message=f"Stock symbol '{value}' format may be invalid",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None
    
    def _validate_reasonable_change(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate stock price changes are reasonable"""
        if value is None:
            return None
        
        try:
            change_percent = float(value)
            
            # Flag extreme daily changes (>50% or <-50%)
            if abs(change_percent) > 50:
                return ValidationResult(
                    field=field,
                    rule="reasonable_change",
                    severity=ValidationSeverity.WARNING,
                    message=f"Daily change of {change_percent:.2f}% seems extreme",
                    value=value,
                    passed=False,
                    timestamp=datetime.now()
                )
        
        except (ValueError, TypeError):
            return ValidationResult(
                field=field,
                rule="numeric_type",
                severity=ValidationSeverity.ERROR,
                message=f"Field '{field}' must be a valid number",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None
    
    def _validate_positive_integer(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate positive integer values"""
        if value is None:
            return None
        
        try:
            int_value = int(value)
            if int_value < 0:
                return ValidationResult(
                    field=field,
                    rule="positive_integer",
                    severity=ValidationSeverity.ERROR,
                    message=f"Field '{field}' must be a positive integer",
                    value=value,
                    passed=False,
                    timestamp=datetime.now()
                )
        except (ValueError, TypeError):
            return ValidationResult(
                field=field,
                rule="integer_type",
                severity=ValidationSeverity.ERROR,
                message=f"Field '{field}' must be a valid integer",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None
    
    # Reuse some methods from ProductDataValidator
    def _validate_required(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Check if required field is present and not empty"""
        if value is None or (isinstance(value, str) and not value.strip()):
            return ValidationResult(
                field=field,
                rule="required",
                severity=ValidationSeverity.CRITICAL,
                message=f"Required field '{field}' is missing or empty",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        return None
    
    def _validate_positive_number(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate positive numeric values"""
        if value is None:
            return None
        
        try:
            num_value = float(value)
            if num_value <= 0:
                return ValidationResult(
                    field=field,
                    rule="positive_number",
                    severity=ValidationSeverity.ERROR,
                    message=f"Field '{field}' must be a positive number",
                    value=value,
                    passed=False,
                    timestamp=datetime.now()
                )
        except (ValueError, TypeError):
            return ValidationResult(
                field=field,
                rule="numeric_type",
                severity=ValidationSeverity.ERROR,
                message=f"Field '{field}' must be a valid number",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None
    
    def _validate_currency_code(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate currency codes"""
        if value is None:
            return None
        
        if not isinstance(value, str) or value.upper() not in self.known_currencies:
            return ValidationResult(
                field=field,
                rule="currency_code",
                severity=ValidationSeverity.WARNING,
                message=f"Unknown currency code: {value}",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None
    
    def _validate_recent_timestamp(self, field: str, value: Any, record: Dict) -> Optional[ValidationResult]:
        """Validate timestamps are recent"""
        if value is None:
            return None
        
        if not isinstance(value, datetime):
            return ValidationResult(
                field=field,
                rule="datetime_type",
                severity=ValidationSeverity.ERROR,
                message=f"Field '{field}' must be a datetime object",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        # Check if timestamp is more than 24 hours old
        if value < datetime.now() - timedelta(hours=24):
            return ValidationResult(
                field=field,
                rule="recent_timestamp",
                severity=ValidationSeverity.WARNING,
                message=f"Timestamp is more than 24 hours old",
                value=value,
                passed=False,
                timestamp=datetime.now()
            )
        
        return None

class DataQualityManager:
    """Central manager for data quality operations"""
    
    def __init__(self):
        self.product_validator = ProductDataValidator()
        self.stock_validator = StockDataValidator()
        self.quality_history = []
    
    def validate_data_batch(self, data: List[Dict], data_type: str) -> DataQualityReport:
        """
        Validate a batch of data records
        
        Args:
            data: List of data records
            data_type: Type of data ('product' or 'stock')
        
        Returns:
            Data quality report
        """
        if data_type == 'product':
            report = self.product_validator.validate_product_batch(data)
        elif data_type == 'stock':
            report = self._validate_stock_batch(data)
        else:
            raise ValueError(f"Unknown data type: {data_type}")
        
        # Store in history
        self.quality_history.append(report)
        
        # Keep only last 100 reports
        if len(self.quality_history) > 100:
            self.quality_history = self.quality_history[-100:]
        
        return report
    
    def _validate_stock_batch(self, stocks: List[Dict]) -> DataQualityReport:
        """Validate batch of stock data"""
        all_results = []
        valid_count = 0
        
        for i, stock in enumerate(stocks):
            stock_results = self.stock_validator.validate_stock_data(stock)
            
            # Add record index to results
            for result in stock_results:
                result.record_index = i
            
            all_results.extend(stock_results)
            
            # Count as valid if no critical or error issues
            has_critical_errors = any(
                r.severity in [ValidationSeverity.CRITICAL, ValidationSeverity.ERROR] 
                and not r.passed 
                for r in stock_results
            )
            
            if not has_critical_errors:
                valid_count += 1
        
        # Calculate quality score
        quality_score = (valid_count / len(stocks)) * 100 if stocks else 0
        
        return DataQualityReport(
            source="stock_data",
            total_records=len(stocks),
            valid_records=valid_count,
            invalid_records=len(stocks) - valid_count,
            validation_results=all_results,
            quality_score=quality_score,
            generated_at=datetime.now()
        )
    
    def get_quality_summary(self) -> Dict[str, Any]:
        """Get summary of data quality over time"""
        if not self.quality_history:
            return {}
        
        recent_reports = self.quality_history[-10:]  # Last 10 reports
        
        avg_quality_score = sum(r.quality_score for r in recent_reports) / len(recent_reports)
        
        total_records = sum(r.total_records for r in recent_reports)
        total_valid = sum(r.valid_records for r in recent_reports)
        
        # Count issues by severity
        all_results = []
        for report in recent_reports:
            all_results.extend(report.validation_results)
        
        severity_counts = {}
        for severity in ValidationSeverity:
            severity_counts[severity.value] = len([
                r for r in all_results 
                if r.severity == severity and not r.passed
            ])
        
        return {
            'average_quality_score': avg_quality_score,
            'total_records_processed': total_records,
            'total_valid_records': total_valid,
            'validation_issues': severity_counts,
            'reports_count': len(recent_reports),
            'last_updated': max(r.generated_at for r in recent_reports)
        }
    
    def export_quality_report(self, report: DataQualityReport) -> str:
        """Export quality report as formatted string"""
        output = []
        output.append(f"Data Quality Report - {report.source}")
        output.append("=" * 50)
        output.append(f"Generated: {report.generated_at}")
        output.append(f"Quality Score: {report.quality_score:.1f}%")
        output.append(f"Total Records: {report.total_records}")
        output.append(f"Valid Records: {report.valid_records}")
        output.append(f"Invalid Records: {report.invalid_records}")
        
        if report.validation_results:
            output.append("\nValidation Issues:")
            output.append("-" * 30)
            
            # Group by severity
            by_severity = {}
            for result in report.validation_results:
                if not result.passed:
                    if result.severity not in by_severity:
                        by_severity[result.severity] = []
                    by_severity[result.severity].append(result)
            
            for severity in ValidationSeverity:
                if severity in by_severity:
                    output.append(f"\n{severity.value.upper()} ({len(by_severity[severity])}):")
                    for result in by_severity[severity][:10]:  # Show first 10
                        output.append(f"  - {result.field}: {result.message}")
                    
                    if len(by_severity[severity]) > 10:
                        output.append(f"  ... and {len(by_severity[severity]) - 10} more")
        
        return "\n".join(output)

# Usage example and testing
if __name__ == "__main__":
    # Test data validation
    validator = DataQualityManager()
    
    # Sample product data for testing
    test_products = [
        {
            'item_id': 'PROD123',
            'title': 'iPhone 15 Pro',
            'price': 999.99,
            'currency': 'USD',
            'category': 'Electronics',
            'seller': 'TechStore',
            'location': 'USA',
            'collected_at': datetime.now()
        },
        {
            'item_id': '',  # Invalid - empty
            'title': 'Samsung Galaxy<script>alert("xss")</script>',  # Invalid - HTML
            'price': -50,  # Invalid - negative
            'currency': 'XXX',  # Invalid - unknown currency
            'category': 'Electronics',
            'seller': 'BadSeller',
            'location': 'Unknown',
            'collected_at': datetime.now()
        }
    ]
    
    print("Testing data validation...")
    report = validator.validate_data_batch(test_products, 'product')
    
    print(f"Quality Score: {report.quality_score:.1f}%")
    print(f"Issues Found: {len([r for r in report.validation_results if not r.passed])}")
    
    # Print detailed report
    print("\n" + validator.export_quality_report(report))