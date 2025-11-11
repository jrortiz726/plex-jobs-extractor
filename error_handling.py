#!/usr/bin/env python3
"""
Enhanced Error Handling and Retry Logic for Plex-CDF Extractors
Implements intelligent retry with exponential backoff, circuit breaker pattern,
and specific error type handling.
"""

import asyncio
import logging
import time
from typing import Any, Callable, Optional, Type, TypeVar, Union, Dict, List
from functools import wraps
from datetime import datetime, timedelta
from enum import Enum
import httpx
import aiohttp
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ErrorCategory(Enum):
    """Categories of errors for appropriate handling"""
    RATE_LIMIT = "rate_limit"
    NETWORK = "network"
    AUTHENTICATION = "authentication"
    NOT_FOUND = "not_found"
    VALIDATION = "validation"
    SERVER_ERROR = "server_error"
    UNKNOWN = "unknown"


class ExtractorError(Exception):
    """Base exception for extractor errors"""
    def __init__(self, message: str, category: ErrorCategory = ErrorCategory.UNKNOWN, retry_after: Optional[int] = None):
        super().__init__(message)
        self.category = category
        self.retry_after = retry_after
        self.timestamp = datetime.now()


class PlexAPIError(ExtractorError):
    """Plex API specific errors"""
    pass


class PlexRateLimitError(PlexAPIError):
    """Rate limit exceeded for Plex API"""
    def __init__(self, message: str = "Rate limit exceeded", retry_after: int = 60):
        super().__init__(message, ErrorCategory.RATE_LIMIT, retry_after)


class CDFAPIError(ExtractorError):
    """CDF API specific errors"""
    pass


class NetworkError(ExtractorError):
    """Network-related errors"""
    def __init__(self, message: str = "Network error"):
        super().__init__(message, ErrorCategory.NETWORK)


class ValidationError(ExtractorError):
    """Data validation errors"""
    def __init__(self, message: str = "Validation error"):
        super().__init__(message, ErrorCategory.VALIDATION)


class RetryConfig:
    """Configuration for retry behavior"""
    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retry_on: Optional[List[Type[Exception]]] = None,
        dont_retry_on: Optional[List[Type[Exception]]] = None
    ):
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retry_on = retry_on or [
            PlexRateLimitError,
            NetworkError,
            httpx.TimeoutException,
            aiohttp.ClientError,
            CogniteAPIError
        ]
        self.dont_retry_on = dont_retry_on or [
            ValidationError,
            CogniteNotFoundError
        ]


class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: Type[Exception] = Exception
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "closed"  # closed, open, half-open
        
    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute function with circuit breaker protection"""
        if self.state == "open":
            if self._should_attempt_reset():
                self.state = "half-open"
            else:
                raise ExtractorError(f"Circuit breaker is open (failures: {self.failure_count})")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise e
    
    async def async_call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with circuit breaker protection"""
        if self.state == "open":
            if self._should_attempt_reset():
                self.state = "half-open"
            else:
                raise ExtractorError(f"Circuit breaker is open (failures: {self.failure_count})")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit"""
        if self.last_failure_time:
            time_since_failure = (datetime.now() - self.last_failure_time).seconds
            return time_since_failure >= self.recovery_timeout
        return False
    
    def _on_success(self):
        """Handle successful call"""
        self.failure_count = 0
        self.state = "closed"
    
    def _on_failure(self):
        """Handle failed call"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")


class RetryHandler:
    """Advanced retry handler with exponential backoff and jitter"""
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
    
    def get_circuit_breaker(self, name: str) -> CircuitBreaker:
        """Get or create a circuit breaker for a specific endpoint"""
        if name not in self.circuit_breakers:
            self.circuit_breakers[name] = CircuitBreaker()
        return self.circuit_breakers[name]
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and optional jitter"""
        delay = min(
            self.config.initial_delay * (self.config.exponential_base ** attempt),
            self.config.max_delay
        )
        
        if self.config.jitter:
            import random
            delay *= (0.5 + random.random())  # Add 0-50% jitter
        
        return delay
    
    def should_retry(self, exception: Exception) -> bool:
        """Determine if we should retry based on exception type"""
        # Don't retry for specific exceptions
        for exc_type in self.config.dont_retry_on:
            if isinstance(exception, exc_type):
                return False
        
        # Only retry for allowed exceptions
        for exc_type in self.config.retry_on:
            if isinstance(exception, exc_type):
                return True
        
        return False
    
    async def async_retry(
        self,
        func: Callable[..., T],
        *args,
        circuit_breaker_name: Optional[str] = None,
        **kwargs
    ) -> T:
        """Execute async function with retry logic"""
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                # Use circuit breaker if name provided
                if circuit_breaker_name:
                    breaker = self.get_circuit_breaker(circuit_breaker_name)
                    return await breaker.async_call(func, *args, **kwargs)
                else:
                    return await func(*args, **kwargs)
                    
            except Exception as e:
                last_exception = e
                
                # Check if we should retry
                if not self.should_retry(e):
                    logger.error(f"Non-retryable error: {e}")
                    raise
                
                # Check if this is the last attempt
                if attempt == self.config.max_attempts - 1:
                    logger.error(f"Max retry attempts ({self.config.max_attempts}) exceeded")
                    raise
                
                # Handle rate limit errors specially
                if isinstance(e, PlexRateLimitError) and e.retry_after:
                    delay = e.retry_after
                    logger.warning(f"Rate limited, waiting {delay}s before retry")
                else:
                    delay = self.calculate_delay(attempt)
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f}s")
                
                await asyncio.sleep(delay)
        
        raise last_exception
    
    def sync_retry(
        self,
        func: Callable[..., T],
        *args,
        circuit_breaker_name: Optional[str] = None,
        **kwargs
    ) -> T:
        """Execute sync function with retry logic"""
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                # Use circuit breaker if name provided
                if circuit_breaker_name:
                    breaker = self.get_circuit_breaker(circuit_breaker_name)
                    return breaker.call(func, *args, **kwargs)
                else:
                    return func(*args, **kwargs)
                    
            except Exception as e:
                last_exception = e
                
                # Check if we should retry
                if not self.should_retry(e):
                    logger.error(f"Non-retryable error: {e}")
                    raise
                
                # Check if this is the last attempt
                if attempt == self.config.max_attempts - 1:
                    logger.error(f"Max retry attempts ({self.config.max_attempts}) exceeded")
                    raise
                
                # Handle rate limit errors specially
                if isinstance(e, PlexRateLimitError) and e.retry_after:
                    delay = e.retry_after
                    logger.warning(f"Rate limited, waiting {delay}s before retry")
                else:
                    delay = self.calculate_delay(attempt)
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f}s")
                
                time.sleep(delay)
        
        raise last_exception


def with_retry(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    exponential_base: float = 2.0,
    retry_on: Optional[List[Type[Exception]]] = None
):
    """Decorator for adding retry logic to functions"""
    def decorator(func):
        config = RetryConfig(
            max_attempts=max_attempts,
            initial_delay=initial_delay,
            exponential_base=exponential_base,
            retry_on=retry_on
        )
        handler = RetryHandler(config)
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await handler.async_retry(func, *args, **kwargs)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return handler.sync_retry(func, *args, **kwargs)
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


class ErrorAggregator:
    """Aggregate and analyze errors for reporting"""
    
    def __init__(self, window_size: int = 3600):  # 1 hour window
        self.errors: List[ExtractorError] = []
        self.window_size = window_size
    
    def add_error(self, error: ExtractorError):
        """Add an error to the aggregator"""
        self.errors.append(error)
        self._cleanup_old_errors()
    
    def _cleanup_old_errors(self):
        """Remove errors outside the time window"""
        cutoff_time = datetime.now() - timedelta(seconds=self.window_size)
        self.errors = [e for e in self.errors if e.timestamp > cutoff_time]
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of errors by category"""
        self._cleanup_old_errors()
        
        summary = {
            "total": len(self.errors),
            "by_category": {},
            "recent_errors": []
        }
        
        # Count by category
        for error in self.errors:
            category = error.category.value
            summary["by_category"][category] = summary["by_category"].get(category, 0) + 1
        
        # Get recent errors (last 10)
        summary["recent_errors"] = [
            {
                "message": str(e),
                "category": e.category.value,
                "timestamp": e.timestamp.isoformat()
            }
            for e in self.errors[-10:]
        ]
        
        return summary
    
    def should_alert(self, threshold: int = 10) -> bool:
        """Check if error rate exceeds threshold"""
        self._cleanup_old_errors()
        return len(self.errors) >= threshold


# Global error aggregator instance
error_aggregator = ErrorAggregator()


def handle_api_response(response: Union[httpx.Response, aiohttp.ClientResponse], api_name: str = "API") -> None:
    """
    Handle API response and raise appropriate exceptions
    
    Args:
        response: HTTP response object
        api_name: Name of the API for error messages
    """
    status = response.status if hasattr(response, 'status') else response.status_code
    
    if status == 429:
        retry_after = int(response.headers.get('Retry-After', 60))
        raise PlexRateLimitError(f"{api_name} rate limit exceeded", retry_after)
    elif status == 401:
        raise ExtractorError(f"{api_name} authentication failed", ErrorCategory.AUTHENTICATION)
    elif status == 404:
        raise ExtractorError(f"{api_name} resource not found", ErrorCategory.NOT_FOUND)
    elif 500 <= status < 600:
        raise ExtractorError(f"{api_name} server error: {status}", ErrorCategory.SERVER_ERROR)
    elif status >= 400:
        raise ExtractorError(f"{api_name} client error: {status}", ErrorCategory.VALIDATION)