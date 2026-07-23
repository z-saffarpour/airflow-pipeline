"""
Retry helper utilities with exponential backoff and jitter.
Provides reusable retry decorators and functions.
"""
import time
import random
import logging
from typing import Callable, Type, Tuple, Optional
from functools import wraps

logger = logging.getLogger(__name__)

def exponential_backoff_with_jitter(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True
) -> float:
    """
    Calculate delay for exponential backoff with optional jitter.
    
    Args:
        attempt: Current attempt number (1-indexed)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential calculation
        jitter: Whether to add random jitter
        
    Returns:
        Delay in seconds
    """
    delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)
    
    if jitter:
        # Add jitter: random value between 0 and delay
        delay = delay * (0.5 + random.random() * 0.5)
    
    return delay


def retry_with_backoff(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    reraise: bool = True,
    on_retry: Optional[Callable[[Exception, int], None]] = None
):
    """
    Decorator for retrying a function with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential calculation
        exceptions: Tuple of exception types to catch and retry
        reraise: Whether to reraise the exception after all retries fail
        on_retry: Optional callback function called on each retry
        
    Returns:
        Decorated function
        
    Example:
        @retry_with_backoff(max_attempts=3, base_delay=2.0, exceptions=(ConnectionError,))
        def fetch_data():
            # ... may fail with ConnectionError
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    
                    if attempt >= max_attempts:
                        logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts",
                            extra={
                                "function": func.__name__,
                                "attempts": max_attempts,
                                "error": str(e)
                            },
                            exc_info=True
                        )
                        if reraise:
                            raise
                        return None
                    
                    # Calculate delay
                    delay = exponential_backoff_with_jitter(
                        attempt=attempt,
                        base_delay=base_delay,
                        max_delay=max_delay,
                        exponential_base=exponential_base
                    )
                    
                    logger.warning(
                        f"Function {func.__name__} failed on attempt {attempt}/{max_attempts}, "
                        f"retrying in {delay:.2f}s",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "delay": delay,
                            "error": str(e)
                        }
                    )
                    
                    # Call on_retry callback if provided
                    if on_retry:
                        try:
                            on_retry(e, attempt)
                        except Exception as callback_error:
                            logger.warning(
                                f"on_retry callback failed: {callback_error}",
                                exc_info=True
                            )
                    
                    # Wait before retry
                    time.sleep(delay)
            
            # Should not reach here, but just in case
            if last_exception and reraise:
                raise last_exception
            return None
            
        return wrapper
    return decorator


class RetryContext:
    """
    Context manager for retry logic with exponential backoff.
    
    Example:
        retry_ctx = RetryContext(max_attempts=3, base_delay=2.0)
        
        while retry_ctx.should_retry():
            try:
                # ... operation that may fail
                break
            except ConnectionError as e:
                retry_ctx.record_failure(e)
    """
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0
    ):
        """
        Initialize retry context.
        
        Args:
            max_attempts: Maximum number of attempts
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds
            exponential_base: Base for exponential calculation
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.current_attempt = 0
        self.last_exception: Optional[Exception] = None
    
    def should_retry(self) -> bool:
        """Check if should retry."""
        return self.current_attempt < self.max_attempts
    
    def record_failure(self, exception: Exception) -> None:
        """
        Record a failure and wait if should retry.
        
        Args:
            exception: The exception that occurred
        """
        self.current_attempt += 1
        self.last_exception = exception
        
        if self.should_retry():
            delay = exponential_backoff_with_jitter(
                attempt=self.current_attempt,
                base_delay=self.base_delay,
                max_delay=self.max_delay,
                exponential_base=self.exponential_base
            )
            
            logger.warning(
                f"Attempt {self.current_attempt}/{self.max_attempts} failed, "
                f"retrying in {delay:.2f}s: {str(exception)}"
            )
            
            time.sleep(delay)
    
    def raise_if_failed(self) -> None:
        """Raise the last exception if all retries failed."""
        if self.current_attempt >= self.max_attempts and self.last_exception:
            logger.error(
                f"All {self.max_attempts} retry attempts failed",
                exc_info=True
            )
            raise self.last_exception
