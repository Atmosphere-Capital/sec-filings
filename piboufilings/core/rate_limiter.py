"""
Rate limiter implementation for SEC EDGAR API access.
"""

import time
import threading
from typing import Optional


class TokenBucketRateLimiter:
    """
    Implementation of the token bucket algorithm for rate limiting.
    This ensures requests to the SEC API don't exceed the allowed rate.
    """
    
    def __init__(self, rate: float, capacity: int = None):
        """
        Initialize the rate limiter.
        
        Args:
            rate: Rate at which tokens are added to the bucket (tokens per second)
            capacity: Maximum number of tokens the bucket can hold (defaults to rate)
        """
        self.rate = rate
        self.capacity = capacity if capacity is not None else rate
        self.tokens = self.capacity
        self.last_refill_time = time.time()
        self.lock = threading.RLock()  # Use RLock for thread safety
        
    def _refill(self):
        """Refill the token bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill_time
        
        # Calculate how many new tokens to add based on elapsed time
        new_tokens = elapsed * self.rate
        
        # Update token count, capped at capacity
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill_time = now
        
    def acquire(self, tokens: int = 1, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire tokens from the bucket. If not enough tokens are available,
        either wait until they become available or return False.
        
        Args:
            tokens: Number of tokens to acquire
            block: Whether to block until tokens become available
            timeout: Maximum time to wait for tokens (in seconds)
            
        Returns:
            bool: True if tokens were acquired, False otherwise
        """
        start_time = time.time()
        
        with self.lock:
            while True:
                self._refill()
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True
                
                if not block:
                    return False # Not enough tokens and not blocking
                
                # Blocking mode: calculate wait time and sleep
                deficit = tokens - self.tokens # Deficit should be > 0 here
                
                if self.rate <= 0: # Cannot acquire if rate is zero or negative
                    return False 
                    
                required_wait_time = deficit / self.rate
                
                if timeout is not None:
                    elapsed_time = time.time() - start_time
                    remaining_timeout = timeout - elapsed_time
                    if remaining_timeout <= 0: # Timeout expired
                        return False
                    if required_wait_time > remaining_timeout:
                        # Not enough time left in timeout to wait for the needed tokens,
                        # so sleep for the remaining_timeout and then re-check (will likely fail if still deficit).
                        # Or, we could return False directly here, but sleeping for remaining_timeout
                        # gives a chance if tokens are added by a very small amount in that window.
                        # For simplicity and to ensure timeout is respected strictly for *this* attempt to acquire:
                        return False # Cannot wait long enough
                
                # Determine actual sleep time
                sleep_duration = required_wait_time
                if timeout is not None:
                    sleep_duration = min(required_wait_time, remaining_timeout) # Ensure we don't sleep past timeout

                if sleep_duration > 0: # Only sleep if there's a positive duration
                    time.sleep(sleep_duration)
                
                # After sleep (or if no sleep was needed but still in loop due to timeout logic),
                # the loop will continue, _refill, and check tokens again.
                # If timeout occurred and we returned False, loop is exited.
                # If timeout is None, loop continues until tokens are acquired.


class GlobalRateLimiter:
    """
    A singleton rate limiter to ensure all SEC API requests across all instances
    don't exceed the allowed rate.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(GlobalRateLimiter, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
            
    def __init__(self, rate: float = 10.0, safety_factor: float = 0.7):
        """
        Initialize the global rate limiter.
        
        Args:
            rate: Maximum allowed requests per second (defaults to 10.0)
            safety_factor: Factor to apply to rate for safety margin (defaults to 0.7)
        """
        # Only initialize once
        if self._initialized:
            return
            
        self.limiter = TokenBucketRateLimiter(rate * safety_factor)
        self._initialized = True
        
    def acquire(self, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire permission to make a request.
        
        Args:
            block: Whether to block until a token becomes available
            timeout: Maximum time to wait for a token (in seconds)
            
        Returns:
            bool: True if permission was granted, False otherwise
        """
        return self.limiter.acquire(tokens=1, block=block, timeout=timeout) 