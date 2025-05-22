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
            self._refill()
            
            # If we have enough tokens, take them and return True
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
                
            # If not blocking, return False immediately
            if not block:
                return False
                
            # Calculate how long we need to wait to get enough tokens
            deficit = tokens - self.tokens
            wait_time = deficit / self.rate
            
            # If timeout is specified and wait time exceeds it, return False
            if timeout is not None and wait_time > timeout:
                return False
                
            # If we've already waited too long, return False
            if timeout is not None and time.time() - start_time >= timeout:
                return False
                
            # Wait for the required time
            if wait_time > 0:
                if timeout is not None:
                    # Adjust wait time to respect the timeout
                    remaining_timeout = timeout - (time.time() - start_time)
                    wait_time = min(wait_time, remaining_timeout)
                    if wait_time <= 0:
                        return False
                
                time.sleep(wait_time)
                
                # Refill the bucket again after waiting
                self._refill()
                
                # Check if we now have enough tokens
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True
                else:
                    # Something went wrong with our calculations
                    return False
            
            # Take the tokens and return True
            self.tokens -= tokens
            return True


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