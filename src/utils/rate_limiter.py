import time
from functools import wraps

class RateLimiter:
    def __init__(self, max_requests, time_window):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []

    def can_make_request(self):
        now = time.time()
        # Nettoyer les anciennes requêtes
        self.requests = [req for req in self.requests if now - req < self.time_window]
        
        if len(self.requests) < self.max_requests:
            self.requests.append(now)
            return True
        return False

def rate_limit(max_requests=30, time_window=60):
    limiter = RateLimiter(max_requests, time_window)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            while not limiter.can_make_request():
                time.sleep(1)  # Attendre 1 seconde
            return func(*args, **kwargs)
        return wrapper
    return decorator 