import finnhub
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv
from .utils.avro_utils import AvroUtils
from .websocket.finnhub_websocket import FinnhubWebSocket
from .utils.rate_limiter import rate_limit
import time

class FinnhubClient:
    """Main client for Finnhub operations."""
    
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('FINNHUB_TOKEN')
        self.ws_url = f"wss://ws.finnhub.io?token={self.api_key}"
        self.finnhub_client = finnhub.Client(api_key=self.api_key)
        self.avro_utils = AvroUtils()
        self.websocket = FinnhubWebSocket(self.ws_url, self.avro_utils)
        self.websocket.set_subscribe_callback(self._subscribe_symbols)
        self.crypto_symbol_valid_list = []
        self.retry_count = 3
        self.retry_delay = 5

    def get_crypto_symbols(self) -> List[str]:
        """Get list of available crypto symbols."""
        data = self.finnhub_client.crypto_symbols('BINANCE')
        return [str(item['symbol']) for item in data]

    @rate_limit(max_requests=30, time_window=60)
    def get_quote(self, symbol: str) -> Optional[Dict]:
        """Get quote for a specific symbol with retry mechanism."""
        for attempt in range(self.retry_count):
            try:
                return self.finnhub_client.quote(symbol)
            except Exception as e:
                if "429" in str(e):  # API limit reached
                    if attempt < self.retry_count - 1:
                        print(f"API limit reached, waiting {self.retry_delay} seconds...")
                        time.sleep(self.retry_delay)
                        continue
                print(f"Error getting quote for {symbol}: {str(e)}")
                return None
        return None

    def check_symbol_trades(self, symbol: str) -> bool:
        """Check if symbol has recent trades."""
        quote = self.get_quote(symbol)
        return quote['c'] > 0
    
    def validate_symbol_trades(self) -> List[str]:
        """Validate symbol format with error handling."""
        print("Validating symbol trades...")
        
        for symbol in self.get_crypto_symbols():
            quote = self.get_quote(symbol)
            if quote and quote.get('c', 0) > 0:
                self.crypto_symbol_valid_list.append(symbol)
                print(f"Valid symbol found: {symbol}")
            
            # Ajouter un petit délai entre les requêtes
            time.sleep(0.1)
        
        print(f"Found {len(self.crypto_symbol_valid_list)} valid symbols")
        return self.crypto_symbol_valid_list

    def _subscribe_symbols(self, ws) -> None:
        """Subscribe to symbols with recent trades."""
        for symbol in self.crypto_symbol_valid_list:
            ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')

    def run(self) -> None:
        """Start the Finnhub client."""
        self.crypto_symbol_valid_list = self.validate_symbol_trades()
        self.websocket.run() 