import finnhub
import os
import time
import logging
from typing import List, Dict, Optional, Set
from threading import Thread, Lock
from dotenv import load_dotenv
from src.utils.avro_utils import AvroUtils
from src.websocket.finnhub_websocket import FinnhubWebSocket
from src.utils.rate_limiter import rate_limit

class FinnhubClient:
   
    API_BATCH_SIZE = 30  # Maximum API calls per minute
    API_WAIT_TIME = 60   # Seconds to wait between batches
    
    def __init__(self):
        """Initialize the Finnhub client with necessary configurations and connections."""
        self._initialize_configuration()
        self._initialize_components()
        self._initialize_state()
        self._setup_logging()

    def _initialize_configuration(self) -> None:
        """Set up API configuration and endpoints."""
        load_dotenv()
        self.api_key = os.getenv('FINNHUB_TOKEN')
        if not self.api_key:
            raise ValueError("FINNHUB_TOKEN environment variable is not set")
        self.ws_url = f"wss://ws.finnhub.io?token={self.api_key}"

    def _initialize_components(self) -> None:
        """Initialize main components and utilities."""
        self.finnhub_client = finnhub.Client(api_key=self.api_key)
        self.avro_utils = AvroUtils()
        self.websocket = self._initialize_websocket()

    def _initialize_state(self) -> None:
        """Initialize internal state tracking."""
        self.active_symbols: Set[str] = set()
        self.is_running: bool = False
        self._symbols_lock = Lock()

    def _setup_logging(self) -> None:
        """Configure logging for the client."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('finnhub_client.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('FinnhubClient')

    def _initialize_websocket(self) -> FinnhubWebSocket:
        """Initialize and configure WebSocket connection."""
        websocket = FinnhubWebSocket(self.ws_url, self.avro_utils)
        websocket.set_subscribe_callback(self._subscribe_symbols)
        return websocket

    @rate_limit(max_requests=30, time_window=60)
    def get_quote(self, symbol: str) -> Optional[Dict]:
        """
        Fetch quote data for a specific symbol with retry mechanism.
        """
        for attempt in range(3):
            try:
                return self.finnhub_client.quote(symbol)
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    self.logger.warning(f"Rate limit reached, waiting 5 seconds...")
                    time.sleep(5)
                    continue
                self.logger.error(f"Error fetching quote for {symbol}: {str(e)}")
                return None
        return None

    def get_crypto_symbols(self) -> List[str]:
        """
        Fetch available crypto symbols from Binance.
        """
        try:
            data = self.finnhub_client.crypto_symbols('BINANCE')
            return [str(item['symbol']) for item in data]
        except Exception as e:
            self.logger.error(f"Error fetching crypto symbols: {e}")
            return []

    def _validate_symbols_batch(self, symbols: List[str]) -> List[str]:
        """
        Validate a batch of symbols against the API.
        
        Args:
            symbols: List of symbols to validate
            
        Returns:
            List of valid symbols with active trading
        """
        valid_symbols = []
        for symbol in symbols:
            if not self.is_running:
                break
            quote = self.get_quote(symbol)
            if quote and quote.get('c', 0) > 0:
                valid_symbols.append(symbol)
                self.logger.info(f"Valid symbol found: {symbol}")
        return valid_symbols

    def _symbol_validation_worker(self) -> None:
        """Background worker for continuous symbol validation."""
        while self.is_running:
            try:
                all_symbols = self.get_crypto_symbols()
                
                for i in range(0, len(all_symbols), self.API_BATCH_SIZE):
                    if not self.is_running:
                        break
                        
                    batch = all_symbols[i:i + self.API_BATCH_SIZE]
                    valid_symbols = self._validate_symbols_batch(batch)
                    
                    self._update_active_symbols(valid_symbols)
                    
                    if i + self.API_BATCH_SIZE < len(all_symbols):
                        self.logger.info(f"Waiting {self.API_WAIT_TIME}s for next batch...")
                        time.sleep(self.API_WAIT_TIME)
                
            except Exception as e:
                self.logger.error(f"Symbol validation error: {e}")
                time.sleep(self.API_WAIT_TIME)

    def _update_active_symbols(self, valid_symbols: List[str]) -> None:
        """
        Update the set of active symbols and their subscriptions.
        
        Args:
            valid_symbols: List of newly validated symbols
        """
        with self._symbols_lock:
            for symbol in valid_symbols:
                if symbol not in self.active_symbols:
                    self.websocket.subscribe_symbol(symbol)
                    self.active_symbols.add(symbol)

    def _subscribe_symbols(self, ws) -> None:
        """Subscribe to all active symbols on WebSocket reconnection."""
        with self._symbols_lock:
            for symbol in self.active_symbols:
                ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')

    def run(self) -> None:
        """
        Start the Finnhub client and its components.
        """
        self.is_running = True
        self.logger.info("Starting Finnhub client...")
        
        threads = [
            Thread(target=self.websocket.run, daemon=True),
            Thread(target=self._symbol_validation_worker, daemon=True)
        ]
        
        for thread in threads:
            thread.start()
        
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Shutting down Finnhub client...")
            self.is_running = False 