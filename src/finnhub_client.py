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
        self.active_symbols: Set[str] = {'BINANCE:BTCUSDT', 'BINANCE:ETHUSDT', 'BINANCE:SOLUSDT', 'BINANCE:BNBUSDT', 'BINANCE:ADAUSDT', 'BINANCE:XRPUSDT'}
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
        ]
        
        for thread in threads:
            thread.start()
        
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Shutting down Finnhub client...")
            self.is_running = False 