import websocket
import json
import os
import time
import logging
from typing import Callable, Set
from threading import Lock
from src.utils.avro_utils import AvroUtils
from src.utils.kafka_producer import KafkaMessageProducer

class FinnhubWebSocket:

    RECONNECTION_DELAY = 5  # Seconds to wait before reconnection attempts
    
    def __init__(self, ws_url: str, avro_utils: AvroUtils):
        """
        Initialize the WebSocket handler.
        """
        self.ws_url = ws_url
        self.avro_utils = avro_utils
        self.ws = None
        self.subscribed_symbols: Set[str] = set()
        self._connect_lock = Lock()
        self._setup_logging()
        self._subscribe_callback = None
        self.kafka_producer = KafkaMessageProducer()

    def _setup_logging(self) -> None:
        """Configure logging for the WebSocket handler."""
        self.logger = logging.getLogger('FinnhubWebSocket')

    def set_subscribe_callback(self, callback: Callable) -> None:
        """
        Set callback function for subscription management.

        Args:
            callback: Function to be called on connection open
        """
        self._subscribe_callback = callback

    def subscribe_symbol(self, symbol: str) -> None:
        """
        Subscribe to a trading symbol's real-time data.

        Args:
            symbol: Trading symbol to subscribe to
        """
        with self._connect_lock:
            if not self.ws:
                self.logger.warning(f"Cannot subscribe to {symbol}: WebSocket not connected")
                return

            if symbol in self.subscribed_symbols:
                self.logger.debug(f"Symbol {symbol} already subscribed")
                return

            try:
                subscription_message = json.dumps({
                    "type": "subscribe",
                    "symbol": symbol
                })
                self.ws.send(subscription_message)
                self.subscribed_symbols.add(symbol)
                self.logger.info(f"Subscribed to symbol: {symbol}")
            except Exception as e:
                self.logger.error(f"Failed to subscribe to {symbol}: {str(e)}")

    def _on_message(self, ws, message) -> None:
        """
        Handle and display incoming WebSocket messages.

        Args:
            ws: WebSocket instance
            message: Raw message data from WebSocket
        """
        try:
            message = json.loads(message)
            schema_path = os.path.join(os.path.dirname(__file__), '..', 'schema', 'trades.avsc')
            
            # Encode message to Avro format
            avro_message = self.avro_utils.avro_encode(
                {
                    'data': message['data'],
                    'type': message['type']
                },
                self.avro_utils.load_avro_schema(schema_path)
            )
            self.kafka_producer.send_message(avro_message)
            # Decode and print the message
            decoded_message = self.avro_utils.avro_decode(
                avro_message, 
                self.avro_utils.load_avro_schema(schema_path)
            )
            #print(decoded_message)
            
            

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON message received: {str(e)}")
        except Exception as e:
            self.logger.error(f"Message processing error: {str(e)}")

    def _on_error(self, ws, error: Exception) -> None:
        """
        Handle WebSocket errors.

        Args:
            ws: WebSocket instance
            error: Error that occurred
        """
        self.logger.error(f"WebSocket error occurred: {str(error)}")

    def _on_close(self, ws, close_status_code: int = None, close_msg: str = None) -> None:
        """
        Handle WebSocket connection closure.

        Args:
            ws: WebSocket instance
            close_status_code: Status code for connection closure
            close_msg: Closure message
        """
        close_info = f"Status: {close_status_code}, Message: {close_msg}" if close_status_code else "No status provided"
        self.logger.warning(f"WebSocket connection closed. {close_info}")

    def _on_open(self, ws) -> None:
        """
        Handle WebSocket connection opening.

        Args:
            ws: WebSocket instance
        """
        self.logger.info("WebSocket connection established")
        
        if self._subscribe_callback:
            try:
                self._subscribe_callback(ws)
            except Exception as e:
                self.logger.error(f"Error in subscription callback: {str(e)}")

    def run(self) -> None:
        """
        Run the WebSocket connection with automatic reconnection.
        """
        while True:
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_open=self._on_open
                )
                
                self.logger.info("Starting WebSocket connection...")
                self.ws.run_forever()
                
            except Exception as e:
                self.logger.error(f"WebSocket connection error: {str(e)}")
                self.logger.info(f"Reconnecting in {self.RECONNECTION_DELAY} seconds...")
                time.sleep(self.RECONNECTION_DELAY) 