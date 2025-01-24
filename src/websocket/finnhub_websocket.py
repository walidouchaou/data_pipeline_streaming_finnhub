import websocket
import json
from typing import Callable, List
import traceback
from ..utils.avro_utils import AvroUtils
import os

class FinnhubWebSocket:
    """Handle WebSocket connections and messages for Finnhub."""
    
    def __init__(self, ws_url: str, avro_utils: AvroUtils):
        self.ws_url = ws_url
        self.avro_utils = avro_utils
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self._subscribe_callback = None

    def set_subscribe_callback(self, callback: Callable[[websocket.WebSocketApp], None]) -> None:
        """Set callback for subscription on websocket open."""
        self._subscribe_callback = callback

    def on_message(self, ws, message) -> None:
        """Handle incoming WebSocket messages."""
        message = json.loads(message)
        schema_path = os.path.join(os.path.dirname(__file__), '..', 'schema', 'trades.avsc')
        avro_message = self.avro_utils.avro_encode(
            {
                'data': message['data'],
                'type': message['type']
            },
            self.avro_utils.load_avro_schema(schema_path)
        )
        print(self.avro_utils.avro_decode(
            avro_message, 
            self.avro_utils.load_avro_schema(schema_path)
        ))

    def on_error(self, ws, error) -> None:
        print(f"Erreur: {error}")
        print(traceback.format_exc())

    def on_close(self, ws) -> None:
        print("Connexion fermée")

    def on_open(self, ws) -> None:
        print("Connexion ouverte")
        if self._subscribe_callback:
            self._subscribe_callback(ws)

    def run(self) -> None:
        """Start WebSocket connection."""
        self.ws.run_forever() 