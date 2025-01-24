import avro.schema
import avro.io
import io
import json
from typing import Dict, Any

class AvroEncodeError(Exception):
    pass

class AvroUtils:
    """Utilities for Avro encoding and decoding operations."""
    
    def avro_encode(self, data: Dict[str, Any], schema: avro.schema.Schema) -> bytes:
        """Encode data using Avro schema."""
        try:    
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            return bytes_writer.getvalue()
        except Exception as e:
            raise AvroEncodeError(f"Failed to encode data to Avro: {str(e)}")
    
    def load_avro_schema(self, schema_path: str) -> avro.schema.Schema:
        """Load and parse Avro schema from file."""
        return avro.schema.parse(open(schema_path).read())
    
    def avro_decode(self, bytes_data: bytes, schema: avro.schema.Schema) -> str:
        """Decode Avro bytes to JSON string."""
        bytes_reader = io.BytesIO(bytes_data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        return json.dumps(reader.read(decoder)) 