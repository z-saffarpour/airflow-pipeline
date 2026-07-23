"""
Message Serializer for converting data to Kafka message format.
Handles serialization logic separately from business logic.
"""
import json
from typing import Any, Dict, List, Optional
from datetime import datetime, date,time
from decimal import Decimal


class MessageSerializer:
    """
    Serializes data for Kafka messages.
    Follows Single Responsibility Principle by handling only serialization.
    """

    @staticmethod
    def serialize_row(row: Dict[str, Any]) -> bytes:
        """
        Serialize a single row to JSON bytes for Kafka.

        Args:
            row: Dictionary representing a database row

        Returns:
            JSON bytes ready for Kafka producer
        """
        # Convert row to JSON-serializable format
        serializable_row = MessageSerializer._make_json_serializable(row)
        
        # Convert to JSON string and encode to bytes
        json_str = json.dumps(serializable_row, ensure_ascii=False)
        return json_str.encode('utf-8')

    @staticmethod
    def serialize_batch(rows: List[Dict[str, Any]]) -> List[bytes]:
        """
        Serialize multiple rows to JSON bytes.

        Args:
            rows: List of dictionaries representing database rows

        Returns:
            List of JSON bytes ready for Kafka producer
        """
        return [MessageSerializer.serialize_row(row) for row in rows]

    @staticmethod
    def deserialize_row(payload: bytes) -> Dict[str, Any]:
        """
        Deserialize a Kafka message value (JSON UTF-8) to a row dict.

        Args:
            payload: Message value bytes produced by serialize_row

        Returns:
            Dictionary representing a database row

        Raises:
            ValueError: If payload is empty or not a JSON object
        """
        if payload is None:
            raise ValueError("Cannot deserialize None Kafka payload")
        if isinstance(payload, memoryview):
            payload = payload.tobytes()
        if isinstance(payload, bytearray):
            payload = bytes(payload)
        if not isinstance(payload, (bytes, str)):
            raise TypeError(
                f"Expected bytes/str Kafka payload, got {type(payload).__name__}"
            )

        raw = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError(
                f"Kafka message value must be a JSON object, got {type(data).__name__}"
            )
        return data

    @staticmethod
    def deserialize_batch(payloads: List[bytes]) -> List[Dict[str, Any]]:
        """Deserialize multiple Kafka message values to row dicts."""
        return [MessageSerializer.deserialize_row(payload) for payload in payloads]

    @staticmethod
    def _make_json_serializable(obj: Any) -> Any:
        """
        Convert an object to JSON-serializable format.
        Handles special types like datetime, date, Decimal, bytes.

        Args:
            obj: Object to convert

        Returns:
            JSON-serializable version of the object
        """
        if isinstance(obj, dict):
            return {key: MessageSerializer._make_json_serializable(value) 
                    for key, value in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [MessageSerializer._make_json_serializable(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, time):
            return obj.strftime("%H:%M:%S")
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        elif obj is None:
            return None
        else:
            return obj

    @staticmethod
    def create_message_key(row: Dict[str, Any], key_column: str) -> Optional[bytes]:
        """
        Create a Kafka message key from a row.

        Args:
            row: Dictionary representing a database row
            key_column: Column name to use as key

        Returns:
            Message key as bytes, or None if key_column not in row
        """
        if key_column not in row:
            return None
        
        key_value = row[key_column]
        if key_value is None:
            return None
        
        # Convert to string and encode
        return str(key_value).encode('utf-8')

    @staticmethod
    def create_headers(
        source_table: str,
        execution_date: str,
        batch_number: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Create Kafka message headers.

        Args:
            source_table: Name of the source table
            execution_date: Execution date in YYYYMMDD format
            batch_number: Optional batch number

        Returns:
            Dictionary of headers
        """
        headers = {
            'source_table': source_table,
            'execution_date': execution_date,
            'timestamp': datetime.utcnow().isoformat(),
        }
        
        if batch_number is not None:
            headers['batch_number'] = str(batch_number)
        
        return headers
