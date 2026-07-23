"""
Abstract Base Class for Message Serializer.
Provides contract for serializing/deserializing messages.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any


class MessageSerializerInterface(ABC):
    """
    Abstract interface for serializing messages.
    """
    
    @abstractmethod
    def serialize(self, data: Dict[str, Any]) -> bytes:
        """
        Serialize data to bytes.
        
        Args:
            data: Data to serialize
            
        Returns:
            Serialized bytes
        """
        pass
    
    @abstractmethod
    def deserialize(self, data: bytes) -> Dict[str, Any]:
        """
        Deserialize bytes to data.
        
        Args:
            data: Bytes to deserialize
            
        Returns:
            Deserialized data
        """
        pass
