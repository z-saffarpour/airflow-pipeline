"""
Pipeline Interfaces Package - Abstract Base Classes
====================================================
Contains all abstract interfaces for dependency inversion.
"""

from pipeline.interfaces.DataReader import DataReader
from pipeline.interfaces.MessageProducer import MessageProducer
from pipeline.interfaces.TopicManager import TopicManager
from pipeline.interfaces.MessageSerializerInterface import MessageSerializerInterface
from pipeline.interfaces.DatabaseOptimizer import DatabaseOptimizer

__all__ = [
    "DataReader",
    "MessageProducer",
    "TopicManager",
    "MessageSerializerInterface",
    "DatabaseOptimizer",
]
