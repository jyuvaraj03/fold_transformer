from abc import ABC, abstractmethod
from typing import List, Dict, Any, Iterable
from .models import Transaction

class TransactionReader(ABC):
    @abstractmethod
    def read(self, source: str) -> Iterable[dict]:
        """Reads raw transaction data from a source."""
        pass

class TransactionFilter(ABC):
    @abstractmethod
    def filter(self, transactions: Iterable[Transaction]) -> List[Transaction]:
        """Filters a list of transactions."""
        pass

class TransactionGrouper(ABC):
    @abstractmethod
    def group(self, transactions: List[Transaction]) -> Dict[str, List[Transaction]]:
        """Groups transactions by a key."""
        pass

class TransactionMapper(ABC):
    @abstractmethod
    def map(self, transaction: Transaction) -> Dict[str, Any]:
        """Maps a transaction to an output format."""
        pass

class TransactionWriter(ABC):
    @abstractmethod
    def write(self, grouped_data: Dict[str, List[Dict[str, Any]]], output_dir: str):
        """Writes processed data to the output directory."""
        pass
