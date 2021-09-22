from typing import List
from abc import ABC, abstractmethod

class Logger(ABC):
    @abstractmethod
    def log(self, message):
        pass
    
class Mapper(ABC):
        @abstractmethod
        def map(self, fields: List[str]):
            pass

class Validator(ABC):
    def __init__(self, logger: Logger):
        self.logger = logger

    @abstractmethod
    def validate(self, fields: List[str], record_line: int) -> bool:
        pass
    
class Parser(ABC):
    def __init__(self, validator: Validator, mapper: Mapper):
        self.validator = validator
        self.mapper = mapper

    def parse_trades(self, trade_data: List[str]):
        trades: T = []
        for index, record in enumerate(trade_data):
            fields = self.parse(record)
            if not self.validator.validate(fields, index + 1):
                continue
            trade = self.mapper.map(fields)
            trades.append(trade)
        return trades

    @abstractmethod
    def parse(self, record: str) -> List[str]:
        pass


class Repository(ABC):
    def __init__(self, logger: Logger):
        self.logger = logger

    @abstractmethod
    def store_records(self, records) -> None:
        pass

class Reader(ABC):
    @abstractmethod
    def read_data(self) -> List[str]:
        pass