from typing import List
from datetime import datetime
import pytz

from .interfaces import TransactionReader, TransactionFilter, TransactionGrouper, TransactionWriter, TransactionMapper
from .io_ops import CsvReader, CsvWriter
from .filters import DateFilter
from .groupers import AccountGrouper
from .mappers import CsvMapper

class TransactionPipeline:
    def __init__(self, 
                 reader: TransactionReader, 
                 filters: List[TransactionFilter], 
                 grouper: TransactionGrouper,
                 mapper: TransactionMapper,
                 writer: TransactionWriter):
        self.reader = reader
        self.filters = filters
        self.grouper = grouper
        self.mapper = mapper
        self.writer = writer

    def run(self, input_path: str, output_dir: str):
        # 1. Read
        transactions = self.reader.read(input_path)
        
        # 2. Filter
        for f in self.filters:
            transactions = f.filter(transactions)
            
        # 3. Group
        grouped_txns = self.grouper.group(transactions)
        
        # 4. Map (convert Transaction objects to dicts for writing)
        grouped_data = {}
        for key, txns in grouped_txns.items():
            grouped_data[key] = [self.mapper.map(t) for t in txns]
            
        # 5. Write
        self.writer.write(grouped_data, output_dir)

def process_file(input_path: str, output_dir: str, target_timezone: str = 'Asia/Kolkata', since_date: str = None):
    # Setup components
    reader = CsvReader(target_timezone=target_timezone)
    
    filters = []
    if since_date:
        try:
            tz = pytz.timezone(target_timezone)
            since_dt = datetime.strptime(since_date, '%Y-%m-%d')
            since_dt = tz.localize(since_dt)
            print(f"Filtering transactions since {since_dt.isoformat()}")
            filters.append(DateFilter(since_date=since_dt))
        except Exception as e:
            print(f"Error parsing since_date: {e}")
            return

    grouper = AccountGrouper()
    mapper = CsvMapper()
    writer = CsvWriter()
    
    pipeline = TransactionPipeline(reader, filters, grouper, mapper, writer)
    pipeline.run(input_path, output_dir)

