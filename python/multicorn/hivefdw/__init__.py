from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG

from hive_service import ThriftHive
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class HiveForeignDataWrapper(ForeignDataWrapper):
    """
    Hive FDW for PostgreSQL
    """
    
    def __init__(self, options, columns):
        super(HiveForeignDataWrapper, self).__init__(options, columns)
        if 'host' not in options:
            log_to_postgres('The host parameter is required and the default is localhost.', WARNING)
        self.host = options.get("host", "localhost")
        
        if 'port' not in options:
            log_to_postgres('The host parameter is required and the default is 10000.', WARNING)
        self.port = options.get("port", "10000")
        
        if 'table' and 'query' not in options:
            log_to_postgres('table or query parameter is required.', ERROR)
        self.table = options.get("table", None)
        self.query = options.get("query", None)
            
        self.columns = columns

    def execute(self, quals, columns):
        if self.query:
            statement = self.query
        else:
            statement = "SELECT " + ",".join(self.columns.keys()) + " FROM " + self.table
        
        log_to_postgres('Hive query: ' + unicode(statement), DEBUG)
        
        try:
            transport = TSocket.TSocket(self.host, self.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = ThriftHive.Client(protocol)
            transport.open()
            
            client.execute(statement)
            
            for row in client.fetchAll():
                line = {}
                cols = row.split("\t");
                idx = 0
                for column_name in self.columns:
                    line[column_name] = cols[idx]
                    idx = idx + 1
                yield line
                    
        except Thrift.TException, tx:
            log_to_postgres(tx.message, ERROR)
        finally:
            transport.close()
    
