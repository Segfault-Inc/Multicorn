from kalamar.storage.alchemy import AlchemyAccessPoint


class PostgresAlchemyAccessPoint(AlchemyAccessPoint):
    protocol = "alchemy-postgres"

    def __init__(self,config):
        super(PostgresAlchemyAccessPoint,self).__init__(config)



