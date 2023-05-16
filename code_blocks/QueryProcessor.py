from sqlite3 import connect
from pandas import read_sql, DataFrame
from utils import RDF_DB_URL, SQL_DB_URL
from rdflib import Graph, Literal, URIRef
from processor import Processor
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get 

class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        entityId_stripped = entityId.strip("'")
        db_url = self.getDbPathOrUrl() if len(self.getDbPathOrUrl()) else SQL_DB_URL
        df = DataFrame()
        if db_url == SQL_DB_URL:
            with connect(db_url) as con:
                query = \
                "SELECT *" +\
                " FROM Entity" +\
                " LEFT JOIN Annotation" +\
                " ON Entity.id = Annotation.target" +\
                " WHERE 1=1" +\
                f" AND Entity.id='{entityId_stripped}'"
                df = read_sql(query, con)

        elif db_url == RDF_DB_URL:
            endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
            query = """
                PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
                PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
                PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

                SELECT ?entity
                WHERE {
                    ?entity ns2:identifier "%s" .
                }
                """ % entityId 

            df = get(endpoint, query, True)
            return df
        return df
