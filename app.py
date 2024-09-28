from abc import ABC, abstractmethod
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import json

# Abstract Source Class
class Source(ABC):
    @abstractmethod
    def get_data(self) -> pd.DataFrame:
        pass

# Abstract Destination Class
class Destination(ABC):
    @abstractmethod
    def write_data(self, data: pd.DataFrame):
        pass

# FileSource class that stores the path
class FileSource(Source):
    def __init__(self, path: str):
        self.path = path

# CSVFileSource class that reads all CSV files from a directory and stores them in a pandas DataFrame
class CSVFileSource(FileSource):
    def __init__(self, path: str):
        super().__init__(path)
        self.data = self._read_all_csv_files()

    def _read_all_csv_files(self) -> pd.DataFrame:
        all_files = [f for f in os.listdir(self.path) if f.endswith('.csv')]
        dataframes = []
        for file in all_files:
            file_path = os.path.join(self.path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)
        return pd.concat(dataframes, ignore_index=True).drop_duplicates()

    def get_data(self) -> pd.DataFrame:
        return self.data



# PostgresDestination class that writes pandas DataFrame to PostgreSQL using dictionary-based credentials
class PostgresDestination(Destination):
    def __init__(self, credentials: dict, destination_table: str):
        super().__init__()
        self.credentials = credentials
        self.connection_string = self._create_connection_string()
        self.destination_table = destination_table

    def _create_connection_string(self) -> str:
        user = self.credentials.get('user')
        password = self.credentials.get('password')
        host = self.credentials.get('host', 'localhost')
        port = self.credentials.get('port', 5432)
        database = self.credentials.get('database')
        return f'postgresql://{user}:{password}@{host}:{port}/{database}'

    def write_data(self, data: pd.DataFrame):
        engine = create_engine(self.connection_string)
        data.to_sql(self.destination_table, engine, if_exists='replace', index=False)
        print("Data written to PostgreSQL")
        engine.dispose()


# Factory to create Source classes
class SourceFactory:
    @staticmethod
    def create_source(source_type: str, *args, **kwargs) -> Source:
        if source_type == "csv":
            return CSVFileSource(*args, **kwargs)
        else:
            raise ValueError(f"Unknown source type: {source_type}")

# Factory to create Destination classes
class DestinationFactory:
    @staticmethod
    def create_destination(destination_type: str, *args, **kwargs) -> Destination:
        if destination_type == "postgres":
            return PostgresDestination(*args, **kwargs)
        else:
            raise ValueError(f"Unknown destination type: {destination_type}")

# ETL class that manages the process
class ETL:
#    def __init__(self, source: Source, destination: Destination):
    def __init__(self):
        self.source = None
        self.destination = None
        self.configuration = get_configuration()
        self._set_members()
        
        
    def _set_members(self): 
        self.source=self.configuration["source"]["data"]
        self.source_type=self.configuration["source"]["type"]
        destination_credentials=self.configuration['destination']['credentials']
        destination_type=self.configuration['destination']['type']
        destination_table_name=self.configuration['destination']['destination_name']
        self.destination = DestinationFactory.create_destination(destination_type, credentials=destination_credentials, destination_table=destination_table_name)
        
    def run(self):
        source_data_dct={}
        # get data
        for k in self.source.keys():
            source_data_dct[k]=SourceFactory.create_source(self.source_type, path=self.source[k]).get_data()
        # table transformations
        for tables_t in self.configuration["transformations"]['tables']:
            table_name=tables_t["table_name"]
            transformations=tables_t["transformations"]
            for tr in transformations:
                source_data_dct[table_name]=eval(tr["name"])(source_data_dct[table_name], tr["parameters"])  
        # joins
        for join_t in self.configuration["transformations"]['join']:
            merged=source_data_dct[join_t["source_1"]].merge(source_data_dct[join_t["source_2"]], on=join_t["on"], how=join_t["how"]) 
        self.destination.write_data(merged)

        
def get_configuration(file_path = '/Users/amit/github/etl_task/config/conf.json'):
    with open(file_path, 'r') as file:
        conf = json.load(file)
    return(conf)
    

def rename(df, parameters):
    return(df.rename(columns=parameters))

def set_types(df, parameters):
    for k in parameters.keys():
        if (parameters[k]=="to_numeric"):
            df[k]=pd.to_numeric(df[k], errors='coerce')
            df=df.dropna(subset=[k])
        else:
            df[k]=df[k].astype(parameters[k])
    return(df)    
    
    
if __name__ == "__main__":
    etl = ETL()
    etl.run()