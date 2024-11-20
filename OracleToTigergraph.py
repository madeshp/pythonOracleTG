import cx_Oracle
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging
import configparser
from typing import Iterator, List, Tuple
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class OracleToTigerGraphPipeline:
    def __init__(self, config_path: str):
        """Initialize the ETL pipeline with configuration"""
        self.config = self._load_config(config_path)
        self.spark = self._initialize_spark()
        
    def _load_config(self, config_path: str) -> configparser.ConfigParser:
        """Load configuration from file"""
        config = configparser.ConfigParser()
        config.read(config_path)
        return config

    def _initialize_spark(self) -> SparkSession:
        """Initialize Spark session with TigerGraph connector"""
        return (SparkSession.builder
                .appName("OracleToTigerGraphETL")
                .config("spark.jars", self.config['TigerGraph']['connector_jar'])
                .getOrCreate())

    def _get_oracle_connection(self):
        """Create Oracle database connection"""
        try:
            connection = cx_Oracle.connect(
                user=self.config['Oracle']['user'],
                password=self.config['Oracle']['password'],
                dsn=self.config['Oracle']['dsn']
            )
            return connection
        except Exception as e:
            logging.error(f"Failed to connect to Oracle: {str(e)}")
            raise

    def _fetch_data_in_chunks(self, chunk_size: int = 100000) -> Iterator[Tuple[pd.DataFrame, List]]:
        """Fetch data from Oracle in chunks using server-side cursor"""
        # Modified query to only fetch unprocessed records
        query = f"""
            SELECT *
            FROM {self.config['Oracle']['table_name']}
            WHERE tgload IS NULL
            ORDER BY {self.config['Oracle']['primary_key']}
        """
        
        try:
            with self._get_oracle_connection() as connection:
                cursor = connection.cursor()
                cursor.arraysize = chunk_size
                cursor.execute(query)
                
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                        
                    # Extract primary key values for updating status later
                    column_names = [desc[0] for desc in cursor.description]
                    pk_index = column_names.index(self.config['Oracle']['primary_key'])
                    pk_values = [row[pk_index] for row in rows]
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(rows, columns=column_names)
                    yield df, pk_values
                    
        except Exception as e:
            logging.error(f"Error fetching data: {str(e)}")
            raise

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the data - combine specified columns into comma-separated values"""
        try:
            # Get columns to combine from config
            columns_to_combine = self.config['Transform']['columns_to_combine'].split(',')
            
            # Create new column with comma-separated values
            df['combined_values'] = df[columns_to_combine].apply(
                lambda x: ','.join(str(val) for val in x if pd.notna(val)), axis=1
            )
            
            return df
        except Exception as e:
            logging.error(f"Error transforming data: {str(e)}")
            raise

    def load_to_tigergraph(self, df: pd.DataFrame) -> bool:
        """Load data into TigerGraph using Spark connector"""
        try:
            # Convert pandas DataFrame to Spark DataFrame
            spark_df = self.spark.createDataFrame(df)
            
            # Write to TigerGraph
            (spark_df.write
             .format("org.apache.spark.tigergraph.connector")
             .option("graph", self.config['TigerGraph']['graph_name'])
             .option("vertex.label", self.config['TigerGraph']['vertex_label'])
             .option("url", self.config['TigerGraph']['url'])
             .option("username", self.config['TigerGraph']['username'])
             .option("password", self.config['TigerGraph']['password'])
             .mode("append")
             .save())
            
            return True
            
        except Exception as e:
            logging.error(f"Error loading data to TigerGraph: {str(e)}")
            return False

    def update_oracle_status(self, pk_values: List, connection) -> bool:
        """Update the status of processed records in Oracle"""
        try:
            cursor = connection.cursor()
            
            # Prepare the update statement
            update_sql = f"""
                UPDATE {self.config['Oracle']['table_name']}
                SET tgload = :1
                WHERE {self.config['Oracle']['primary_key']} IN ({','.join([':' + str(i+2) for i in range(len(pk_values))])})
            """
            
            # Current timestamp for the update
            timestamp = datetime.now()
            
            # Execute the update
            cursor.execute(update_sql, [timestamp] + pk_values)
            connection.commit()
            
            logging.info(f"Updated status for {len(pk_values)} records in Oracle")
            return True
            
        except Exception as e:
            logging.error(f"Error updating Oracle status: {str(e)}")
            connection.rollback()
            return False
        finally:
            cursor.close()

    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        try:
            total_records = 0
            chunk_size = int(self.config['Pipeline']['chunk_size'])
            
            logging.info("Starting ETL pipeline...")
            
            # Use a single connection for all updates
            with self._get_oracle_connection() as connection:
                for chunk_df, pk_values in self._fetch_data_in_chunks(chunk_size):
                    records_in_chunk = len(chunk_df)
                    logging.info(f"Processing chunk with {records_in_chunk} records...")
                    
                    # Transform the data
                    transformed_df = self.transform_data(chunk_df)
                    
                    # Load to TigerGraph
                    if self.load_to_tigergraph(transformed_df):
                        # Update Oracle status only if TigerGraph load was successful
                        if self.update_oracle_status(pk_values, connection):
                            total_records += records_in_chunk
                            logging.info(f"Successfully processed chunk. Total records processed: {total_records}")
                        else:
                            logging.error("Failed to update Oracle status for chunk")
                    else:
                        logging.error("Failed to load chunk to TigerGraph")
            
            logging.info(f"ETL pipeline completed. Total records processed: {total_records}")
            
        except Exception as e:
            logging.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    # Example configuration file (config.ini):
    """
    [Oracle]
    user = your_username
    password = your_password
    dsn = your_dsn
    table_name = your_table
    primary_key = id_column

    [TigerGraph]
    connector_jar = path/to/tigergraph-connector.jar
    graph_name = your_graph
    vertex_label = your_vertex_label
    url = your_tigergraph_url
    username = your_tigergraph_username
    password = your_tigergraph_password

    [Transform]
    columns_to_combine = col1,col2,col3

    [Pipeline]
    chunk_size = 100000
    """
    
    pipeline = OracleToTigerGraphPipeline('config.ini')
    pipeline.run_pipeline()
