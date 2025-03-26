import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


class EnvDetails:
    def __init__(self):
        ## Ini File
        self.__configObj = self.initiate_ini()
        self.__getIniConfigDetails()
        ## .env File
        # self.__getEnvConfigDetails()
       
    ## Since dotenv is not availabe in my system, I used .ini file for development. 
    @staticmethod
    def initiate_ini() -> None:
        import configparser
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), "env.ini"))
        return config["source_db_config_details"]
    
    def __getIniConfigDetails(self) -> None:
        dbHost = self.__configObj["hostname"]
        dbPort = self.__configObj["port"]
        dbUser = self.__configObj["username"]
        dbPassword = self.__configObj["password"]
        dbName = self.__configObj["dbname"]
        schema = self.__configObj["schema"]
        self.config_table = f'"{schema}"."{self.__configObj["configDB"]}"'
        self.report_dtls_table = f'"{schema}"."{self.__configObj["reportDetailsDB"]}"'
        self.audit_table = f'"{schema}"."{self.__configObj["auditDB"]}"'
        self.connectionURL = f'postgresql://{dbUser}:{dbPassword}@{dbHost}:{dbPort}/{dbName}'
        print(f"""
        self.connectionURL: {self.connectionURL}
        self.config_table: {self.config_table}
        self.report_dtls_table: {self.report_dtls_table}
        self.audit_table: {self.audit_table}
        """)
    
    ## Comment Started
    '''    
    def __getEnvConfigDetails(self) -> None:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), "env.ini"))
        
        dbHost = os.getenv["hostname"]
        dbPort = os.getenv["port"]
        dbUser = os.getenv["username"]
        dbPassword = os.getenv["password"]
        dbName = os.getenv["dbname"]
        schema = os.getenv["schema"]
        self.config_table = f'"{schema}"."{os.getenv["configDB"]}"'
        self.report_dtls_table = f'"{schema}"."{os.getenv["reportDetailsDB"]}"'
        self.audit_table = f'"{schema}"."{os.getenv["auditDB"]}"'
        self.connectionURL = f'postgresql://{dbUser}:{dbPassword}@{dbHost}:{dbPort}/{dbName}'
        print(f"""
        self.connectionURL: {self.connectionURL}
        self.config_table: {self.config_table}
        self.report_dtls_table: {self.report_dtls_table}
        self.audit_table: {self.audit_table}
        """)       
    ''' 
    ## Comment Ended 
    
    
class ConfigDetails(EnvDetails):
    def __init__(self):
        EnvDetails.__init__(self)
        self.connectConfigDB()

    def connectConfigDB(self) -> None:
        try:
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            
            # print(f"self.connectionURL: {self.connectionURL}" )
            self.engine = create_engine(self.connectionURL)
            session = sessionmaker(bind=self.engine)
            session()
        except Exception as err:
            print(f"Error in Connecting to DB. Error: {err}")      
        
    def db_client(self, sql_query:str) -> pd.DataFrame:
        """DB Client to read data from SQL and returns DataFrame"""
        try:
            self.engine.dispose()
            with self.engine.connect() as conn:
                df_db = pd.read_sql(sql_query, conn)
                print(df_db.head(10))
                print(df_db.columns)
                return df_db 
        except Exception as err:
            print(f"Error in reading data. Error: {err}")
        return pd.DataFrame()
    
    def db_dml_client(self, sql_query:str) -> pd.DataFrame:
        """DB Client to read data from SQL and returns DataFrame"""
        try:
            self.engine.dispose()
            with self.engine.connect() as conn:
                df_db = pd.read_sql(sql_query, conn)
        except Exception as err:
            print(f"Error in DML operation. Error: {err}")

    def read_config_data(self):
        """Reads the Common Configuration Data from Table"""
        query = f"""
        select * from {self.config_table}
        """
        dfConfig = self.db_client(sql_query=query)
        return dfConfig
    
    def read_report_config_data(self):
        """Reads the Report details from Table"""
        query = f"""
        select * from {self.report_dtls_table} 
        """
        dfReports = self.db_client(sql_query=query)
        return dfReports
    
    def read_audit_data(self):
        """Reads the Report Audit Date for processing it"""
        from datetime import datetime
        query = f"""
        select * from {self.audit_table} 
        where date(load_date) = '{datetime.now().strftime('%Y-%m-%d')}'
        """
        dfAudit = self.db_client(sql_query=query)
        return dfAudit
        
    
## Created for Testing Purpose
def test_main():
    obj = ConfigDetails() #.config()
    df1 = obj.read_config_data()
    df2 = obj.read_report_config_data()
    df3 = obj.read_audit_data()
    
# if __name__ == "__main__":
#     test_main()
    

