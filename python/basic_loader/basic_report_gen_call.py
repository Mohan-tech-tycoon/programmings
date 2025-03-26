from common_utils import ConfigDetails
import pandas as pd


class ReportExecution(ConfigDetails):
    # def __init__(self):
    #     ConfigDetails.__init__(self)
        
    def get_report_config_details(self) -> pd.DataFrame:
        df_config_data = self.read_config_data()
        df_report_details = self.read_report_config_data()
        df_audit_datails = self.read_audit_data()
        
    def request_report_api_call(self):
        """ From Audit Info Table - Joined the Agend Code and Report ID to retrieve the Request URL"""
        df_report_details = self.read_report_config_data()
        df_audit_datails = self.read_audit_data()
        for rpt in df_report_details.itertuples():
            dfval = df_audit_datails[df_report_details.query(f"agent_code=='{rpt.agent_code}' and report_id=='{rpt.report_id}'")]
            print(dfval)
            
        
if __name__ == "__main__":
    rpt = ReportExecution()
    rpt.request_report_api_call()
    
