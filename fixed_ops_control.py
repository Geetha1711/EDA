import argparse
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv

from fixed_ops_helper import helper



load_dotenv(dotenv_path=".env")

class CustomError(Exception):
    pass

class FixedOps():

    def __init__(self, report_for, labor_type, service_type, start_date = None, end_date = None ):
        self.start_date = start_date
        self.end_date = end_date
        self.report_for = report_for
        self.labor_type = labor_type
        self.service_type = service_type
        self.ro_file_path = os.environ.get("RO_FILE_PATH", "ROLineItems_File.csv")
        self.work_days_file_path = os.environ.get("WORKING_DAYS_FILE","working_days.csv")
        self.pymt_method = os.environ.get("PAYMENT_METHOD", "payment method")
        self.srvc_method = os.environ.get("SERVICE_METHOD","service type")
        self.report_dir = os.environ.get("REPORT_DIR",'')
        self.helper = helper()

    '''Read the source files'''
    def read_files(self):
        try:
            ro_line_items = pd.read_csv(self.ro_file_path)
            working_days = pd.read_csv(self.work_days_file_path)
            working_days_list = working_days.to_dict(orient='records')
        except Exception as e:
            raise {"error_message": f"Exception while reading the file {str(e)}"}
        return ro_line_items, working_days_list

    '''Validate the input parameters and extract the data based on specifications'''
    def filter_df(self, ro_line_items):
        final_df = self.helper.get_df_by_date(ro_line_items, self.start_date, self.end_date)
        if labor_type.lower().strip() != 'all':
            final_df = final_df[final_df['Service_Group'].str.lower().str.strip() == labor_type.lower().strip()]
        if service_type.lower().strip() != 'all':
            final_df = final_df[final_df['Service_Type'].str.lower().str.strip() == service_type.lower().strip()]

        return final_df

    '''Get the payment report based on the specifications'''
    def get_payment_report(self, final_df):
        report_df = pd.DataFrame(columns=self.helper.report_cols_list)
        for dealer_group_name, dealer_group_df in final_df.groupby('Dealer_Name'):
            report_df = self.helper.groupby_helper(dealer_group_name, dealer_group_df, report_df)
            for service_advisor_name, service_advisor_df in dealer_group_df.groupby("Service_Advisor_Details"):
                report_df = self.helper.groupby_helper(service_advisor_name, service_advisor_df, report_df)
                for payment_method_name, payment_method_df in service_advisor_df.groupby("Payment_Method"):
                    report_df = self.helper.groupby_helper(payment_method_name, payment_method_df, report_df)
                    for ro_id_name, ro_df in payment_method_df.groupby('RO_ID'):
                        report_df = self.helper.groupby_helper(ro_id_name, ro_df, report_df,groupby_name='RO_ID')
        return report_df

    '''Get the service report based on the specifications'''
    def get_service_report(self, final_df):
        report_df = pd.DataFrame()
        for dealer_group_name, dealer_group_df in final_df.groupby('Dealer_Name'):
            report_df = self.helper.groupby_helper(dealer_group_name, dealer_group_df, report_df)

            for service_group, service_group_df in dealer_group_df.groupby("Service_Group"):
                report_df = self.helper.groupby_helper(service_group, service_group_df, report_df)

                for service_type, service_type_df in service_group_df.groupby("Service_Type"):
                    report_df = self.helper.groupby_helper(service_type, service_type_df, report_df)

                    for service_advisor_name, service_advisor_df in service_type_df.groupby("Service_Advisor_Details"):
                        report_df = self.helper.groupby_helper(service_advisor_name, service_advisor_df, report_df)

                        for payment_method_name, payment_method_df in service_advisor_df.groupby("Payment_Method"):
                            report_df = self.helper.groupby_helper(payment_method_name, payment_method_df, report_df)

                            for ro_id_name, ro_df in payment_method_df.groupby('RO_ID'):
                                report_df = self.helper.groupby_helper(ro_id_name, ro_df, report_df,groupby_name='RO_ID')
        return report_df

    '''Get the report type on the specifications'''
    def get_report(self, filter_df):
        report_df = pd.DataFrame()
        if self.report_for.lower().strip() == self.pymt_method and not filter_df.empty:
            report_df = self.get_payment_report(filter_df)
        elif report_for.lower().strip() == self.srvc_method and not filter_df.empty:
            report_df = self.get_service_report(filter_df)
        else:
            print("No data for the given specifications, kindly try for different specifications")

        '''write the report to the file'''
        if not report_df.empty:
            col_list = self.helper.report_cols_list
            col_list.remove('RO_Closed_Date')
            report_df = report_df[col_list]
            report_df['RO_ID'] = report_df['RO_ID'].astype(str)
            report_df = report_df.rename(columns={"RO_ID": 'Drill', "RO_Count": 'Closed ROs'})
            report_df = report_df.applymap(lambda x: '{:,.0f}'.format(x) if isinstance(x, (int, float)) and pd.notnull(x) else x)
            report_df = self.helper.add_prefix_suffix(report_df)
            report_df.columns = report_df.columns.str.replace('_', ' ')
            file_name = self.helper.get_file_name(self.report_for)
            with open(self.report_dir+file_name, 'w') as f:
                f.write(f'FrogData\n')
                f.write(f'Fixed Ops Overview by {self.report_for} for Demo\n')

            report_df.to_csv(self.report_dir+file_name,mode='a', index=False)
            print(f"The {self.report_for} report was saved in the mentioned directory")

    '''Pipeline to run the program'''
    def pipeline(self):
        #read input files
        ro_line_items, self.helper.working_days_list = self.read_files()
        #filter dataframe based on input parameters
        filter_df = self.filter_df(ro_line_items)
        self.get_report(filter_df)


'''Main function to parse the arguments and run the program'''
def parse_arguments():
    # Add arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", "--start_date", default=None, help="All data on and after this data is considered.")
    parser.add_argument("-ed", "--end_date", default=None, help="All data on and before this data is considered.")
    parser.add_argument("-r", "--report_for", default='Payment Method',
                        help='Used to specify which table we want ')
    parser.add_argument("-p", "--labor_type", default='All',
                        help="If specific labor type is specified then filter report to only include this labor type.")
    parser.add_argument("-s", "--service_type", default='All',
                        help="If specific service type is specified then "
                             "filter report to only include this service type.")

    # Parse arguments.
    args = parser.parse_args()
    start_date = args.__getattribute__("start_date")
    end_date = args.__getattribute__("end_date")
    report_for = args.__getattribute__("report_for")
    labor_type = args.__getattribute__("labor_type")
    service_type = args.__getattribute__("service_type")

    if report_for not in ['Payment Method', 'Service Type']:
        raise CustomError("Incorrect report specified. Currently, we support reports for 'Payment Method' and "
                          "'Service Type'")

    return start_date, end_date, report_for, labor_type, service_type

'''Main function to run the program'''
if __name__=="__main__":
    start_date, end_date, report_for, labor_type, service_type = parse_arguments()
    print("start_date, end_date, report_for, labor_type, service_type ", start_date, end_date, report_for, labor_type, service_type )
    fixed_ops = FixedOps(report_for, labor_type, service_type, start_date, end_date)
    fixed_ops.pipeline()


# start_date = "2023-03-19"
# end_date = "2023-12-01"
# labor_type= 'ot'
# service_type = 'Mechanical'
# report_for = 'payment method'
#run the pgm in below format
#python3 fixed_ops_control.py -sd 2023-03-19 -ed 2023-12-01 -r "Payment Method" -p "OT" -s "Mechanical"