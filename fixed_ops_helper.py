import os
import datetime
import numpy as np
from dotenv import load_dotenv
import pandas as pd

load_dotenv(dotenv_path=".env")

'''Helper class for Fixed Ops Report'''
class FixedOPSHelper():

    def __init__(self):
        self.file_name_prefix = os.environ.get("FILE_NAME_PREFIX","Fixed_Ops_Overview_by_")
        self.working_days_list = []
        self.total_sales_src_cols_list = ['Labor_Sale', 'Misc_Sale', 'Parts_Sale', 'Sublet_Sale']
        self.total_cost_src_cols_list = ['Labor_Cost', 'Misc_Cost', 'Parts_Cost', 'Sublet_Cost']
        self.discounts_src_cols_list = ['Labor_Discount', 'Misc_Discount', 'Parts_Discount', 'Sublet_Discount']
        self.report_cols_list = ['RO_ID', 'RO_Closed_Date', 'RO_Count', 'RO/Day', 'Day/RO', 'Total_Sale',
                                 'Total_Gross', 'Gross/RO', 'Labor_Hours', 'Hours/RO', 'ELR',
                                 'Labor_Sale', 'Labor_Gross', 'Parts_Sale', 'Parts_Gross', 'Misc_Sale',
                                 'Discounts', 'Lbr_Gr_%', 'Pts_Gr_%', 'Pt/Lb_Sale', 'Open_ROs']
        self.prefix_suffix_dict = {
            "$":['Total_Sale', 'Labor_Sale', 'Labor_Gross', 'Parts_Sale', 'Parts_Gross', 'Discounts', 'Total_Gross', 'Gross/RO'],
            "%":['Lbr_Gr_%', 'Pts_Gr_%', 'Pt/Lb_Sale']
        }

    '''extract data based on date range'''
    def get_df_by_date(self, final_df, start_date=None, end_date=None):
        if start_date is not None and end_date is not None:
            final_df = final_df[(final_df['RO_Closed_Date'] >= start_date) & (final_df['RO_Closed_Date'] <= end_date)]
        elif start_date is not None:
            final_df = final_df[final_df['RO_Closed_Date'] >= start_date]
        elif end_date is not None:
            final_df = final_df[final_df['RO_Closed_Date'] <= end_date]
        else:
            final_df = final_df
        return final_df

    '''frame the file name based on report_for'''
    def get_file_name(self, report_for):
        file_name_suffix = datetime.datetime.today().strftime('%Y_%m_%d') + ".csv"
        file_name_middle = report_for.replace(' ', '_')
        file_name = self.file_name_prefix + file_name_middle + "_" + file_name_suffix
        return file_name

    '''calculate sum of columns'''
    def get_column_sum(self, lst):
        return round(sum(filter(lambda x: not np.isnan(x), lst)),2)

    '''calculate RO Working Days'''
    def get_working_days(self, item):
        start_date = datetime.datetime.strptime(item['RO_Open_Date'], '%Y-%m-%d')
        end_date = datetime.datetime.strptime(item['RO_Closed_Date'], '%Y-%m-%d')
        contribution = sum(self.working_days_list[(start_date + datetime.timedelta(day)).weekday()]['Contribution'] for day in
            range((end_date - start_date).days + 1))
        return contribution

    '''calculate Aggregates'''
    def groupby_helper(self, group_name, group_df, report_df,groupby_name=None):
        temp_df = pd.DataFrame(columns=self.report_cols_list)
        temp_df.loc[0, 'RO_ID'] = group_name
        temp_df.loc[0, 'RO_Count'] = group_df.loc[group_df['RO_Closed_Date'].notnull(), 'RO_ID'].nunique()
        if groupby_name and groupby_name == 'RO_ID':
            temp_df = self.calc_aggregates(group_df)
            report_df = pd.concat([report_df, temp_df[self.report_cols_list]])
        else:
            temp_df = self.calc_groupby_aggregates(temp_df, group_df)
            report_df = pd.concat([report_df, temp_df])
        del temp_df
        return report_df

    '''calculate Series Sum'''
    def series_sum(self, input_df, keys_list, output_df):
        for key in keys_list:
            output_df.loc[0,key] =  input_df[key].sum(skipna=True).round(2)
        return output_df


    '''calculate Basic Mathematical Operations'''
    def basic_operations(self, df, key_list):
        for key in key_list:
            if key.get('operation') == 'diff':
                df.loc[0,key.get('output_key')] = round(df.loc[0,key.get('input_keys')[0]] - df.loc[0,key.get('input_keys')][1],2)
            elif key.get('operation') == 'percentage_ratio':
                df.loc[0,key.get('output_key')] = round(df.loc[0,key.get('input_keys')[0]] * 100 /df.loc[0,key.get('input_keys')][1],2) if df.loc[0, key.get('input_keys')[0]] and df.loc[0, key.get('input_keys')[1]] else np.nan
            elif key.get('operation') == 'ratio':
                df.loc[0,key.get('output_key')] = round(df.loc[0,key.get('input_keys')[0]]/
                df.loc[0,key.get('input_keys')][1],2) if df.loc[0, key.get('input_keys')[0]] and df.loc[0, key.get('input_keys')[1]] else np.nan

        return df

    '''calculate group by Aggregates'''
    def calc_groupby_aggregates(self, temp_df, payment_method_df):
        sum_keys_list = ['Labor_Sale', 'Parts_Sale', 'Misc_Sale', 'Sublet_Sale', 'Labor_Cost', 'Parts_Cost', 'Misc_Cost','Sublet_Cost','Labor_Hours','Labor_Discount', 'Parts_Discount', 'Misc_Discount', 'Sublet_Discount']
        key_list = [{"input_keys":["Total_Sale","Total_Cost"], "output_key":"Total_Gross", "operation":"diff"},
                    {"input_keys":["Labor_Sale","Labor_Cost"], "output_key":"Labor_Gross", "operation":"diff"},
                    {"input_keys":["Parts_Sale","Parts_Cost"], "output_key":"Parts_Gross", "operation":"diff"},
                    {"input_keys":["Misc_Sale","Misc_Cost"], "output_key":"Misc_Gross", "operation":"diff"},
                    {"input_keys":["Sublet_Sale","Sublet_Cost"], "output_key":"Sublet_Gross", "operation":"diff"},
                    {"input_keys":["Labor_Gross","Labor_Sale"], "output_key":"Lbr_Gr_%", "operation":"percentage_ratio"},
                    {"input_keys":["Parts_Gross","Parts_Sale"], "output_key":"Pts_Gr_%", "operation":"percentage_ratio"},
                    {"input_keys":["Parts_Sale","Labor_Sale"], "output_key":"Pt/Lb_Sale", "operation":"percentage_ratio"},
                    {"input_keys":["Labor_Sale","Labor_Hours"], "output_key":"ELR", "operation":"ratio"},
                    {"input_keys":["RO_Count","No_Work_Days"], "output_key":"RO/Day", "operation":"ratio"}]

        temp_df = self.series_sum(payment_method_df, sum_keys_list, temp_df)
        temp_df.loc[0, 'No_Work_Days'] = round(
            payment_method_df[['RO_Closed_Date', 'RO_Open_Date']].apply(self.get_working_days, axis=1).sum(), 2)
        temp_df.loc[0, 'Total_Sale'] = round(temp_df.loc[0, self.total_sales_src_cols_list].sum(skipna=True), 2)
        temp_df.loc[0, 'Total_Cost'] = round(temp_df.loc[0, self.total_cost_src_cols_list].sum(skipna=True), 2)
        temp_df.loc[0, 'Total_Discount'] = round(temp_df.loc[0, self.discounts_src_cols_list].sum(skipna=True), 2)
        temp_df = self.basic_operations(temp_df, key_list)
        temp_df.loc[0, 'Day/RO'] = round(temp_df.loc[0, 'No_Work_Days'] / temp_df.loc[0, 'RO_Count'], 2) if temp_df.loc[0, 'RO_Count'] and temp_df.loc[0, 'No_Work_Days'] else np.nan
        temp_df.loc[0, 'Open_ROs'] = payment_method_df.loc[payment_method_df['RO_Closed_Date'] == '', 'RO_ID'].nunique()
        temp_df.loc[0, 'Gross/RO'] = round(temp_df.loc[0, 'Total_Gross'] / temp_df.loc[0, 'RO_Count'], 2) if temp_df.loc[
        0, 'RO_Count'] and temp_df.loc[0, 'RO_Count'] else np.nan
        temp_df.loc[0, 'Hours/RO'] = round(temp_df.loc[0, 'Labor_Hours'] / temp_df.loc[0, 'RO_Count'], 2) if temp_df.loc[ 0, 'Labor_Hours'] and temp_df.loc[0, 'RO_Count'] else np.nan
        temp_df.loc[0, 'Discounts'] = round(temp_df.loc[0, self.discounts_src_cols_list].sum(skipna=True), 2) #self.aggregate_sum(temp_df, 'Discounts', self.discounts_src_cols_list)#

        return temp_df

    '''calculate aggregates'''
    def calc_aggregates(self, payment_method):
        payment_method['RO_Count'] = payment_method[['RO_Closed_Date']].apply(
            lambda row: 1 if row['RO_Closed_Date'] else 0, axis=1)
        payment_method['No_Work_Days'] = payment_method[['RO_Closed_Date', 'RO_Open_Date']].apply(
            self.get_working_days, axis=1)
        payment_method['RO/Day'] = payment_method[['RO_Count', 'No_Work_Days']].apply(
            lambda x: round(x['RO_Count'] / x['No_Work_Days'], 2) if x['RO_Count'] and x['No_Work_Days'] else np.nan,
            axis=1)
        payment_method['Day/RO'] = payment_method[['RO_Count', 'No_Work_Days']].apply(
            lambda x: round(x['No_Work_Days'] / x['RO_Count'], 2) if x['RO_Count'] and x['No_Work_Days'] else np.nan,
            axis=1)
        payment_method['Total_Sale'] = payment_method[self.total_sales_src_cols_list].apply(lambda x: self.get_column_sum(x), axis=1)
        payment_method['Total_Cost'] = payment_method[self.total_cost_src_cols_list].apply(lambda x: self.get_column_sum(x), axis=1)
        payment_method['Total_Gross'] = payment_method[['Total_Sale', 'Total_Cost']].apply(lambda x: round(x['Total_Sale'] - x['Total_Cost'], 2), axis=1)
        payment_method['Gross/RO'] = payment_method[['Total_Gross', 'RO_Count']].apply(lambda x: round((x['Total_Gross'] / x['RO_Count']), 2) if x['Total_Gross'] and x['RO_Count'] else np.nan,axis=1)
        payment_method['Hours/RO'] = payment_method[['Labor_Hours', 'RO_Count']].apply(lambda x: x['Labor_Hours'] / x['RO_Count'] if x['Labor_Hours'] and x['RO_Count'] else np.nan, axis=1)
        payment_method['ELR'] = payment_method[['Labor_Sale', 'Labor_Hours']].apply(lambda x: round(x['Labor_Sale'] / x['Labor_Hours'], 2) if x['Labor_Hours'] and x['Labor_Sale'] else np.nan, axis=1)
        payment_method['Discounts'] = payment_method[self.discounts_src_cols_list].apply(lambda x: self.get_column_sum(x), axis=1)
        payment_method['Labor_Gross'] = payment_method[['Labor_Sale', 'Labor_Cost']].apply(lambda x: round(x['Labor_Sale'] - x['Labor_Cost'], 2), axis=1)
        payment_method['Lbr_Gr_%'] = payment_method[['Labor_Sale', 'Labor_Gross']].apply(lambda x: round((x['Labor_Gross'] * 100 / x['Labor_Sale']), 2) if x['Labor_Gross'] and x['Labor_Sale'] else np.nan, axis=1)
        payment_method['Parts_Gross'] = payment_method[['Parts_Sale', 'Parts_Cost']].apply(lambda x: round(x['Parts_Sale'] - x['Parts_Cost'], 2), axis=1)
        payment_method['Pts_Gr_%'] = payment_method[['Parts_Sale', 'Parts_Gross']].apply(lambda x: round((x['Parts_Gross'] * 100 / x['Parts_Sale']), 2) if x['Parts_Gross'] and x['Parts_Sale'] else np.nan, axis=1)
        payment_method['Pt/Lb_Sale'] = payment_method[['Parts_Sale', 'Labor_Sale']].apply(lambda x: round((x['Parts_Sale'] * 100 / x['Labor_Sale']), 2) if x['Parts_Sale'] and x['Labor_Sale'] else np.nan, axis=1)
        payment_method['Open_ROs'] = payment_method[['RO_Closed_Date']].apply(lambda x: 1 if x['RO_Closed_Date'] == '' else np.nan, axis=1)

        return payment_method

    '''prefix suffix for columns'''
    def add_prefix_suffix(self,report_df):
        for prefix_suffix, columns in self.prefix_suffix_dict.items():
            if prefix_suffix == '$':
                for col in columns:
                    report_df[col] = report_df[col].apply(lambda x: '$' + str(x) if x and pd.notnull(x) else x)
            elif prefix_suffix == '%':
                for col in columns:
                    report_df[col] = report_df[col].apply(lambda x: str(x) + '%' if x and pd.notnull(x) else x)

        return report_df


def helper():
    __ = FixedOPSHelper()
    return __