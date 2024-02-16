#dw_df1 = pd.read_csv('Prize-AppData2025.csv')
#DataWarehouse_Dataframe = _format_dw_query(dw_df1)
#DataWarehouse_Dataframe = _format_dw_query(_data_warehouse_query())


#prototype_sheet_key = '1eZ4sr06syE1uyhnLD9IoJ-kjbNL7fvX6NGMy-6AZgWc'
#Worksheet = _connect_to_gsheet(prototype_sheet_key,"Waitlist and Registration Management")
#GoogleSheet_Dataframe = _intervention_monitor_ids_to_df(Worksheet)

#Join dataframes
#df_join = _get_data_refresh_from_warehouse(GoogleSheet_Dataframe,DataWarehouse_Dataframe)

#Refresh sheet
#_truncate_and_reload_monitor(df_join,Worksheet)       
#Append new apps to shet
#_check_for_new_students_and_append()



def _sort_intervention_monitor() -> None:
    """Sort intervention monitor last name then by  enrollment status with un-enrolled students at bottom"""
    sheet_dim = (Worksheet.rows, Worksheet.cols)
    Worksheet.sort_range('A3', sheet_dim, basecolumnindex=9, sortorder='DESCENDING')
    Worksheet.sort_range('A3', sheet_dim, basecolumnindex=7, sortorder='DESCENDING')
    Worksheet.sort_range('A3', sheet_dim, basecolumnindex=6, sortorder='ASCENDING')

#_sort_intervention_monitor()