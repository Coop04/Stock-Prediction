'''
    this is to calculate the year to date dataframe
'''

import pandas as pd
import datetime as datetime

#if __name__ in ["__main__"]:
    
    #import from_api as fromApi

#else:
    #from . import from_api as fromApi

def YearToDateFun(dailyDf, requiredDate=None):
    
    # fetch the daily data of the symbol

    df = dailyDf
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['year'] = df['date'].dt.year
    
    if requiredDate is None:
        requiredDate = df.iloc[0]['date']

    req_day = requiredDate.day
    req_month = requiredDate.month
   
    start_year = df.iloc[0]['date'].year
    end_year = df.iloc[-1]['date'].year

    result=[]

    for current_year in range(start_year, end_year-1, -1):

        requiredDate = datetime.datetime(current_year, req_month, req_day)
        df_sub = df[df['date'] <= requiredDate]

        if not df_sub.empty:
            result.append({'year':df_sub.iloc[0]['date'].year, 'to_date_close':df_sub.iloc[0]['close']})

    result_df=pd.DataFrame(result)

    yearToDate = df.groupby('year').first().reset_index()

    yearToDate=yearToDate[['year','close']].sort_index(ascending=False)

    merged_df = pd.merge(result_df,yearToDate , on='year', how='outer')
    merged_df= merged_df.sort_index(ascending = False) 
    merged_df['yearToDate'] = (merged_df['to_date_close']- merged_df['close'].shift(-1)) * 100/ merged_df['close'].shift(-1)
    
    merged_df = merged_df[['year','yearToDate']]
    return merged_df

   
