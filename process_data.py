import pandas as pd

df=pd.read_csv('input.csv',dtype=str)
# simpliying the column names 
ren_column ={'Symbol':'Name',
             'Underlying Close Price (A)':'U_Close_Price',
             'Underlying Previous Day Close Price (B)':'U_Prev_Close_Price',
             'Underlying Log Returns (C) = LN(A/B)':'U_Log_Returns',
             'Previous Day Underlying Volatility (D)':'U_Prev_Day_Volatility',
             'Current Day Underlying Daily Volatility (E) = Sqrt(0.995*D*D + 0.005*C*C)':'CDU_Daily_Volatility',
             'Underlying Annualised Volatility (F) = E*Sqrt(365)':'U_Annualised_Volatility'}
df=df.rename(columns=ren_column)

df.to_csv('output.csv',index=False)