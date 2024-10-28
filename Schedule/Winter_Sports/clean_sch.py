import pandas as pd
import datetime

df_sch_skiing = pd.read_excel("./schedule-2025.xlsx", sheet_name="Skiing")
df_sch_curling = pd.read_excel("./schedule-2025.xlsx", sheet_name="Curling") 

today = datetime.datetime.today()

df_sch_skiing = df_sch_skiing[df_sch_skiing['Date'] <= today]
df_sch_curling = df_sch_curling[df_sch_curling['Date'] <= today]

