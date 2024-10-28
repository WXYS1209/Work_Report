import pandas as pd
import argparse
from openpyxl import load_workbook
from datetime import datetime, timedelta

columns = ['Date', 'Time', 'Title']
df = pd.DataFrame(columns=columns)

dff = []

def get_schedule_cdp(date_str = None):
    global df
    global dff
    
    # file_path = f"//tnsfs/tnsfs/部门各成员文件夹/杜思成/赛程节目单抓取/节目单/Schedule {date_str}.xlsx"
    temp_df = pd.read_excel("./tmp.xlsx")
    # try:
    #     temp_df = pd.read_excel(file_path, sheet_name="风云足球")
    #     print(f"Schedule found on {date_str}.")
    # except FileNotFoundError:
    #     print(f"No such file on {date_str}.")
    #     dff.append(date_str)
    #     return df
    # except Exception as e:
    #     # Handle any other exceptions
    #     print(f"Error reading the file: {e}")
    #     dff.append(date_str)
    #     return df

    df = pd.concat([df, temp_df], ignore_index=True)
    return df


parser = argparse.ArgumentParser(description='Merge CDP program sheet.')
parser.add_argument('start_date', type=str)
parser.add_argument('end_date', type=str)

args = parser.parse_args()

d1 = args.start_date
d2 = args.end_date
start_date = pd.to_datetime(d1)
end_date = pd.to_datetime(d2)

# for i in range((end_date - start_date).days + 1):
#     temp_date = start_date + timedelta(days=i)
#     temp_date_str = temp_date.strftime('%Y-%m-%d')
get_schedule_cdp()

# dff = pd.DataFrame({"Missing": dff})
# dff.to_excel("./missing_date.xlsx")

try:
    df.to_excel("D:/wangxiaoyang/Regular_Work/Reports/Football_Program/CDP/cdp_ps.xlsx", index=False)
    print('Schedule wrote to D:/wangxiaoyang/Regular_Work/Reports/Football_Program/CDP/cdp_ps.xlsx.')
except Exception as e:
    print(f"Error writing the file: {e}")



