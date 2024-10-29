# main.py
# from Scripts import get_other_result #, get_cdp_result, get_total_soccer_result, get_final_result, get_summary
import pandas as pd
import emoji
import warnings
import time
from LeagueProcessor.league_processor import LeagueProcessor
from LeagueProcessor.summary_processor import SummaryProcessor
from LeagueProcessor.other_func import pause_execution
# from LeagueProcessor.normal_channel_result_generator import process_normal_result, process_cdp_result

print(emoji.emojize(":smiling_face_with_sunglasses: Greeings! Let's begin generating reports! :soccer_ball:"))
time.sleep(1)

## Check data timeliness
update_sch = input("\U0001F643 InfoSys Data, Match Schedule, Total Soccer and CDP program updated? [y/n] ")
if not update_sch == "y":
  print(emoji.emojize(":stop_sign: Please continue after updating those."), end=" ")
  pause_execution()

## Get update period
update_date_str = input("\U0001F643 Please give start date and end date as: [YYYY-MM-DD YYYY-MM-DD]: ").split()
while len(update_date_str) != 2:
    update_date_str = input("\U0001F643 Please give start date and end date as: [YYYY-MM-DD YYYY-MM-DD]: ").split()

# Check if dates are valid
while True:
    try:
        start_date = pd.to_datetime(update_date_str[0], format="%Y-%m-%d", errors='raise')
        end_date = pd.to_datetime(update_date_str[1], format="%Y-%m-%d", errors='raise')
        break  # If successful, break the loop
    except (ValueError, TypeError):
        update_date_str = input("\U0001F643 Invalid format. Please give start date and end date as [YYYY-MM-DD YYYY-MM-DD]: ").split()

## Get contains_pre_day flag
p_d = input("\U0001F643 Whether contain previous day? [y/n] ")
while not p_d in ['y', 'n']:
    p_d = input("\U0001F643 Whether contain previous day? [y/n] ")

if p_d == 'y':
    contains_pre_day = True
else:
    contains_pre_day = False
    
update_date = pd.to_datetime(update_date_str)
print(emoji.emojize(f":brain: Good job! Let's then begin generating result for none-CDP channels:"))

# 存储共享变量
shared_variables = {
            "Period": update_date,
            "p_d": contains_pre_day,
                }

# 创建 LeagueProcessor 实例并运行处理
processor = LeagueProcessor(shared_variables)
processor.process_normal_result()
processor.process_cdp_result()
processor.process_total_soccer_result()
processor.process_final_result()


## Get Overall period
overall_date_str = input("\U0001F643 Please give start date as: [YYYY-MM-DD]: ").split()
while len(overall_date_str) != 1:
    overall_date_str = input("\U0001F643 Please give start date as: [YYYY-MM-DD]: ").split()

# Check if dates are valid
while True:
    try:
        overall_start_date = pd.to_datetime(overall_date_str[0], format="%Y-%m-%d", errors='raise')
        break  # If successful, break the loop
    except (ValueError, TypeError):
        overall_date_str = input("\U0001F643 Invalid format. Please give start date as [YYYY-MM-DD]: ").split()
overall_date = pd.to_datetime([overall_date_str[0], update_date_str[1]])

# 存储共享变量
shared_variables = {
            "Period": update_date,
            "p_d": contains_pre_day,
            "Overall": overall_date
                }

summary_processor = SummaryProcessor(shared_variables)
summary_processor.process_summary_result()


