import pandas as pd
import os
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class FileManager:
    def load_data(self, file_path, sheet_name):
        return pd.read_excel(file_path, header=None, sheet_name=sheet_name)

    def save_result(self, df, league, out_file, out_dir = "Result"):
        output_dir = f'{parent_dir}/{out_dir}'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        result_path = os.path.join(output_dir, out_file)
        
        if not os.path.exists(result_path):
            with pd.ExcelWriter(result_path, mode='w', engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=league, index=False)
        else:
            with pd.ExcelWriter(result_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name=league, index=False)
