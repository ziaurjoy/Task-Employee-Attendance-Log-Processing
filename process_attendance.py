import os
import json
import pandas as pd
from datetime import datetime
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv()


# Set shift time from env file
shift_start_time = datetime.strptime(os.environ.get("SHIFT_START_TIME"), "%H:%M:%S").time()
shift_end_time = datetime.strptime(os.environ.get("SHIFT_END_TIME"), "%H:%M:%S").time()


# this funcation read the log file and return log data
def read_log_data(file_path):

    try:

        with open(file_path, 'r') as file:

            log_data = []

            for line_number, line in enumerate(file, 1):
                
                parts = line.split()

                if len(parts) < 6:

                    with open("error.txt", "a") as f:
                        f.write(line)

                    continue

                timestamp = parts[3]
                dt = datetime.utcfromtimestamp(int(timestamp))

                user_id = parts[0]
                user_name = f"{parts[1]} {parts[2]}"
                device_name = f"{parts[4]} {parts[5]}"
                date_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                date = date_time.split(" ")[0]

                log_data.append(
                    {"date": date, "user_id": user_id, "user_name": user_name, "date_time": date_time, "device": device_name}
                )

            return log_data

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")



# this funcation taken paramer log data and grouped by date
def group_by_date(all_data):

    grouped_data = defaultdict(list)

    for item in all_data:
        grouped_data[item["date"]].append(item)

    return grouped_data



def process_attendance(all_date_list, group_result):
    result = []

    for date in all_date_list:
        per_day_logs = group_result.get(date, None)

        # per date seperate user id:
        all_user_id = list(set([item["user_id"] for item in per_day_logs]))

        per_day_user_attendance = []

        for user_id in all_user_id:

            # filter user dict by user id
            per_user_logs = [item for item in per_day_logs if item["user_id"] == user_id]

            # filter dicts only shifting time data
            filtered = [
                item for item in per_user_logs
                if datetime.strptime(item["date_time"], "%Y-%m-%d %H:%M:%S").time() < shift_end_time and datetime.strptime(item["date_time"], "%Y-%m-%d %H:%M:%S").time() > shift_start_time
                ]

            if filtered:
                min_item = min(filtered, key=lambda x: datetime.strptime(x["date_time"], "%Y-%m-%d %H:%M:%S"))
                max_item = max(filtered, key=lambda x: datetime.strptime(x["date_time"], "%Y-%m-%d %H:%M:%S"))
                total_punches = len(per_user_logs)
                emp_code = min_item.get('user_id', None)
                first_punch = datetime.strptime(min_item.get('date_time', None), "%Y-%m-%d %H:%M:%S")
                last_punch = datetime.strptime(max_item.get('date_time', None), "%Y-%m-%d %H:%M:%S")
                working_hours = last_punch - first_punch

                total_seconds = int(working_hours.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60

                working_hours = f"{hours:02d}:{minutes:02d}"

                late_entry = 1 if shift_start_time < first_punch.time() else 0
                early_exit = 1 if last_punch.time() < shift_end_time else 0
                per_day_user_attendance.append({
                    "total_punches": total_punches,
                    "emp_code": emp_code,
                    "first_punch": first_punch.strftime("%H:%M:%S"),
                    "last_punch": last_punch.strftime("%H:%M:%S"),
                    "working_hours": working_hours,
                    "late_entry": late_entry,
                    "early_exit": early_exit,
                })
    

        date_dict_date = {
            date: per_day_user_attendance
        }
        result.append(date_dict_date)

    return result


def data_format_to_df(generated_logs): 

    rows = []

    for logs in generated_logs:
        for date, items in logs.items():
            for item in items:
                _dict = {
                        "date": date,
                        "total_punches": item["total_punches"],
                        "emp_code": item["emp_code"],
                        "first_punch": item["first_punch"],
                        "last_punch": item["last_punch"],
                        "working_hours": item["working_hours"],
                        "late_entry": item["late_entry"],
                        "early_exit": item["early_exit"],
                    }
                
                rows.append(_dict)

    return rows


if __name__ == "__main__":

    # path link from env file
    log_file_path = os.environ.get("FILE_PATH")

    # this function will return log data
    log_data = read_log_data(log_file_path)

    # full data set to create group by date
    group_by_date_data = group_by_date(log_data)
    
    group_result = dict(sorted(group_by_date_data.items()))

    all_date_list = list(group_result.keys())

    generated_logs = process_attendance(all_date_list, group_result)

    with open("parsed_logs.json", "w") as f:
        json.dump(generated_logs, f, indent=4)

    df_formated_data = data_format_to_df(generated_logs)
    
    df = pd.DataFrame(df_formated_data)

    # Save to Excel
    df.to_excel("attendance_summary.xlsx", index=False)

