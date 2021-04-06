import os
import pandas as pd
from datetime import datetime
from common_utils import jy_ap_col


def generate_pb_list(r_dir):
    file_type = ".xlsx"
    # key_word = "注册制股票交易安排"
    key_word = "股票交易安排"
    data_dir = r_dir
    pb_file_name = "pb_list.xlsx"
    acc_sec_name = "accounts_securities.xlsx"

    file_list = get_file_list(data_dir, key_word, file_type)

    if not file_list:
        return False

    df_zero = pd.DataFrame(columns=jy_ap_col)

    for file in file_list:
        file_path = os.path.join(data_dir, file)
        df = get_single_df(file_path, jy_ap_col)
        if type(df) == bool and df is False:
            return False
        df_zero = pd.concat([df_zero, df]).drop_duplicates()

    if not output_acc(df_zero, jy_ap_col, acc_sec_name):
        return False
    if not output_pb_list(df_zero, jy_ap_col, pb_file_name):
        return False

    return True


def get_time(date=False, utc=False, msl=3):
    if date:
        time_fmt = "%Y-%m-%d %H:%M:%S.%f"
    else:
        time_fmt = "%H:%M:%S.%f"

    if utc:
        return datetime.utcnow().strftime(time_fmt)[:(msl-6)]
    else:
        return datetime.now().strftime(time_fmt)[:(msl-6)]


def print_info(status="I"):
    return "\033[0;33;1m[{} {}]\033[0m".format(status, get_time())


def get_file_list(data_dir, key_w, f_type=".xlsx"):
    file_list = list()
    try:
        all_file_list = os.listdir(data_dir)
        for file in all_file_list:
            if key_w in file and "$" not in file and f_type.split(".")[-1] == file.split(".")[-1]:
                file_list.append(file)
    except FileNotFoundError as e:
        print(print_info("E"), end=" ")
        print("{}: {} can not found!".format(e, data_dir))
        return False
    except:
        print(print_info("E"))
        print("Unknown error happened!")
        return False
    finally:
        print(print_info(), end=" ")
        print("Operating file set is {}".format(file_list))
        return file_list


def get_single_df(file_n, col_lst):
    try:
        df = pd.read_excel(file_n, header=1, converters={0: str})[col_lst]
        df = df[df[col_lst[2]].notna()]
        df[col_lst[-1]].fillna("PB", inplace=True)
        print(print_info(), end=" ")
        print("Loaded the {}".format(file_n))
        return df
    except:
        print(print_info("E"), " ")
        print("Can not loading the {}".format(file_n))
        return False


def output_acc(df, col_lst, save_name):
    df_copy = df.copy()
    try:
        if col_lst[-1] in df.keys().tolist():
            df_copy.drop(columns=[col_lst[-1]], inplace=True)
        df_copy = df_copy.set_index(col_lst[0])
        df_copy.to_excel(save_name)
        print(print_info(), end=" ")
        print("{} saved!".format(save_name))
        return True
    except:
        print(print_info("E"), end=" ")
        print("Can not save to {}.".format(save_name))
        return False


def output_pb_list(df, col_lst, save_name):
    pb_list = list()
    for idx, item in zip(range(len(df)), df[col_lst[-1]]):
        if "PB" in item:
            pb_list.append(df[col_lst[0]].tolist()[idx])
    try:
        sr = pd.Series(pb_list)
        sr.to_excel(save_name, index=None, header=None)
        print(print_info(), end=" ")
        print("{} saved!".format(save_name))
        return True
    except:
        print(print_info("E"), end=" ")
        print("Can not save to {}.".format(save_name))
        return False


if __name__ == '__main__':
    root_dir = os.path.abspath(".")
    TF = generate_pb_list(root_dir)
    if TF:
        print(print_info("S"), end=" ")
        print("Success!")
    else:
        print(print_info("E"), end=" ")
        print("Error!")
