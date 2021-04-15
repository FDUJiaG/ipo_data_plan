import os
import re
import time
from datetime import datetime, timedelta
import pandas as pd
from generate_pb_list import print_info, generate_pb_list
from common_utils import del_list, add_acc_sec, ns_col, self_operation, jy_ap_col
from ipo_week_assign import get_data_list, get_pb_list, get_acc_sec, drop_self_op
from get_limit_sell_product import get_file_list
from WindPy import w

import warnings

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', None)


def check_limit_cyb_state(
        r_path,
        op_dir,
        op_file,
        out_dir,
        out_file,
        stock=None,
        op_date=time.strftime("%Y-%m-%d"),
        op_type=".xlsx"
):
    # 获配情况路径
    hp_dir = os.path.join(r_path, "hpqk_data")
    save_name = "限售股股卖出分配周报"

    # 生成目前更新最全的PB名单以及新股账户对应表
    deal_plan_path = os.path.join(root_path, "deal_plan")
    pb_judge = generate_pb_list(deal_plan_path)
    if not pb_judge:
        return False

    # 获取最新的PB名单以及新股账户对应表
    pb_file_name = "pb_list.xlsx"
    acc_sec_name = "accounts_securities.xlsx"
    pb_list = get_pb_list(root_path, pb_file_name)
    acc_sec = get_acc_sec(root_path, acc_sec_name)

    # 获取以往的交易安排
    key_word = "股票交易安排"
    data_dir = os.path.join(r_path, "xsg_plan")
    file_list = get_file_list(data_dir, key_word, op_type)

    if not file_list:
        return False
    # 每次检查一个以往的交易安排来确定限售股
    elif len(file_list) != 1:
        print(print_info("E"), end=" ")
        print("Only one limit sell plan can be run!")
        return False

    # 获取上市时的交易安排
    df_plan = pd.DataFrame(columns=jy_ap_col[:-1])
    for file in file_list:
        file_path = os.path.join(data_dir, file)
        df_tmp = get_df_plan(file_path, jy_ap_col)
        if type(df_tmp) == bool and df_tmp is False:
            return False
        df_plan = pd.concat([df_plan, df_tmp]).drop_duplicates()

    # 处理IPO时的交易安排表
    df_plan.reset_index(inplace=True, drop=True)
    print(print_info(), end=" ")
    print("The IPO plan of {}: \n{}".format(stock, df_plan.head()))

    # 从wind获取相关信息
    ipo_name = "sec_name"
    ipo_price_fun = "ipo_price2"
    ipo_date_fun = "ipo_date"
    unlock_fun = "share_rtd_unlockingdate_fwd"
    wind_func = ",".join([
        ipo_name,
        ipo_price_fun,
        ipo_date_fun,
        unlock_fun
    ])

    # 查看所有注册制创业板的解禁股
    sheet_name = "限售股统计"
    df_zero = get_data_list(r_path, op_dir, op_file, op_type, sheet_name, head_list=[1])
    df_zero.dropna(inplace=True)
    df_zero.reset_index(drop=True, inplace=True)

    if type(df_zero) is bool and df_zero is False:
        return False

    # 创建最后输出的表格
    df_out_col = [
        "账户", "账号", "券商", "软件",
        "交易员", "获配数量", "限售股数量", "估值表数量", "备注"
    ]
    df_out = pd.DataFrame(columns=df_out_col)

    if type(stock) is list:
        for stock_item in stock:
            # 标准化清洗股票代码
            wind_stock = clear_code(stock_item)

            # 获取该股的限售形式
            stock_state = ""
            for code_item, lucky_item, state_item in zip(df_zero["代码"], df_zero["中签配售对象"], df_zero["备注"]):
                if wind_stock == clear_code(code_item):
                    stock_state = get_limit_state(state_item)
                    lucky_list = lucky_str_to_list(lucky_item)
                    break

            # 获取处理时间
            # s_date = datetime.strftime(
            #     datetime.strptime(str("2021-04-15").split()[0], "%Y-%m-%d") + timedelta(days=168), "%Y%m%d"
            # )
            s_date = datetime.strftime(
                datetime.strptime(str(op_date).split()[0], "%Y-%m-%d"), "%Y%m%d"
            )
            wind_data = wind_wsd(wind_stock, wind_func, s_date, s_date)

            print(wind_data.Data)
            hp_name = wind_data.Data[0][0]
            hp_path = get_hp_path(hp_dir, hp_name)
            ipo_price = wind_data.Data[1][0]

            if type(hp_path) is bool and hp_path is False:
                hp_raw_dir = os.path.join(hp_dir, "raw_data")
                hp_code = wind_stock.split(".")[0]
                hp_path = get_hp_path(hp_raw_dir, hp_code)
                print(print_info(), end=" ")
                print("Get the huo_pei data path: {}".format(hp_path))
                hp_data = get_raw_hp_data(wind_stock, hp_name, hp_path, ipo_price)
            elif type(hp_path) is not bool:
                print(print_info(), end=" ")
                print("Get the huo_pei data path: {}".format(hp_path))
                hp_data = get_hp_data(wind_stock, hp_name, hp_path, ipo_price)
                print(hp_data)
            else:
                return False

            hp_data_col = hp_data.columns.to_list()

            # 循环获配表的产品名称和数量
            for item, num in zip(hp_data[hp_data_col[0]], hp_data[hp_data_col[1]]):
                # 摇号限售只考虑部分账户
                if stock_state == "摇号":
                    if item not in lucky_list:
                        continue
                    else:
                        limit_num = num
                else:
                    limit_num = int(round(num / 10, 0))

                account = ""
                qs_name = ""
                state = ""
                if item in pb_list:
                    is_pb = "PB"
                else:
                    is_pb = "普通"

                count = 0
                for product, acc_item, qs_item in zip(
                    df_plan[jy_ap_col[0]], df_plan[jy_ap_col[1]], df_plan[jy_ap_col[2]],
                ):
                    if item in product:
                        account, qs_name = str(acc_item), qs_item
                        count += 1

                if count > 1:
                    print(print_info(), end=" ")
                    print("The {} is Error".format(item))
                    return False

                check_count = 0
                for product, acc_item, qs_item in zip(
                    acc_sec.index, acc_sec[jy_ap_col[1]], acc_sec[jy_ap_col[2]]
                ):
                    if item in product:
                        if account != "" and account != acc_item:
                            # print(account, acc_item)
                            # print(type(account), type(acc_item))
                            state = "现在新股账户为【{}】的【{}】".format(qs_item, acc_item)
                        check_count += 1

                if check_count > 1:
                    print(print_info(), end=" ")
                    print("The {} is Error".format(item))
                    return False

                df_out = df_out.append(
                    {
                        "账户": item,
                        "账号": account,
                        "券商": qs_name,
                        "软件": is_pb,
                        "交易员": "",
                        "获配数量": num,
                        "限售股数量": limit_num,
                        "估值表数量": "开发中",
                        "备注": state
                    },
                    ignore_index=True
                )

            # 去除客户自行交易的账户
            df_save = drop_self_op(df_out, self_operation)
            print(print_info(), end=" ")
            print("Get the output data sample of {}:\n{}".format(stock, df_out.head()))

            # 文件存储
            output_file = "【{}】{}【{}限售】{}".format(hp_name, out_file, stock_state, op_type)
            output_path = os.path.join(r_path, out_dir, output_file)
            df_save.to_excel(output_path, index=None)
            print(print_info(), end=" ")
            print("Success save to the path: {}".format(output_path))
    return True


def get_df_plan(file_n, col_lst):
    col_lst.append("Unnamed: 5")
    try:
        df_tmp = pd.read_excel(file_n, header=1, converters={0: str})[col_lst]
        df_tmp.rename(columns={"方案一": "软件", "Unnamed: 5": "交易员"}, inplace=True)
        df_tmp = df_tmp[df_tmp[col_lst[2]].notna()]
        df_tmp["软件"].fillna("PB", inplace=True)
        print(print_info(), end=" ")
        print("Loaded the {}".format(file_n))
        return df_tmp
    except:
        print(print_info("E"), " ")
        print("Can not loading the {}".format(file_n))
        return False


def lucky_str_to_list(lk_str):
    # 配售对象字符串拆解
    lk_list = list()
    tmp_list = lk_str.split("，")
    for tmp_item in tmp_list:
        if "/" in tmp_item:
            # 取出中文和去除中文
            product_name = ''.join(re.findall('[\u4e00-\u9fa5]', tmp_item)).rstrip("号")
            product_number = re.sub('[\u4e00-\u9fa5]', '', tmp_item)
            num_list = product_number.split("/")
            for num_item in num_list:
                lk_item = product_name + num_item + "号"
                lk_list.append(lk_item)
        else:
            lk_list.append(tmp_item)
    return lk_list


def get_limit_state(lmt_str):
    # 配售方式区分
    if "10%限售6个月" in lmt_str:
        limit_state = "比例"
    elif "限售6个月" in lmt_str:
        limit_state = "摇号"
    else:
        limit_state = "未知"
    return limit_state


def wind_wsd(s_code, c_fun, c_date_st, c_date_end):
    ans = w.wsd(s_code, c_fun, c_date_st, c_date_end, "currencyType=")
    return ans


def clear_code(s_code):
    # 清洗符合wind标准的数据库
    s_code = str(s_code)
    if len(s_code) == 9 and s_code[0] == "3" and s_code[-3:] == ".SZ":
        w_code = s_code
    elif len(s_code) == 6 and s_code[0] == "3":
        w_code = s_code + ".SZ"
    else:
        return False
    return w_code


def get_hp_path(hp_dir, hp_name):
    file_list = os.listdir(hp_dir)
    for file_tmp in file_list:
        if hp_name in file_tmp:
            file_path = os.path.join(hp_dir, file_tmp)
            return file_path
    return False


def get_hp_data(s_code, hp_name, hp_path, ipo_p, f_type="xlsx"):
    # 根据新版获配表获取各个产品的获配数量
    new_name = hp_name + "获配数量"
    if f_type == "xlsx":
        hp_df = pd.read_excel(hp_path)
        hp_df_col = hp_df.columns.to_list()
        print(print_info(), end=" ")
        print("Get the raw huo_pei table columns: {}".format(hp_df_col))
        if str(int(hp_df[hp_name][0])) != s_code.split(".")[0]:
            new_df = hp_df[["配售对象名称", hp_name]]
            new_df.rename(columns={hp_name: new_name}, inplace=True)
        elif str(int(hp_df[hp_name + ".1"][0])) != s_code.split(".")[0]:
            new_df = hp_df[["配售对象名称", hp_name + ".1"]]
            new_df.rename(columns={hp_name + ".1": new_name}, inplace=True)
            # print(round(new_df[hp_name][0] / 47.33))
        else:
            return False
        new_df.dropna(inplace=True)
        # 根据金额换算成股数
        new_df[new_name] = round(new_df[new_name] / ipo_p).astype("int")
        print(print_info(), end=" ")
        print("Get the huo_pei data sample of {}:\n{}".format(s_code, new_df.head()))
    elif f_type == "xls":
        new_df = False
    else:
        print(print_info("E"), end=" ")
        print("Path Error: {}".format(hp_path))
        return False
    return new_df


def get_raw_hp_data(s_code, hp_name, hp_path, ipo_p, f_type="xlsx"):
    # 根据原始获配表获取各个产品的获配数量
    new_name = hp_name + "获配数量"
    if f_type == "xlsx":
        hp_df = pd.read_excel(hp_path)
        hp_df_col = hp_df.columns.to_list()
        print(print_info(), end=" ")
        print("Get the raw huo_pei table columns: {}".format(hp_df_col))
        new_df = hp_df[["配售对象名称", "总金额（万元）"]]
        new_df.rename(columns={"总金额（万元）": new_name}, inplace=True)
        new_df.dropna(inplace=True)
        # 根据金额换算成股数
        new_df[new_name] = round(new_df[new_name] * 10000 / ipo_p).astype("int")
        for idx, item in zip(new_df.index, new_df["配售对象名称"]):
            new_df["配售对象名称"][idx] = fullname_to_short(item)
        print(print_info(), end=" ")
        print("Get the huo_pei data sample of {}:\n{}".format(s_code, new_df.head()))
    elif f_type == "xls":
        new_df = False
    else:
        print(print_info("E"), end=" ")
        print("Path Error: {}".format(hp_path))
        return False
    return new_df


def fullname_to_short(f_name):
    # 将产品名称清洗为标准化简称
    tmp_name = f_name.split("迎水")[1]
    tmp_name = tmp_name.replace("证券", "").replace("私募", "").replace("投资", "").replace("基金", "")
    short_name = tmp_name.replace("龙凤呈祥", "龙凤").replace("安枕飞天", "安飞")
    short_name = short_name.replace("东方赢家", "稳健")
    return short_name


if __name__ == '__main__':
    root_path = os.path.abspath(".")
    op_dir_name = "新股网下数据周报"
    op_file_name = "创业板网下申购配售统计"
    out_dir_name = "xsg_output"
    out_file_name = "限售股卖出分配"
    # op_file_type = ".xlsx"
    op_day = time.strftime("%Y-%m-%d")

    stock_list = [
       "300901"
    ]

    w.start()
    w.isconnected()
    df = check_limit_cyb_state(
        r_path=root_path,
        op_dir=op_dir_name,
        op_file=op_file_name,
        out_dir=out_dir_name,
        out_file=out_file_name,
        stock=stock_list, op_date=time.strftime("%Y-%m-%d"))
    # print(df)

    w.close()
    # df.to_excel("test.xlsx")