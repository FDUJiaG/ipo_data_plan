import os
import time
import datetime
import json
import pandas as pd
from ipo_week_assign import get_data_list, print_info
from common_utils import station_confirm, station_head_dict, station_num_list_dict, self_operation

import warnings

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', None)


def main():
    root_path = os.path.abspath(".")
    data_path = os.path.join(root_path, "gzb_data")
    op_dir_name = "新股网下数据周报"
    op_file_name = "新股网下申购配售统计"
    op_cyb_name = "创业板网下申购配售统计"
    cyb_sheet = "限售股统计"
    output_name = "陈年老股检查表"
    op_file_type = [".xlsx", ".xls"]
    output_dir = "old_stock"
    op_date = time.strftime('%Y%m%d', time.localtime(time.time()))  # 处理日期
    output_path = os.path.join(root_path, output_dir)
    op_dict = {
        "上海": 50,
        "深圳": 50
    }
    sheet_list = list(op_dict.keys())
    info_list = ['代码', '名称']
    df = get_data_list(root_path, op_dir_name, op_file_name, op_file_type[0], sheet_list)

    if type(df) is bool and df is False:
        return False

    # 创业板配售统计
    cyb_df = get_data_list(root_path, op_dir_name, op_cyb_name, op_file_type[0], [cyb_sheet], 1)
    if type(cyb_df) is bool and cyb_df is False:
        return False

    cyb_df = cyb_df[cyb_sheet].drop(labels=0).reset_index(drop=True)
    check_cyb_df = cyb_df[cyb_df["解禁日"] <= datetime.datetime.strptime(op_date, "%Y%m%d")]

    cyb_code_list = [item.split(".")[0] for item in check_cyb_df["代码"]]
    cyb_dict = dict(
        zip(
            # check_cyb_df["代码"],
            cyb_code_list,
            check_cyb_df["名称"]
        )
    )

    print(print_info(), end=" ")
    print("Get the cyb dict:\n {}".format(cyb_dict))

    stock_code = list()
    stock_name = list()
    # 对于沪市和深市，我们分别查找最新的股票进行后续判断
    for key in op_dict.keys():
        df_item = df[key].tail(op_dict[key])
        temp_code = list()
        temp_name = list()
        for col_item in df_item.columns:
            if info_list[0] in col_item:
                temp_code = df_item[col_item].tolist()
            elif info_list[1] in col_item:
                temp_name = df_item[col_item].tolist()

            # 股票代码和股票名都查到之后就结束循环
            if len(temp_code) > 0 and len(temp_name) > 0:
                break
        stock_code += temp_code
        stock_name += temp_name
    stock_dict = dict(zip(stock_code, stock_name))

    # 增加创业板股票的查询
    stock_dict.update(cyb_dict)
    print(print_info(), end=" ")
    print("Get the check dict: \n {}".format(stock_dict))

    file_list = file_scan(data_path)
    file_list_copy = file_list.copy()
    op_file_type_plus = [type_item.split(".")[-1] for type_item in op_file_type]

    for file_item in file_list_copy:
        if file_item.split(".")[-1] not in op_file_type_plus:
            file_list.remove(file_item)
            print(print_info(), end=" ")
            print("remove the file {}".format(file_item))

    # 根据产品名称，如果出现多次，则删除先前出现的文件名
    file_list = sorted(file_list)
    file_list_copy = file_list.copy()
    if "华资" not in file_list_copy[0]:
        cp_temp = file_list_copy[0].split("迎水")[-1].split("私募")[0].split("证券")[0]
    else:
        cp_temp = "华资" + file_list_copy[0].split("迎水")[-1].split("私募")[0].split("证券")[0]
    for idx, file_item in zip(range(len(file_list_copy[1:])), file_list_copy[1:]):
        if "华资" not in file_item:
            cp_name = file_item.split("迎水")[-1].split("私募")[0].split("证券")[0]
        else:
            cp_name = "华资" + file_item.split("迎水")[-1].split("私募")[0].split("证券")[0]
        if cp_temp == cp_name:
            file_list.remove(file_list_copy[idx])
            print(print_info(), end=" ")
            print("ignore the file {}".format(file_list_copy[idx]))
        cp_temp = cp_name

    check_col = ["product", "stock code", "stock name", "stock num", "base date"]
    df_check = pd.DataFrame(columns=check_col)
    station_dict = dict()

    file_list_len = len(file_list)
    for file_idx, file_item in zip(range(file_list_len), file_list):
        station = station_confirm(file_item)
        if "华资" not in file_item:
            cp_name = file_item.split("迎水")[-1].split("私募")[0].split("证券")[0]
        else:
            cp_name = "华资" + file_item.split("迎水")[-1].split("私募")[0].split("证券")[0]
        station_dict[cp_name] = station

        # 获取一个参考估值表的日期
        date_tmp = list(filter(str.isdigit, file_item))
        if "银海" in file_item:
            base_date = "".join(date_tmp[-7:-1])
        else:
            base_date = "".join(date_tmp[-6:])

        target_list, number_list = data_extract(file_item, station, station_head_dict, station_num_list_dict)
        for target_item, number_item in zip(target_list, number_list):
            if target_item in stock_dict.keys():
                df_check = df_check.append(
                    {
                        "product": cp_name,
                        "stock code": target_item,
                        "stock name": stock_dict[target_item],
                        "stock num": number_item,
                        "base date": base_date
                    },
                    ignore_index=True
                )

        print(print_info(), end=" ")
        print("[{:0>3d}/{:0>3d}] Check the: {}".format(file_idx, file_list_len, cp_name))

    # 查看没一个产品的新股账户券商
    # print(json.dumps(station_dict, indent=4, ensure_ascii=False))

    # 去除重复项
    df_check.drop_duplicates(check_col, inplace=True)
    df_check.reset_index(drop=True, inplace=True)

    # 去除客户自己交易的账户
    df_check = drop_self_op(df_check, self_operation, check_col)

    print(print_info(), end=" ")
    # print("Get the check dataframe:\n{}".format(df_check))
    print("Get the value count:\n{}".format(
        df_check["stock code"].value_counts())
    )

    output_name += op_date + ".xlsx"
    df_check.to_excel(
        os.path.join(output_path, output_name),
        index=None
    )


# 文件扫描
def file_scan(data_path, ignore="history"):
    # 返回目标路径下所有三级文件
    file_list = []
    walker = os.walk(data_path)
    for each in walker:
        if ignore not in each[0]:
            for file in each[2]:
                if os.path.exists(each[0] + file):
                    file_list.append(each[0] + file)
                else:
                    file_list.append(each[0] + '\\' + file)
    return file_list


def data_extract(file, station, head_dict, num_list_dict):
    target_list = list()
    stock_num_list =list()
    # 对于每一个估值表， 检查第一列
    df = pd.read_excel(file, header=head_dict[station])
    # 获取表头列
    check_col = df.columns[0]
    # 获取数据列
    num_col = df.columns[num_list_dict[station]]
    df_tmp = df[[check_col, num_col]].dropna()
    check_list = list(df_tmp[check_col])
    num_list = list(df_tmp[num_col])

    for check_item, num_item in zip(check_list, num_list):
        digit_item = "".join(list(filter(str.isdigit, str(check_item))))
        # digit_item = "".join(list(filter(lambda ch: ch in "0123456789", check_item)))
        if len(digit_item) >= 12:
            target_list.append(digit_item[-6:])
            stock_num_list.append(num_item)

    return target_list, stock_num_list


def drop_self_op(df, del_lst, chk_col):
    # 删除用户自行操作的账户
    drop_list = list()
    for idx, item in zip(df.index, df[chk_col[0]]):
        if item in del_lst:
            drop_list.append(idx)

    df.drop(labels=drop_list, inplace=True)
    return df


if __name__ == '__main__':
    main()
