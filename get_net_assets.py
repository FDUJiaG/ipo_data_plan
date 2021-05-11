import os
import time
import json
import pandas as pd
from ipo_week_assign import get_data_list, print_info
from common_utils import station_confirm, station_head_dict, net_asset_loc_dict
from check_old_stock import file_scan

import warnings

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', None)


def get_net_assets():
    root_path = os.path.abspath(".")
    data_path = os.path.join(root_path, "gzb_data")
    output_name = "产品净值列表"
    op_file_type = [".xlsx", ".xls"]

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

    net_asset_col = ["Product", "Securities", "Net Assets"]
    df_net_assets = pd.DataFrame(columns=net_asset_col)
    station_dict = dict()

    # 重新获取无重复的产品列表
    file_list_len = len(file_list)
    for file_idx, file_item in zip(range(file_list_len), file_list):
        # 获取券商标志
        station = station_confirm(file_item)
        if "华资" not in file_item:
            cp_name = file_item.split("迎水")[-1].split("私募")[0].split("证券")[0]
        else:
            cp_name = "华资" + file_item.split("迎水")[-1].split("私募")[0].split("证券")[0]
        station_dict[cp_name] = station

        # 获取估值表
        df = pd.read_excel(file_item, header=station_head_dict[station])
        df = df.set_index(df.columns[0])
        print(print_info(), end=" ")
        print("[{:0>3d}/{:0>3d}] Check the: {}, Station: {}".format(file_idx, file_list_len, cp_name, station))
        # 根据列名和行名获取净值
        df_net_assets = df_net_assets.append(
            {
                "Product": cp_name,
                "Securities": station,
                "Net Assets": df[net_asset_loc_dict[station][1]][net_asset_loc_dict[station][0]]
            },
            ignore_index=True
        )

    # 查看没一个产品的新股账户券商
    # print(json.dumps(station_dict, indent=4, ensure_ascii=False))

    # 去除重复项
    df_net_assets.drop_duplicates(net_asset_col, inplace=True)
    df_net_assets.reset_index(drop=True, inplace=True)

    print(print_info(), end=" ")
    # print("Get the check dataframe:\n{}".format(df_check))
    print("Get the value count:\n{}".format(
        df_net_assets["Securities"].value_counts())
    )

    op_date = time.strftime('%Y%m%d', time.localtime(time.time()))
    output_name += op_date + ".xlsx"
    df_net_assets.to_excel(output_name, index=None)


if __name__ == '__main__':
    get_net_assets()
