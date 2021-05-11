import os
import time
import json
import pandas as pd
from generate_pb_list import print_info, generate_pb_list
from common_utils import del_list, add_acc_sec, ns_col, self_operation
import warnings

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', None)


def main():
    root_path = os.path.abspath(".")
    op_dir_name = "新股网下数据周报"
    op_file_name = "新股网下申购配售统计"
    op_file_type = ".xlsx"
    pb_file_name = "pb_list.xlsx"
    acc_sec_name = "accounts_securities.xlsx"
    save_name = "新股卖出分配周报"

    deal_plan_path = os.path.join(root_path, "deal_plan")

    pb_judge = generate_pb_list(deal_plan_path)

    if not pb_judge:
        return False

    op_dict = {
        "上海": 16,
        "深圳": 4
    }
    sheet_list = list(op_dict.keys())
    info_list = ['代码', '名称']

    df = get_data_list(root_path, op_dir_name, op_file_name, op_file_type, sheet_list)

    if type(df) is bool and df is False:
        return False

    pb_list = get_pb_list(root_path, pb_file_name)
    acc_sec = get_acc_sec(root_path, acc_sec_name)

    ns_op = dict()
    for key in op_dict.keys():
        df_item = df[key].tail(op_dict[key])
        ns_dict, ns_col_copy = op_df(df_item, del_list, pb_list, ns_col, info_list)
        ns_op[key] = get_ns_op(ns_dict, ns_col_copy, acc_sec)

    ns_df = ns_op[sheet_list[0]].drop(ns_col[0], axis=1)

    if len(sheet_list) > 1:
        for idx in range(1, len(sheet_list)):
            # ns_df.to_excel("1.xlsx")
            # ns_op[sheet_list[idx]].drop(ns_col[0], axis=1).to_excel("2.xlsx")
            ns_df = pd.merge(
                ns_df,
                ns_op[sheet_list[idx]].drop(ns_col[0], axis=1),
                how='outer',
                on=['账户', '账号', '券商', '交易员']
            )

    ns_df_drop = drop_line(ns_df, ns_col)
    ns_df_sorted = ns_df_drop.sort_values(by=["券商", "账户"], ascending=[True, True])
    ns_df_save = ns_df_sorted.reset_index(drop=True)
    ns_df_save = drop_self_op(ns_df_save, self_operation)

    return save_df(ns_df_save, root_path, save_name)


def get_pb_list(r_path, pb_file_name):
    pb_file_path = os.path.join(r_path, pb_file_name)
    return pd.read_excel(pb_file_path, header=None, index_col=None)[0].tolist()


def get_acc_sec(r_path, acc_sec_name):
    acc_sec_path = os.path.join(r_path, acc_sec_name)
    df_acc_sec = pd.read_excel(acc_sec_path, header=0, converters={"账号": str})
    acc_sec_keys = df_acc_sec.keys().tolist()
    df_add = pd.DataFrame(columns=acc_sec_keys)
    for idx, item in zip(range(len(acc_sec_keys)), acc_sec_keys):
        df_add[item] = [add_item[idx] for add_item in add_acc_sec]
    df_acc_sec = df_acc_sec.append(df_add, ignore_index=True)
    df_acc_sec.drop_duplicates(["账户"], inplace=True)

    return df_acc_sec.set_index("账户")


def get_data_list(r_path, op_dir_name, op_file_name, op_file_type, sheet_list, head_list=[2, 3]):
    # dir_list = os.listdir(r_path)
    data_list = [item for item in os.listdir(r_path) if op_dir_name in item]
    file_date = data_list[-1].split(op_dir_name)[-1]
    op_date = time.strftime('%Y%m%d', time.localtime(time.time()))
    # print(file_date, op_date)
    if file_date <= op_date:
        file_path = os.path.join(r_path, op_dir_name + file_date, op_file_name + file_date + op_file_type)
        print(print_info(), end=" ")
        print("Deal with the {} data, path is: {}".format(file_date, file_path))

        df = pd.read_excel(file_path, header=head_list, sheet_name=sheet_list, converters={1: str})
        print(print_info(), end=" ")
        print("Get the new-stock DataFrame...")
        return df

    else:
        print(print_info("E"), end=" ")
        print("Could not Get the new-stock DataFrame!")
        return False


def op_df(df_item, del_lst, pb_list, ns_col, info_list):
    col_name = [item[0] for item in df_item.keys()]
    # 删除一些无用的列，但前提是这些列要在数据框中存在
    drop_list = list(set(col_name) & set(del_lst))
    df_new = df_item.drop(drop_list, axis=1)
    temp_keys = df_new.keys()

    product_list = list()
    value_list = list()
    for item in temp_keys:
        if sum(df_new[item].notna()):
            value_list.append(df_new[item].tolist())
            # 注意没有号码的产品名，以及产品通常前缀不超过2个字
            if 'Unnamed:' not in item[1]:
                product_list.append(''.join(item).replace('龙凤呈祥', '龙凤').replace('安枕飞天', '安飞'))
            else:
                product_list.append(item[0])
            len(product_list)

    ns_dict = dict()
    for key, value in zip(product_list, value_list):
        ns_dict[key] = value

    # 删除已经有pb或者客户自行交易的产品
    for item in pb_list:
        if item in ns_dict.keys():
            ns_dict.pop(item)

    # print(json.dumps(ns_dict, ensure_ascii=False, indent=4))

    ns_col_copy = ns_col.copy()
    for name, idx in zip(ns_dict["名称"], ns_dict["代码"]):
        ns_col_copy.append(name + "\n" + str(idx).rjust(6, '0'))

    for item in info_list:
        if item in ns_dict.keys():
            ns_dict.pop(item)

    return ns_dict, ns_col_copy


def get_ns_op(ns_dict, ns_col, acc_sec):
    ns_op = pd.DataFrame(columns=ns_col)
    idx = 0
    for key, value in ns_dict.items():
        idx += 1
        if key in acc_sec.index.tolist():
            # print(acc_sec.loc[key].tolist())
            zh_item = str(acc_sec.loc[key][ns_col[2]])
            qs_item = acc_sec.loc[key][ns_col[3]]
        else:
            zh_item = ""
            qs_item = ""

        item = {
            ns_col[0]: idx,
            ns_col[1]: key,
            ns_col[2]: zh_item,
            ns_col[3]: qs_item,
            ns_col[4]: "operator"
        }

        for v_idx in range(len(value)):
            item[ns_col[5 + v_idx]] = value[v_idx]

        ns_op = ns_op.append(item, ignore_index=True)

    return ns_op


def drop_line(df, col):
    # 删除全 0或NaN 的数据
    col = col[1:]
    df_key = df.keys().tolist()[len(col):]
    df_drop = df[df_key].fillna(0)
    drop_list = list()
    for idx in range(len(df_drop)):
        drop_item = dict(df_drop.iloc[idx])
        if not sum(drop_item.values()):
            drop_list.append(idx)

    df.drop(df.index[drop_list], inplace=True)

    return df


def drop_self_op(df, del_lst):
    # 删除用户自行操作的账户
    drop_list = list()
    for idx, item in zip(df.index, df[ns_col[1]]):
        if item in del_lst:
            drop_list.append(idx)

    df.drop(labels=drop_list, inplace=True)
    return df


def save_df(df, r_path, file_name, op_file_type=".xlsx"):
    op_date = time.strftime('%Y%m%d', time.localtime(time.time()))
    file_name += op_date + op_file_type
    output_dir = os.path.join(r_path, "output")
    if not os.path.exists(output_dir):
        os.makedirs(os.path.join(r_path, "output"))
        print(print_info(), end=" ")
        print("Created the dir: {}.".format(output_dir))
    save_path = os.path.join(output_dir, file_name)
    df.to_excel(save_path, index=None)
    return True


if __name__ == '__main__':
    judge = main()
    if judge:
        print(print_info("S"), end=" ")
        print("Success!")
    else:
        print(print_info("E"), end=" ")
        print("Error!")
