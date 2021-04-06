from WindPy import w
import pandas as pd
import os
import time
from datetime import datetime, timedelta

w.start()
w.isconnected()


def unlocking_date(s_code, c_fun, c_date_st, c_date_end):
    ans = w.wsd(s_code, c_fun, c_date_st, c_date_end, "Days=Alldays")
    return ans


def get_unlocking_date(c_fun):
    root_path = os.path.abspath(".")
    data_path = os.path.join(root_path, "新股网下数据周报20210326", "创业板网下申购配售统计20210326.xlsx")
    df = pd.read_excel(data_path, sheet_name="限售股统计", header=1)
    df = df.drop(labels=0).reset_index(drop=True)
    # op_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))  # 处理日期
    df["wind解禁日"] = None

    for idx, d_item, c_item in zip(range(len(df)), df["上市日"], df["代码"]):
        # print(type(d_item))
        s_code = str(int(c_item.split(".")[0])) + ".SZ"
        # s_date = time.strftime('%Y-%m-%d', time.strptime(str(int(d_item) + 1), "%Y%m%d"))
        try:
            if d_item is not None:
                s_date = datetime.strftime(
                    datetime.strptime(str(d_item).split()[0], "%Y-%m-%d") + timedelta(days=168), "%Y%m%d"
                )
                unl_date = unlocking_date(s_code, c_fun, s_date, s_date)
                if unl_date.Data[0][0] is not None:
                    df["wind解禁日"][idx] = datetime.strftime(unl_date.Data[0][0], "%Y-%m-%d")
        except:
            print(c_item)
    return df["wind解禁日"]


if __name__ == '__main__':
    stock_code = "300878.SZ"
    check_fun = "share_rtd_unlockingdate_fwd"
    check_date_start = "2021-02-28"
    check_date_end = "2021-02-28"
    # ans = unlocking_date(stock_code, check_fun, check_date_start, check_date_end)
    # print(ans.Data)

    df = get_unlocking_date(check_fun)
    print(df)
    df.to_excel("unlock_date.xlsx", index=None)
    w.stop()
