

# ipo_data_plan

非PB账户IPO交易安排

## 主要功能

- [x] 获取进入PB的产品列表， `generate_pb_list.py`
- [x] 传统新股交易安排周报，`ipo_week_assign.py`
- [x] 获取解禁股最近解禁日期， `unlocking_date.py`
- [x] 获取解禁股交易安排， `get_limit_sell_product.py`
- [x] 根据估值表检查未卖出的新股情况， `check_old_stock.py`
- [x] 根据估值表获取产品的净值， `get_net_assets.py`

## 重要文件夹

```bash
$ tree
```

或者windows下

```bash
$ winpty tree.com
```

查看重要文件夹

```console
...
├─bak
├─deal_plan
├─gzb_data
├─old_stock
├─output
├─xsg_output
└─xsg_plan
```

