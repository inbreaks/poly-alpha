# BTC Basis MVP 验收测试说明

当前这套最小验收护栏只做三件事：

1. 保证 `price-only` 市场过滤不会把明显的新闻型 BTC 市场放进来
2. 保证题面解析不会悄悄改坏
3. 保证“上一根已完成 CEX bar”这条因果约束不会被改回未来函数

## 运行方法

在仓库根目录执行：

```bash
python3 -m unittest discover -s scripts/tests -v
```

## 当前覆盖点

- `scripts/tests/test_btc_price_market_filter.py`
  价格型市场过滤
- `scripts/tests/test_market_rule_parsing.py`
  题面解析
- `scripts/tests/test_causal_time_alignment.py`
  构库与回测两侧的时间对齐

## 当前还没覆盖的风险

- DuckDB 全链路回放一致性
- walk-forward 样本外分段结果回归
- 官方 resolution 结算对账
- Python / Rust parity fixture
