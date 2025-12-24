# Change: 增加卖点指标

## Why
当前系统只提供买入点策略（如双阳夹MA60、放量突破等），缺少卖出点指标。完整的交易系统需要同时识别买入和卖出信号，帮助用户做出更全面的交易决策。

## What Changes
- 添加卖点指标（Sell Point Indicator）功能
- 卖点指标实现为Strategy的子类，复用现有的策略架构
- 卖点指标注册到 `AVAILABLE_STRATEGIES`，通过现有API接口暴露（`/api/strategies` 和 `/api/stocks/strategies`）
- 添加至少一个卖点指标实现（如：跌破MA60卖点指标）

## Impact
- **Affected specs**: `stock-analysis` (新能力)
- **Affected code**: 
  - `strategies/` - 新增卖点指标实现文件（继承Strategy基类）
  - `strategies/__init__.py` - 将卖点指标注册到 `AVAILABLE_STRATEGIES` 和 `DAYS_MAP`
  - 无需修改API层，复用现有 `/api/strategies` 和 `/api/stocks/strategies` 接口

