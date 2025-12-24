## 1. 实现卖点指标
- [x] 1.1 创建第一个卖点指标实现：跌破MA60卖点指标 (`strategies/sell_indicators.py`)，继承 `Strategy` 基类
- [x] 1.2 在 `strategies/__init__.py` 中将卖点指标注册到 `AVAILABLE_STRATEGIES` 字典
- [x] 1.3 在 `DAYS_MAP` 中添加卖点指标所需的历史数据天数

## 2. 验证集成
- [x] 2.1 验证卖点指标可以通过 `/api/strategies` 接口列出（API代码自动遍历AVAILABLE_STRATEGIES，已确认注册成功）
- [x] 2.2 验证卖点指标可以通过 `/api/stocks/strategies` 接口应用（复用现有apply_strategies函数，已确认可用）
- [ ] 2.3 测试卖点指标在不同市场（US/CN）上的工作（需要实际数据测试）
- [x] 2.4 验证卖点指标与现有买入策略在同一个分析器中可以同时使用（StockAnalyzer支持添加多个策略，已确认）

## 3. 测试和文档
- [ ] 3.1 测试卖点指标返回正确的股票信息（需要实际数据测试）
- [x] 3.2 验证卖点指标逻辑正确性（代码逻辑已实现：前一天在MA60上，当前天跌破MA60，且成交量放大）
- [ ] 3.3 更新相关文档说明卖点指标的使用方式

