# 交易事件身份迁移

本阶段未执行生产部署。先停止旧版再启动新版，不混跑新旧核销逻辑。升级前用现有一致性备份命令创建新文件并验证，路径按部署调整，禁止覆盖唯一备份：

```sh
litepay -backup-db data/litepay.db -output backups/litepay-before-event-identity.db
litepay -verify-db backups/litepay-before-event-identity.db
```

`store.Open` 自动添加身份列及事件闸门、辅助对账表。身份回填在事务内执行，失败拒绝启动，中断可重试；不改订单金额、PAID 状态、历史流水主键或回调记录。

- `incoming_txs`、`tx_observations` 增加 `event_id`、`detail_id`、`reference_id`、`canonical_id`、`identity_issue`。
- `event_claims` 以 `(provider,event_id)` 为主键，与核销流水、订单、槽位和观察状态在同一事务写入。
- `funding_bill_evidences`、`auxiliary_runs` 只作辅助证据，不能用于核销。

旧身份仅从保存的原始 JSON 读取 Binance `transactionId` 或 OKX `depId`。不会把 `orderId`、`fromWdId`、`billId` 当作父事件，也不通过去掉旧明细后缀来猜测 ID。无法确认的旧记录标记 `legacy_unknown`、保留原主键；潜在别名只用于阻止重复认领。

历史同一父事件多次支付时保留全部历史记录，闸门标记 `legacy_conflict`，不删除、合并付款。歧义观察建立持久事件屏障，后续接口只返回一条明细也不能自动清除；现有后台补单不能绕过。人工须结合真实资金、商城订单及交付记录处置。

回归覆盖旧字段 SQLite 升级、PAID 金额与原流水主键保持、重复启动、历史别名/冲突、并发事件认领和失败事务回滚。CI Docker 演练验证部署失败恢复；生产迁移需另行授权。

放行前失败可用停机快照回滚。放行后已有新付款时，禁止恢复旧库覆盖当前数据，也不应让没有事件屏障的旧二进制继续接单；应先停止收款、保留现场，再对账处理。
