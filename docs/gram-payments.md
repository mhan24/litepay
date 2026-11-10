# GRAM 收款与定期检测

LitePay 使用 Toncenter v2 接收 TON 主网原生 GRAM，以 USD 创建商城订单。钱包地址不是 Jetton 合约，GRAM 不进入 Binance / OKX 站内转账通道。

## 配置与运行

1. 启动 LitePay，打开后台的支付设置，在开放币种中勾选 GRAM。
2. 在「GRAM · TON 主网」填写自己的收款地址；Toncenter API Key 可选。确认持有该钱包后保存。公开接口可以读取任意钱包，不能用来证明钱包所有权。
3. 后台会持续显示余额和 GRAM/USD 行情的检测时间。即使没有待付款订单，也会检测余额与行情；新订单须等行情可用后才能选择 GRAM 付款。
4. 付款人选择 TON / GRAM，在 Telegram（电报）打开「钱包」，点击右上角「扫一扫」扫描收款地址二维码，再复制页面的完整应付金额。也可复制地址或使用「打开钱包付款」。发送原生 GRAM，手续费另付，勿发送 Jetton。

二维码由服务端生成，内容为订单锁定的收款地址，与 Telegram 钱包的地址二维码格式一致，不调用第三方二维码服务。打开钱包链接另以 `ton://transfer/` 携带同一地址及 nanogram 整数金额。付款方式或金额变化时页面刷新；已支付、取消或超时后隐藏二维码及钱包付款入口。

配置通过 `settings.Patch` 写入 SQLite，服务重启后恢复。启动 YAML 只保存监听、数据库和后台登录设置。定期检测由服务进程执行，服务退出后停止。

行情与余额检测周期、行情时效上限见 `internal/gram/monitor.go` 的 `PriceInterval`、`BalanceInterval`、`MaxPriceAge`。到账查询间隔由后台「到账查询间隔」控制，其默认值见 `config.Default` 的 TON 配置。接口耗时、分页、限流和网络故障可能延长两次成功检测之间的时间。

## Toncenter API 的字段与收款证据

使用以下只读接口，不需要钱包私钥或助记词：

- `getTransactions`：查询地址交易，使用 `archival=true`、`lt` 和 `hash` 分页。单独取最近十条不能保证补齐高峰或停机期间的交易。
- `getAddressBalance`：读取余额；交易列表不能代替余额查询。

| 交易字段 | 含义与处理 |
| --- | --- |
| `transaction_id.hash` | 交易身份；与 BOC 的根哈希比对，转换为十六进制后用于去重和后台核对。 |
| `transaction_id.lt` | 逻辑时间，用于历史分页；不是 Unix 时间。 |
| `utime` | Unix 秒，与 BOC 时间比对，用于付款窗口匹配。 |
| `in_msg.value` | 入站消息携带的原生币数量，单位为 nanogram，除以十亿得到 GRAM；不能仅凭这一字段认定收款。 |
| `data` | Base64 BOC。实际收款金额、收款地址、执行状态、退回标记和消息内容从这里解析。 |
| `out_msgs` | 出站消息；伴随转出或合约执行的交易不作为普通收款自动核销。 |

普通内部入站转账须匹配收款地址，没有退回、转出、状态初始化或合约调用载荷。正常 VM 路径要求执行成功且未 aborted。另允许未初始化账户接收 non-bounceable 的 `NO_STATE` 转账：须有完整信用阶段、信用额与消息金额一致、无欠费扣收、状态为 uninit、无 action/bounce，且明确的信用额减存储费用仍为正。此路径的 aborted 表示未执行 action，不能单独当作资金退回。其他 skipped 原因、bounceable 或冻结账户不自动核销。空消息和文本备注消息可接收。

BOC 损坏、哈希/逻辑时间/时间/账户不一致、分页顺序异常或响应不完整时，本页失败，保留已提交的 LT/Hash 检查点；下一轮从该检查点继续。每轮最多一页，固定范围全部完成后才推进时间游标，不受单次 100 页上限影响。查询失败不自动把订单当作未付款而关闭。后续补查、金额档位、唯一认领和回调沿用 `store.Process` 的事务规则。

到账精确到链上的九位小数；支付域使用六位小数。后面三位非零的到账保留原始金额并拒绝自动核销，不截断、不四舍五入。到账由后台自动扫描核对，收银台不接受手动提交交易哈希。

## 实时报价与误差边界

行情源为 Kraken 原生币的 `TONUSD` 买一/卖一价格，取中间价，直接使用 USD 交易对。Tonviewer 是浏览器页面；TonAPI 的 rates 文档将其价格限定为展示用途，因此不用于订单换算。

买卖价差超过中间价的 0.5%、响应不完整、价格非正、检测失败或报价过期时，暂停生成新报价。失败后保留上次价格及检测时间供诊断，但该价格不能继续用于新订单。

选择付款方式时，用十进制有理数计算 `USD / 每枚 GRAM 的 USD 价格`，向上对齐至六位小数，再分配独占金额。舍入与订单识别尾差合计最多 0.1%；没有满足此上限的档位就拒绝选择，不扩大尾差。这里的 0.1% 是相对本单所用报价的计算边界，不保证其他交易所、Tonviewer 或之后时刻的市场价格差异也在此范围。

订单保存所用价格、检测时间和完整应付数量。后续行情检测不改变已选择订单的应付数量；同一付款方式的重复请求也复用已锁定的金额。商城回调 `amount` 是原始 USD 订单金额，`actual_amount` 是实收 GRAM 数量。商户须按其接入契约解释这两个单位。

管理员鉴权接口 `GET <后台入口>/gram/status` 返回余额、USD 价格、买卖价、检测时间、有效性和错误状态。其返回值用于监控，不作为付款证据。

## 验证

执行 `go test ./...`、`go test -race ./...`、`go vet ./...`、`go build ./...`。测试覆盖公开链上样本的 BOC 解析、分页、重复交易、拒绝不安全到账、精度、过期行情、报价尾差和签名回调。

真实验收时创建小额 USD 订单，以页面完整数量转账，记录链上哈希，核对到账确认与商城交付；重复查询不应产生第二次认领。还应中断行情源，确认新报价停止而已锁定订单仍能按链上证据核销。自动化模拟和只读接口查询不能替代这一步。

接口参考：[Toncenter v2](https://toncenter.com/api/v2/)、[TON 原生币余额单位](https://docs.ton.org/api/v2/rpc/get-address-balance)、[Kraken Ticker](https://docs.kraken.com/api-reference/market-data/get-ticker-information)、[TonAPI OpenAPI rates 契约](https://github.com/tonkeeper/opentonapi/blob/master/api/openapi.yml)。

NO_STATE 信用与 aborted 行为核对依据：[TON 执行阶段](https://docs.ton.org/foundations/phases)、[节点交易序列化与信用阶段实现](https://github.com/ton-blockchain/ton/blob/master/crypto/block/transaction.cpp)。本地验证交易 BOC 与返回哈希，信任 Toncenter 的链数据来源；不宣称独立验证主链共识或账户余额的 Merkle 证明。

## 主链最终性门槛

BOC 验证通过仍不足以核销。每页候选原生币到账还需在 Toncenter v3 的交易索引中按账户、LT 和交易哈希精确对应，并取得正数 `mc_block_seqno`、合法分片引用，以及不低于该序号的 masterchainInfo 主链高度。缺失、HTTP 失败、索引滞后或身份不一致时本页不提交、不推进检查点，后续重试；Store 也拒绝没有主链序号的新增 TON confirmed 证据。

每页最多增加两次共享限速预算的查询，不为每笔交易单独请求。这里核验的是所信任 Toncenter 索引的主链归属，不宣称运行独立验证节点或校验完整的主链 Merkle/签名证明。接口参考：[交易索引](https://docs.ton.org/api/v3/blockchain-data/get-transactions)、[支付最终性要求](https://docs.ton.org/applications/payments/overview)。
