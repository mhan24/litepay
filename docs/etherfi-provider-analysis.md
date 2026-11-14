# ether.fi Cash 作为第三收款通道：`etherfi_cash.py` 分析与接入方案

> **状态：只有代码与字段形态分析，未做任何真实调用、未写任何接入代码、未产生真实资金。**
> 文中每个字段都标注了来源：取自脚本中的**观察值**（脚本作者实测后固化的字符串）或**推断**。
> 「观察值」不等于官方契约；ether.fi 没有公开的商户收款 API 文档。

---

## 0. 结论摘要

1. `etherfi_cash.py` 是一个**对 ether.fi Cash Web 前端私有 API 的客户端**，不是开放平台 SDK。
   认证靠邮箱 OTP 换取的 Cookie + `X-Sardine-Session` + `X-Account-Id` + `X-Active-User`，
   并需要过一层风控脚本（CJS）计算与远程求解服务器。**没有 API Key，没有 webhook，没有签名。**
2. 收款侧可用的只有两个面：**充值地址**（`/v2/onramp/{accountId}/crypto`，EVM 路由直接返回 Safe 地址）
   和**交易历史**（`/v3/account-safe/{safeId}/transaction-history`，`eventType=ADDING_FUNDS`）。
   两者都是只读 GET，**技术上足以实现「轮询历史 + 按金额核销」**，与现有 Binance/OKX 模型同构。
3. **最大的障碍不是接口，是金额口径**：ether.fi 对多条充值路由收取跨链桥费并在到账时换链/换币。
   发送方转出 10.01 USDT，入账可能是 10.01 − 1.40（Tron）或 10.01 × 0.9997（0.03% 桥费），
   币种链也可能从 Tron 变成 Ethereum。**「按精确金额独占档位」的前提在大部分路由上不成立。**
   因此接入范围必须先收敛到**同链同币 1:1 入账的路由**，其余一律不开。
4. 建议按 P0 事实核验 → P1 只读对账 → P2 上线收款 三段推进，P0 未通过不写生产代码。

---

## 1. 脚本做了什么

`etherfi_cash.py`（7874 行）是 ether.fi Cash 的命令行客户端，能力分四类：

| 类别 | 模式 | 与收款的关系 |
| --- | --- | --- |
| 会话与身份 | `login` `signup` `account-status` `account-locale` | **必需**：所有查询都依赖它建立会话 |
| 收款相关只读 | `onramp` `addresses` `history` `deposits` `tx-detail` `watch` | **这就是我们要复用的部分** |
| 账户与行情只读 | `balance` `overview` `assets` `features` `price` `read` `safe-tx` | 不需要 |
| 资金操作 | `quote` `withdrawal` `swap-execute` `withdraw-execute` `cctp-*` `journal` | **绝不接入**：需要 Turnkey P-256 签名与私钥 |

`watch` 模式是脚本作者已经写好的一个多信号轮询监控器（SQLite 持久化 + JSONL 事件流），
**它的历史扫描算法就是我们要的参考实现**，见第 5.4 节。

### 1.1 认证模型（接入时的第一个工程难点）

- 头：`X-Account-Id`、`X-Active-User`、`X-Sardine-Session` + Cookie（`session_<userId>`）。
- 身份自证：`GET /v2/users/me` 必须与本地 `user_id`/`account_id`/`safe.id` 三方一致，否则拒绝请求（`verify_identity`）。
- 风控：`Sardine` 会话由 `sardine_live` 模块续期；登录/注册阶段要下载 `c.js` 并用远程求解服务器
  （`_SERVER`，`cjs/prepare`、`cjs/solve`、`sardine/live`、`round59/solve`）算出六个字段的 human payload。
- 失败语义：401/403 → 会话失效；429 → **脚本明确不自动重试**；非 JSON 响应视为安全拦截页。

**结论**：这不是能用 curl 复刻的东西。接入必须把「会话供给」当作独立、可失败的依赖，
而不是像 Binance/OKX 那样把 API Key 填进后台就完事。

---

## 2. 收款地址面

### 2.1 端点

```
GET  /app/cash/api/v2/onramp/{accountId}/crypto          # 目录：networks[]，含 kind、limits、disclosures
PUT  /app/cash/api/rhino/{accountId}/{group}-deposit-address  # 仅 provider_address 路由才需要
GET  /app/cash/api/account/is-etherfi-account-address/{addr}  # 只判断是否 ether.fi 账户地址，不证明归属
```

### 2.2 两种地址形态（`deposit.kind`）

| kind | 含义 | 字段 | 是否每单不同 |
| --- | --- | --- | --- |
| `safe_address` | **直接返回 Safe 自己的地址**，`addressFormat` 必须为 `evm` | `deposit.address` | **否，固定地址** |
| `provider_address` | 第三方地址提供商（Rhino）下发，需 PUT 换地址 | `deposit.endpoint.href` + 响应 `depositAddress` | 可能固定，脚本会缓存（`provider_cache`） |

`onramp_route_eligibility` 对 `safe_address` 的校验很克制：只验 EVM 格式与非零，
并明确打印「可用，但未验证最终路由」。这符合事实——**目录里的地址不等于能收款**。

### 2.3 对收款模型的决定性影响

**ether.fi 给的是账户固定地址，不是「每单一个地址」。**
所以：

- ❌ 不能用「每单独立地址」做订单归属（很多加密收银台的做法）。
- ✅ 必须沿用 Binance/OKX 的方式：**地址固定，靠「应付金额独占档位」保证唯一性**。
  现有 `store.Select` 的档位机制（`Slot` 表 + `PayUnits` 唯一占用）可以原样复用，
  唯一要改的是 `receiver` 存 Safe 地址而不是 UID。

这一点是好消息：`model.Order.Receiver` 本来就是字符串，`store.Process` 也是按字符串等值匹配。

---

## 3. 交易历史面

### 3.1 端点

```
GET /app/cash/api/v3/account-safe/{safeId}/transaction-history
    ?transactionHistoryEvents[]=ADDING_FUNDS   # 只取充值；不要用 is_deposit() 的字符串猜测
    &page=1&limit=100                          # limit 上限 100；脚本自身限制 pages<=20
    &from=<ISO8601+tz>&to=<ISO8601+tz>         # 窗口过滤，语义未验证
    &cursor=<meta.nextCursor>                   # 游标优先于 page
    &withPendingCancellation=true&withPendingManualReview=true
    &withGranularRefundStatus=true&withDisputeStatus=true
GET /app/cash/api/v3/account-safe/{safeId}/transaction-history/{eventId}/{eventType}  # 单事件详情
```

### 3.2 字段（脚本实际读取过的，均为**观察值**）

| 字段 | 用途 | 备注 |
| --- | --- | --- |
| `id` | 事件身份（UUID） | 与 `txHash` 二选一作为 `EventID`；脚本用 `id or txHash` |
| `eventType` | `ADDING_FUNDS` 等 | 脚本列出 9 类事件；充值判断靠它 |
| `statusTag` | 状态 | 脚本观察到 `SETTLED`、`PENDING`、`PROCESSING`、`REQUESTED`、`IN_PROGRESS` |
| `timestamp` | 记录时间（ISO 字符串） | 映射到 `OccurredAt` |
| `sourceToken` / `sourceTokens` | `{symbol, address, amount, decimals, amountUSD}` | `amount` 是**原始整数**，需 `decimals` 缩放。**疑似「源链转出毛额」，核销应以此为准**（见 §10.3） |
| `outputToken` | 转换后资产 | **有它就意味着币种/链可能变了**；是扣费后净额，不用于核销 |
| `chainId` | 链 | 脚本明确标注「API chainId 不等于源链」，见 `chainAttribution` |
| `sourceTxHash` / `txHash` | 链上哈希 | 用户可核对的凭据 |
| `sourceWallet` / `recipient` / `safeAddress` | 来源与落点 | |
| `topupID` | 充值单号 | 脚本把它当查询参考号写入 JSONL |
| `explorerUrl` | 浏览器链接 | |
| `relayStatus` / `destChainId` / `destDomain` / `mintTxHash` / `executeTxHash` | CCTP 类事件 | 充值不关心 |
| `meta.nextCursor` / `meta.nextPage` | 分页 | 脚本要求 `nextPage` 单调递增，否则报错 |

### 3.3 金额口径的三个坑

1. **精度**：`sourceToken.amount` 是最高 256 位的原始整数 + `decimals`（0~30）。
   支付域只有 6 位。与 OKX 的处理方式一致：**超精度记录保留为证据（`RawAmount`）并拒绝核销，不让整次查询失败**。
2. **币种会变**：`FALLBACK_ONRAMP_DISCLOSURES` 里的官方文案已经写死了这些转换——
   Tron 的 USDT 以 Ethereum 主网 USDT 到账、Solana 的 USDC 以 Optimism USDC 到账、
   BTC 以 Optimism 上的 Liquid BTC 到账、EUR 类换成 EURC。
3. **费用会被扣**（2026-11-10 已由真实 API 输出证实）：跨链路由收 0.03%（Tron 固定 $1.40 + 0.03%，
   BTC 固定 $1 + 0.03%，>$50k 时 0.13%）。帮助中心 FAQ 那句「不收充值费」只对同链直存成立。

**这不是致命问题，因为核销应该匹配 `sourceToken`（源链转出毛额）而不是 `outputToken`（扣费后净额）。**
两者都在事件里。详见 §10.3。前提是 `sourceToken.amount` 确实是毛额——P0 必须实测确认。

**关键几何关系**：Cash Vault 在 OP Mainnet。在 Ethereum / Arbitrum / Base / BSC 上收 USDC，
本质是**跨链充值到 OP**（官方文案原文：「存款將以 Optimism 上的 USDC 形式收取」）。
真正 1:1 的只有 **OP Mainnet 上的 USDC / USDT0**，且它大概率是 `safe_address`（见 §10.6）。

---

## 4. 与 Binance / OKX 内转的对照

| 维度 | Binance（现有） | OKX（现有） | ether.fi（拟接入） |
| --- | --- | --- | --- |
| 凭据 | API Key/Secret（HMAC） | Key/Secret/Passphrase | **登录 Cookie + Sardine Session + OTP** |
| 账户自证 | `/api/v3/account` 取 `uid` | `/api/v5/account/config` 取 `uid` | `/v2/users/me` 三方一致校验 `safe.id` |
| 收款标识 | UID | UID | **Safe 地址（固定）** |
| 事件 ID | `transactionId` | `depId` | `id`（UUID）或 `txHash` |
| 参考号 | 数字 `orderId` | `fromWdId` | `topupID` / `sourceTxHash`（**无查款端点**） |
| 方向判定 | `amount` 正负 | `type=3` 请求过滤 + `fromWdId` 回填 | `eventType=ADDING_FUNDS` 请求过滤 |
| 终态判定 | 有明细即确认 | `state=2` | `statusTag == SETTLED`（**观察值**） |
| 金额是否 1:1 | 是 | 是 | **不确定**（桥费/换币） |
| 分页 | 时间窗口二分 | `after` 毫秒重叠分页 | `cursor`（+`page` 回退） |
| 推送 | 无，轮询 | 无，轮询 | 无，轮询 |

**同构点**：身份 + 币种 + 完整金额 + 时间窗口 + 唯一候选 → 原子核销。这套 `store.Process` 逻辑可以原样复用。
**不同点**：三个——(a) 凭据模型完全不同；(b) ether.fi 没有「按参考号查一笔」的端点；(c) 金额是否 1:1 存疑。

---

## 5. 接入方案（对齐现有架构）

### 5.1 新增包 `internal/etherfi`

实现与 `binance.Client` / `okx.Client` 相同的三个方法，`worker.source` 接口零改动：

```go
AccountUID(ctx) (string, error)                       // 返回 Safe 地址（作为 Receiver）
History(ctx, start, end) ([]model.Transaction, error) // ADDING_FUNDS + 窗口过滤 + cursor 翻页
Lookup(ctx, ref, start, end) ([]model.Transaction, error) // P2 再决定要不要做
```

`History` 的实现要点：

- 显式带 `transactionHistoryEvents[]=ADDING_FUNDS`，**不要用脚本的 `is_deposit()` 子串猜测**。
- `limit=100`；翻页优先用 `meta.nextCursor`，回退 `nextPage` 时必须校验单调递增（照抄脚本的 `history_pages`）。
- 任何一条记录缺 `id`、`timestamp` 不可解析、`sourceToken.amount` 非整数 → **整次查询失败，不推进游标**
  （与现有 OKX「响应契约不完整时整次失败」一致）。
- 单条记录金额超出精度或币种不在 `{USDT,USDC}` → 记为 `Rejected` + `RawAmount`，不失败。
- `statusTag != SETTLED` → `Unconfirmed`，交给现有 `recheckPending` 补查。
- `Provider = "etherfi"`，`Receiver = Safe 地址`，`Reference = topupID`（缺失则留空）。

### 5.2 必须改动的既有代码点

| 文件 | 位置 | 改动 |
| --- | --- | --- |
| `internal/config/config.go` | `ProviderBinance/ProviderOKX` 常量、`ProviderEnabled`、`ProviderInterval`、`ProviderLookback` | 加 `ProviderEtherfi` 与三条分支 |
| `internal/config/config.go` | `ScanRetention` | 现取 `max(Binance, OKX)`，改为含 Etherfi |
| `internal/store/store.go:205` | `Process` 的 provider 白名单 | `(t.Provider != "binance" && t.Provider != "okx")` 需加入 `"etherfi"` |
| `internal/worker/poller.go:62` | `client()` 的 provider 分支 | 加 etherfi 分支；**注意 etherfi 的「凭据失效」语义不同，需要单独的失败分类** |
| `internal/worker/poller.go:315` | `Run` 的平台列表 | 加入 `ProviderEtherfi` |
| `internal/server/handlers.go` | `renderCashier`、`providerOptions`、`SelectPayment`、`Ready` | 加第三平台；`Ready` 的三平台循环 |
| `internal/server/bepusdt.go:220` | `bepusdtPaymentMethods` | 加 `network: "ether.fi"` |
| `internal/settings/settings.go` | `Patch` / `Snapshot` / `Apply` / 账户核验 | 加 etherfi 会话凭据字段与核验分支 |
| `internal/server/templates/admin_settings.html` | 平台卡片 | 加 etherfi 卡片：**不是填 API Key，而是填会话凭据或会话代理地址** |
| `internal/server/templates/cashier.html` | 三处 `eq .Provider` 文案 | 从「向此 UID 发起站内转账」改为「向此地址转账，仅限 X 网络」 |
| `internal/server/templates/payment_inquiry.html` | 查款表单 | etherfi 无参考号查询，见 5.5 |

### 5.3 配置与后台

会话凭据是**会过期且需要人工过 OTP** 的东西，不能塞进 `settings` 表当普通字段。两种可行形态，二选一：

- **A（推荐，P1 阶段）**：litepay 只读一个**本地 sidecar 导出的会话文件**（cookie jar + Sardine key + safeId），
  由运维单独维护、定期刷新。litepay 不接触 OTP，也拿不到密码。会话失效时扫描器报错、订单保持待付款
  （现有 `Expire` 已经做到「扫描没覆盖就不关单」），后台显示「会话失效」。
- **B（不推荐）**：litepay 内嵌 OTP 流程。等于把账号密码之外的完整账户能力放进无人值守服务，
  一旦被入侵后果远大于只读 API Key。

**建议 A**，并在 README 里写清「这是一个私人 Web 接口，不属于官方开放平台」。

### 5.4 扫描器：复用 `watch` 的锚点算法

脚本 `watch_deposits` 的核心保证值得直接搬到 Go：

- 每轮保存**首页 N 条事件的 id 列表**（`fund_v2_anchors`）。
- 下一轮必须**与上一轮锚点有重叠**才算「窗口已连续」，否则 `ANCHOR_MISSING` → **不推进检查点**。
- 首次运行只建基线，**已存在的历史事件一律不当新到账**（`BALANCE BASELINE` 同理）。

这比现有 Binance/OKX 的裸游标更强：ether.fi 的 `cursor` 是否跨进程有效、是否有 TTL，都没有文档，
锚点重叠是唯一能自证「没漏事件」的办法。落地上建议 `model.ScanCursor` 增加一个 `Anchors` 字段
（或新建 `etherfi_anchors` 表），`ScanResult` 成功时一并写入。

同时保留脚本的边界处理：`pending_unseen > 0` 时打印警告并标记 `historyWindowVerified=false`，
绝对不要在这种状态下核销。

### 5.5 收银台与查款

| 项 | Binance/OKX | ether.fi |
| --- | --- | --- |
| 收款方 | UID | Safe 地址（长地址，建议给复制按钮 + 二维码） |
| 必填提示 | 「站内转账、选币种、填精确金额」 | **「仅限 {network} 网络、{token}、精确金额」**（网络写错资金丢失） |
| 查款 | 填 orderId / fromWdId | **无按参考号查询端点** |

**查款建议**：P1 不做用户侧查款，只做「自动扫描 + 后台人工核对」。
理由：`tx-detail` 必须同时有 `eventId`(UUID) 和 `eventType`，用户手上的 `txHash` 不能直接查；
在历史里按 `sourceTxHash` 搜要翻页，成本不可控。等 P0 验证出 `from`/`to` 窗口过滤语义后，
可以做成「按订单付款窗口拉一次历史，匹配哈希或金额」，仍然只是兜底，不承诺实时。

---

## 6. 分阶段落地

### P0 · 事实核验（不写生产代码，可丢弃脚本）

全部用 `etherfi_cash.py --mode` 手工执行，产出**真实样本 JSON** 存档：

1. `--mode onramp --network <op|arb|bsc|...> --token USDT` → 确认哪些路由 `status=enabled`、`kind` 是什么。
2. `--mode addresses --network <op> --token USDC` → 拿到 `safe_address`，确认就是 Safe 地址。
3. **最小额真实充值一次（同链同币，例如 OP 上 USDC），记录：转出额 / `sourceToken.amount` / `outputToken` / `statusTag` 变化序列 / 到账延迟。**
   → **这一步直接决定项目是否继续**：必须确认 1:1 入账，否则金额档位方案不成立。
4. 再充一笔 Tron USDT 小额，记录桥费扣减的实际数字（验证 `$1.40 + 0.03%`）。
5. 观察 `statusTag` 是否出现 `PENDING → SETTLED` 中间态，以及中间态时 `sourceToken` 是否已完整。
6. 验证 `from`/`to` 参数的**时区与包含性**（左闭右闭？UTC？）；验证 `cursor` 重启后是否仍可用。
7. 验证并发：两个订单同时等待时一轮扫描能否读到两条记录（`limit` 与窗口的边界）。
8. 记录「从链上广播到 history 出现」的延迟分布 → 决定轮询间隔与 `LookbackBuffer` 默认值。

### P1 · 只读对账（写代码，但后台开关默认关闭）

- 新增 `internal/etherfi`、config/settings/worker/store 的接线、后台卡片只读展示。
- **不允许下单**：`handler.SelectPayment` 拒绝 `provider=etherfi`。
- 目的：用真实流量跑通「拉历史 → 落库 → 匹配档位（dry-run，不核销）」，看后台事件表。

### P2 · 打开收款

- 只对 P0 验证通过的路由开放；`onramp` 目录里 `status != enabled` 或非白名单网络直接不展示。
- 收银台强制显示网络 + 币种 + 地址三要素，网络写错的风险文案必须显式。
- 仍然保持：会话失效 → 不关单、不误判。

---

## 7. 风险清单（按严重度）

| # | 风险 | 影响 | 处置 |
| --- | --- | --- | --- |
| 1 | **实收 ≠ 应付**（桥费/换币） | 核销永远不匹配，用户已付但订单不确认 | P0-3/P0-4 验证；只开 1:1 路由 |
| 2 | **私人 Web API，非官方开放平台** | 随时改字段/加风控/封号；无 SLA、无支持 | README 与后台明确标注；接口全部做严格契约校验，字段变了就整次失败 |
| 3 | **会话靠 OTP，无人值守会失效** | 扫描停摆，订单积压 | 现有 `Expire` 已保证「未覆盖不关单」；后台告警 + sidecar 续期 |
| 4 | 风控（Sardine/CJS/Cloudflare）拦请求 | 403/非 JSON，查询失败 | 失败即不推进游标；不做自动重试（脚本的 429 处理就是先例） |
| 5 | 资金来源合规文案 | 官方明确「仅限本人账户入金，第三方入金将被退回」 | **这条对收银台是致命的**：P0 就要确认商户收款是否被允许，必要时放弃该通道 |
| 6 | 地址固定 + 金额档位 | 同一时间档位耗尽即无法下单 | 现成逻辑已处理（`maxOffset` 用尽则拒绝） |
| 7 | 单人维护的远程求解服务器 | 登录阶段单点依赖 | 只在登录时用；若走 sidecar 方案，litepay 完全不碰 |
| 8 | 无法按参考号查款 | 用户申诉时只能后台人工翻 | P1 明确不提供；后台保留完整证据链（`sourceTxHash` + `explorerUrl`） |

**第 5 条请优先确认**：`first_topup_own_account` / `own_account_deposits_only` 这两条官方提示
意味着「别人给你这个地址转钱」在 ether.fi 的规则里是要被退回的。如果这是硬规则，
那么把 ether.fi 当收银台收款通道就是不成立的——**这一点比任何技术细节都优先**。

---

## 8. 一句话回答「能不能做」

技术上能做：`History` 的字段够、`cursor` 翻页够、`store.Process` 的核销模型能复用，
`watch` 模式已经证明「按历史事件增量监控」这条路走得通。

但**先回答两个业务问题再写代码**：
(1) 同链同币直存是否 1:1 入账？
(2) ether.fi 规则是否允许「他人向我的地址转账」用于商户收款？

任一为否，这个通道就应该停在 P0，不要进入代码阶段。

---

## 9. 网络清单核验：ETH / Arbitrum / Base 的 USDC 到底有没有

> 依据：官方帮助中心 [Supported assets and networks for your Vault with Cash](https://help.ether.fi/en/articles/758804-supported-assets-and-networks-for-your-vault-with-cash)（页面标注 "Updated over 2 weeks ago"）。
> 早先那篇 `articles/833702-usdc-and-usdt-deposit-networks-and-addresses` 已被删除（两次抓取均 404），以 758804 为准。

### 9.1 官方口径：有

| 资产 | 官方列出的支持网络 |
| --- | --- |
| **USDC** | **Ethereum、OP Mainnet、Arbitrum、Base**、BNB Smart Chain、Celo、Gnosis、HyperEVM、Katana、Mantle、Polygon、Solana、Tempo |
| USDT | Ethereum、OP Mainnet、Arbitrum、Base、BNB Smart Chain、Celo、Gnosis、HyperEVM、Kaia、Katana、Mantle、Plasma、Polygon、Solana、Tempo、Tron |
| USDT0 | OP Mainnet、Arbitrum、HyperEVM |
| EURC | Ethereum、OP Mainnet、Base、Solana（Base 与 Solana 走后第三方提供商） |

官方原文还有一句关键的限定：

> "Some networks use a provider-backed deposit route before funds arrive in Cash."

这句话直接对应 API 里的 `deposit.kind = provider_address`（Rhino 第三方）。

### 9.2 为什么你会觉得「没有」

`etherfi_cash.py` **不硬编码任何网络清单**——目录完全来自运行时
`GET /v2/onramp/{accountId}/crypto` 的 `networks[]`。所以「有没有」只能实测，脚本源码回答不了。
但可以定位到几类典型原因：

| 现象 | 原因 | 证据 |
| --- | --- | --- |
| 目录里看得到 arb/base/eth 的 USDC，但**没有地址** | 这些路由是 `provider_address`，而 `addresses` 模式只有在**恰好选中一个 network+token** 时才去 PUT 取地址 | `addresses()` 中 `fetch_provider = len(selected) == 1 and bool(args.network or args.token)` |
| 批量列目录时显示 `address not fetched (select exact route)` | 同上 | 同上分支 |
| 路由存在但 `NOT SELECTABLE` | `status != 'enabled'`，或 `statusReasonKey` 有地区/账户限制 | `onramp_route_eligibility()` |
| 取到地址但被 `BLOCKED` | provider 返回的 `depositChains` / `supportedTokens` / `isActive` 与路由不匹配 | `validate_provider_route()` |
| 目录里根本没有这条 | 无法由脚本决定；可能是账户/地区未开放，或已下线 | 只能实测 |

另外要注意：**EVM 类 provider 路由是「一条共享地址」**——
`PROVIDER_DEPOSIT_NETWORKS` 把 `ethereum/arbitrum/base/bsc/celo/gnosis/mantle/polygon/...` 全部
映射到同一个 `evm-deposit-address` 端点。也就是说如果你看到一个共享的 EVM 充值地址，
它同时覆盖 Ethereum / Arbitrum / Base / BSC 等多条链，而不是每个网络一个地址。

### 9.3 费用口径存在冲突

| 来源 | 说法 |
| --- | --- |
| 帮助中心 FAQ（[Fund Your Vault with Crypto](https://help.ether.fi/en/articles/637846-fund-your-vault-with-crypto)） | "We do not charge deposit fees. You will only pay blockchain gas fees." |
| 脚本 `FALLBACK_ONRAMP_DISCLOSURES` | 通用桥费 0.03%；Tron 固定 $1.40 + 0.03%；BTC 固定 $1 + 0.03% |

两者都是 ether.fi 的说法。合理解释是：**同链直存不收，跨链/桥接收**，而脚本里的那份是**旧快照**（变量名就叫
`FALLBACK_`）。但**这只是推断，未验证**。

### 9.4 对收款方案的实际影响（这是本节的重点）

Cash Vault 在 **OP Mainnet**。于是：

- **OP Mainnet 上的 USDC / USDT0** → 同链直存，最可能 1:1，**这是唯一值得作为收银台通道的路由**。
- **Ethereum / Arbitrum / Base 上的 USDC** → 资金必须先跨到 OP 才算入账，
  即「有 USDC 入口」≠「实收金额等于转出金额」。这三条恰恰是我在第 3.3 节提醒过的风险路由。

所以「arb / base / eth 有没有 USDC」的答案是**有**，但**它不能直接用来做精确金额核销**。
真正要问的是：**OP Mainnet 上的 USDC 是不是 1:1 入账？** 这才是 P0 的第一问。

### 9.5 用脚本实测的具体命令

```sh
# 1) 列出所有 enabled 的稳定币路由（看有没有 arb/base/eth/op）
python3 etherfi_cash.py --mode onramp --token USDC

# 2) 单条路由的完整规则：状态、地址类型、min/max、全部 disclosures
python3 etherfi_cash.py --mode onramp --network arbitrum --token USDC
python3 etherfi_cash.py --mode onramp --network base      --token USDC
python3 etherfi_cash.py --mode onramp --network ethereum  --token USDC
python3 etherfi_cash.py --mode onramp --network optimism  --token USDC   # 注意 networkKey 的真实拼写以目录为准

# 3) 取实际充值地址（必须精确到单条 network+token，否则不取）
python3 etherfi_cash.py --mode addresses --network arbitrum --token USDC --raw

# 4) 只筛 provider-backed 路由：--chain evm / solana / tron / bitcoin / stellar / eur
python3 etherfi_cash.py --mode addresses --network arbitrum --token USDC --chain evm --raw
```

第 2 步的输出会直接给出 `Address availability:` 与 `BLOCKED:` 原因，
比目测 App 界面可靠。**把 `--raw` 的 JSON 存档**，它就是 P0 的证据。

**仍需补一条未验证点**：`--network optimism` 这个写法是否命中，取决于目录里
`networkKey` 的真实取值（脚本用 `networkKey` 做 Rhino 映射，`matching_networks` 则同时匹配
`id / networkKey / networkName / chainId`）。以目录实际输出为准。

---

## 10. 实测样本（2026-11-10，来自真实 `onramp` 输出）

### 10.1 BSC USDC —— 已验证

```
僅限從 BNB Smart Chain (BSC) 網路存入 USDC。存入其他資產或從其他網路存入的資金可能會遺失。
注意：USDC（BNB Smart Chain (BSC)）存款將以 Optimism 上的 USDC 形式收取。
此網路的存款將收取 0.03% 的跨鏈手續費。
最低存款 $0.50 · 最高 $10,000,000（每筆轉帳；金額不累計）。
```

这一条**推翻了帮助中心 FAQ 的「不收充值费」说法**：跨链路由确实收 0.03%，并且确实换链
（BSC 的 USDC 以 Optimism 上的 USDC 入账）。FAQ 那句话只对同链直存成立。

对应到脚本的 disclosure key：`crypto_deposit_network_warning` + `crypto_same_token_conversion`
+ `crypto_bridge_fee` + `crypto_deposit_limits` 四条模板同时命中——**说明 `FALLBACK_ONRAMP_DISCLOSURES`
不是过时快照，四个模板都还在线上生效**。第 9.3 节的「旧快照」推断作废。

### 10.2 Arb USDC —— 只拿到第一行（用户输入被截断）

```
僅限從 Arbitrum 網路存入 USDC。存入其他資產或從其他網路存入的資金可能會遺失。
```

**缺**：是否也有 `crypto_same_token_conversion`（Arbitrum→Optimism 换链）、是否也有 0.03%、
min/max 是多少。需要补完整输出。

### 10.3 最重要的一条结论：核销应该匹配 `sourceToken`，不是 `outputToken`

0.03% 手续费 + 换链，看起来会毁掉「按精确金额独占档位」。**但其实不会**，因为事件里有两个金额：

| 字段 | 含义 | 是否精确等于用户输入 |
| --- | --- | --- |
| `sourceToken.amount` / `sourceTokens[]` | **用户在源链转出的金额（毛额）** | **是**，与费率无关 |
| `outputToken.amount` | 实际入账到 Vault 的金额（净额） | 否，已被扣 0.03% 且换了链 |

脚本自己的命名和展示方式就是这个语义：

- `history_event_display_summary()` 打印 `source=/out=` 两组；
- `watch_deposit_summary()` **用 `sourceToken` 算展示金额**，`outputToken` 完全没参与。

所以核销口径应该是：

```
EventID   = id（UUID）
Currency  = sourceToken.symbol（必须 ∈ {USDT, USDC}）
Units     = sourceToken.amount 按 sourceToken.decimals 缩放
OccurredAt= timestamp
Reference = topupID
```

这样 0.03% 只影响**商户实际到手的钱**，不影响**订单是否被正确认领**。

**但这是推断，未验证。** P0 必须确认：`sourceToken.amount` 到底是「转出毛额」还是「扣费后净额」。
只要它是毛额，跨链路由就完全可用；如果它是净额，跨链路由必须放弃（见 10.5）。

### 10.4 换链带来的两个真实后果（不影响核销，但影响账）

1. **账实不符**：$10.01 的订单，Vault 里只多 $10.006997。每 $10 少 $0.003。
   对账表必须同时存 `sourceToken`（约定金额）和 `outputToken`（实际到账），不能只存一个。
2. **币种判定要小心**：`tron_mainnet_conversion` 说明 Tron USDT 以 Ethereum 主网 USDT 入账，
   符号不变但链变了。若将来出现「换币」路由（`crypto_wrapped_conversion`：
   BTC → liquidBTC），`outputToken.symbol` 就不再是 USDT/USDC，必须靠 `sourceToken.symbol` 判定。

### 10.5 硬约束：限额与路由白名单

| 约束 | 值 | 对 litepay 的影响 |
| --- | --- | --- |
| 最低存款（BSC USDC） | **$0.50** | 现有 `order.min_amount` 默认 `0.01`，低于 0.50 的订单在该路由根本付不了。**必须按 provider 分别设最小额**，现在 `MinAmount` 是全局字段 |
| 最高存款（BSC USDC） | $10,000,000 | 无影响（支付域上限 `MaxMall` = 1,000,000） |
| 计费方式 | 每笔转账，金额不累计 | 与「一单一笔转账」的模型一致，没问题 |

### 10.6 一条尚未用到的硬证据：OP Mainnet 不可能是 provider 路由

`PROVIDER_DEPOSIT_NETWORKS` 里**没有 `optimism` / `op` 这一项**：

```
ethereum, arbitrum, arc, avalanche, base, bsc, celo, gnosis, kaia,
katana, mantle, plasma, polygon, solana, bitcoin, stellar, tron, tempo, eur
```

而 `onramp_route_eligibility()` 对 `kind == 'provider_address'` 的路由要求
`network_key in PROVIDER_DEPOSIT_NETWORKS` 且 `provider_group` 与 `addressFormat` 一致，
否则直接判为不可用。

**推论**：OP Mainnet 的 USDC/USDT0 路由不可能是 `provider_address`，只能是 **`safe_address`**
——也就是**直接转到你自己的 Safe 地址**。那就不是「充值」，而是一笔普通的 ERC-20 转账：

- 没有 0.03%、没有换链、没有第三方；
- **金额天然 1:1**，由链上保证，`sourceToken.amount == outputToken.amount`；
- 这才是唯一真正适合精确金额核销的路由。

**验证方法**（两条命令对比地址）：

```sh
python3 etherfi_cash.py --mode balance                                   # 记下 Safe Address
python3 etherfi_cash.py --mode addresses --network op --token USDC --raw # 看 deposit.kind / deposit.address
```

若 `kind == safe_address` 且地址与 Safe Address 相同 → 结论成立。

### 10.7 修正后的路由分级

| 路由 | 类型 | 费率 | 核销可行性 | 建议 |
| --- | --- | --- | --- | --- |
| **OP Mainnet USDC / USDT0** | `safe_address`（推断） | 无（只有 gas） | **高**：金额 1:1 | **首选，P2 只开这一条** |
| BSC / ARB / ETH USDC | `provider_address`（Rhino 共享 EVM 地址） | 0.03% + 换到 OP | **中**：取决于 `sourceToken` 语义 | P1 只做只读对账，不放开下单 |
| Tron USDT | provider | $1.40 + 0.03% + 换链 | 低：固定费在大额下也显著 | 不开 |
| BTC | provider | $1 + 0.03%（>$50k 为 0.13%）+ 换成 liquidBTC | 低：币种都变了 | 不开 |

**一句话**：ETH/ARB/BASE 的 USDC 是有的，但它们都是「跨链充值到 OP」，
真正能当收银台用的是 **OP Mainnet 自己**。

---

## 11. 链上实测：充值地址 `0x39fa82…F0DF`（2026-11-10）

> 全部来自公共 RPC 的只读 `eth_call` / `eth_getCode`，**不需要任何登录凭据**。
> 这一节把第 10.6 节的推断变成了观测事实。

### 11.1 观测结果

| 检查项 | OP Mainnet | Arbitrum | Base | Ethereum |
| --- | --- | --- | --- | --- |
| `eth_getCode` | **有，242 字节代理** | **无** | **无** | — |
| `nonce` | 1 | 0 | 0 | — |
| USDC `balanceOf` | 0 | 0 | 0 | 0 |

合约身份（在 OP 上调用其 ABI）：

| 调用 | 返回 | 含义 |
| --- | --- | --- |
| `getOwners()` `0xa0e67e2b` | `[0xab70ba0911f5dd50e8677ebf39c1110feb21f872]` | 单 owner |
| `getThreshold()` `0xe75235b8` | `1` | **1-of-1，Safe 兼容账户** |

### 11.2 结论：这就是 Vault 本身，且只在 OP 部署

地址是 OP Mainnet 上的一个 **1-of-1 Safe 兼容合约钱包**，且 **Arbitrum / Base 上没有任何代码**。

所以「这条路由没有桥费提示」不是错觉，而是必然：

- 往它在 **OP** 上转 USDC = 一笔普通 ERC-20 转账，钱直接进你自己的 Safe，
  ether.fi 全程不介入 → **无跨链、无 0.03%、金额由 EVM 保证 1:1**。
- BSC / ARB / ETH 那几条带 0.03% 的路由是**另一个地址**（Rhino 的共享 provider 地址），
  不是这个 Safe。两者观感完全不同是正常的。

第 10.6 节的推断（「OP 不在 `PROVIDER_DEPOSIT_NETWORKS` → OP 只能是 `safe_address`」）
由此闭环：**OP 路由 = 直接转账到自己的 Safe。**

### 11.3 新发现：这个 Safe 是可升级的（非标准 Safe）

代理字节码把 beacon 地址硬编码在合约里，通过 `implementation()` 取实现再 delegatecall：

```
代理 → beacon 0x48e1630278d8310c86650405ebfbc110cdf3d1ec
     beacon.owner() = 0xf4e147db314947fc1275a8cbb6cde48c510cd8cf   （ether.fi 控制的 EOA）
     beacon.implementation() = 0x79a6d20280e4b564ea1d8f5369c6544887f7397e （23,980 字节）
```

beacon 自身暴露 `upgradeTo(address)` + `transferOwnership(address)`（`Ownable`）。

**含义**：标准 Gnosis Safe 代理把 singleton 存在 storage slot 0 且不可升级；
这个变体从外部 beacon 取实现，**beacon owner（ether.fi）可以单方面更换 Vault 的实现合约**。

对收款匹配逻辑没有直接影响（仍按地址 + 金额认定），但它是「资金不完全由用户私钥控制」的硬证据，
应记入风险表。接入前建议向 ether.fi 确认该 beacon 的升级治理流程。

### 11.4 一条绝对禁止的操作

**不要把这个地址当作 Arbitrum / Base / Ethereum 的充值地址直接转账。**

该地址在这几条链上**没有部署代码**，也不在 ether.fi 的 provider 路由中。ERC-20 转账不要求
目标合约存在，钱会真的写进那个地址的余额，然后**没有任何人能取出来**——除非 ether.fi 事后
在那些链上反事实部署同一个 Safe（不能指望）。

要收 Arb / Base 的 USDC，只能用 App 里那条明确提示
「將以 Optimism 上的 USDC 形式收取」+ 0.03% 的路由，对应的是**另一个地址**。

### 11.5 一个反直觉的推论

既然 OP 路由是「直接转给自己的 Safe」，那么：

- **ether.fi 的充值历史里能否看到这笔到账，取决于他们是否索引了该 Safe 的原生转账。**
  钱确实到账了（链上余额变了），但 `ADDING_FUNDS` 事件是否立刻生成、`statusTag` 走什么状态，
  **仍然只能用真实充值验证**。这是 P0-3 剩下的唯一未知项。
- 反过来说，**如果 ether.fi 的索引出问题，链上余额是对的、API 却看不到** ——
  这种情形下 litepay 会一直不确认。建议 P1 起就把「链上 `balanceOf` 变化」作为
  **旁证信号**（只告警、不核销），这与脚本 `watch` 模式同时监控 `safe-details` 余额的设计一致。
