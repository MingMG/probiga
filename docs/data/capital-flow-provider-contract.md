# Daily capital-flow acquisition

The collector first asks Eastmoney for the exact date. A transport failure or
an identity-valid response without that date invokes Sina's dated distribution
API. A malformed response or an identity mismatch stops publication. Within one
batch, a broken Eastmoney transport is tried only by requests already in flight;
later stocks use the alternate. The next acquisition tries Eastmoney again.
Sina requests are limited to four per second and four concurrent workers by
default. One 100-session history page is reused across missing dates for the
same stock during the acquisition process.

A wholly empty current batch leaves all target stocks for the same exact-date
alternate chain. It cannot cause yesterday's rows to be reused as current data.
All newly acquired frames also check main = large + superlarge before any
database write, using the persisted partition inspector's accounting tolerance.
This prevents an invalid provider row from first entering storage and only
being detected by the later repair inspector.

Sina's public historical API does not echo a symbol. The collector binds each
response to its actual HTTPS URL, market-qualified stock, history method, page,
ordering and native `opendate`; redirects, duplicate dates, missing values and
inconsistent native bucket totals fail validation. This is request-bound source
evidence, not a claim that the response contains a stock code.

The native API values are CNY. Its web client divides these by 10,000 for the
display. The four trade-size buckets map as follows:

| Native field | Native size | Stored field |
| --- | --- | --- |
| `r0_net` | over CNY 1 million | `max_net_inflow` |
| `r1_net` | CNY 200,000 to 1 million | `lg_net_inflow` |
| `r2_net` | CNY 50,000 to 200,000 | `mid_net_inflow` |
| `r3_net` | below CNY 50,000 | `sm_net_inflow` |

The common main bucket is `r0_net + r1_net`. It is neither Sina's all-size
`netamount` nor Sina's own differently defined "main" label. No missing field
is converted to zero. The four native net buckets must sum to `netamount`, and
each absolute bucket net must not exceed that bucket's native turnover.

Provider algorithms differ. A stock/date row has one provider, stored in
`data_source`; complete partitions may contain several providers. New Sina rows
use `sina_l1` and are never relabelled as Eastmoney. Existing labelled rows,
including the historical `east`, `east_min_close`, `baidu` and `push2hist` records,
retain their values and identity. Receipts hash the source with each row and
record the observed providers and their semantics. This policy does not assert
that two providers report identical amounts, and does not produce a synthetic
flow from OHLC data. The legacy Baidu helper is not an acquisition fallback.

Historical repair writes only missing or invalid target identities in an atomic
transaction, then verifies the whole traded A-share universe and hashes the
preserved rows. Older B-share/nontraded rows outside that universe are retained;
they neither satisfy an A-share gap nor prevent filling it. Fresh provider
frames still reject unexpected codes. QMT-native tasks retain their native
requirements; this alternate belongs to public daily-flow acquisition.

Native references: [Sina field definitions](https://finance.sina.com.cn/temp/guest4377.shtml)
and [the public historical-distribution client](https://n.sinaimg.cn/finance/cnstock/pc/zjlx.z.js).

Release boundary: cross-end, because the collector, historical repair proof and
shared scheduler receipt validation consume the same provider contract. No
database schema, QMT login or task ownership is changed.
