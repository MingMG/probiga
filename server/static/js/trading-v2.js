(function () {
  'use strict';
  var accountId = 'paper-main-v2';
  var state = {};
  var labels = {
    QMT_DAILY_KLINE_NOT_ATTESTED: ['QMT 日 K 尚未完成逐行补证', '旧行情不能冒充国金 QMT 真值，因此禁止新仓。'],
    QMT_DAILY_KLINE_ATTESTATION_INCOMPLETE: ['QMT 日 K 补证覆盖不完整', '仅匹配成功的行可用于推广和交易。'],
    B_001_ACTUAL_BROKER_FEES: ['实际券商费率未确认', '未确认佣金、最低佣金等费用前不计算“实际净收益”。'],
    'B-001_ACTUAL_BROKER_FEES': ['实盘券商费率未确认', '只影响未来真实交易，不再阻塞 ProBigA 模拟盘。'],
    'B-002_ACCOUNT_INSTRUMENT_PERMISSIONS': ['实盘品种权限未确认', '只影响未来真实交易，不再阻塞 ProBigA 模拟盘。'],
    'B-003_RELIABLE_LEVEL1_BID_ASK': ['实盘 Level-1 连续性未验收', '模拟盘可降级使用新鲜 QMT 快照并计入冻结滑点。'],
    PAPER_FEE_PROFILE_MISSING: ['模拟费用规则缺失', '内部模拟盘必须绑定一套明确且冻结的费用假设。'],
    PAPER_INSTRUMENT_RULES_MISSING: ['模拟品种规则缺失', '手数、T+1、涨跌停与可买范围尚未绑定。'],
    NO_PAPER_ENABLED_STRATEGY: ['没有启用模拟试运行策略', '至少一套策略必须进入“模拟试运行”或“模拟运行”状态。'],
    MARKET_REGIME_EXTREME: ['当前市场状态为极端风险', '模拟盘已启用，但本期策略纪律要求暂不开新仓。'],
    MARKET_REGIME_DATA_BLOCKED: ['当前市场状态缺少可靠数据', '数据恢复前不创建新仓，已有持仓仍按风控处理。'],
    'V2_JOB_WORKER_MISSING': ['V2任务执行器未运行', '回测和决策任务不会在接口请求中临时补算。'],
    'V2_JOB_WORKER_STALE': ['V2任务执行器心跳过期', '执行器恢复并重新验收前禁止新增风险。'],
    DATA_QUALITY_BLOCK: ['数据质量阻断', '影响价格、时序或规则的缺口不能降级为警告。'],
    DATA_SNAPSHOT_MISSING: ['决策快照缺失', '后台执行器尚未生成可还原的输入快照。'],
    V2_ACCOUNT_MISSING: ['V2 主模拟账户缺失', '独立 20 万元账户尚未初始化。'],
    RECONCILIATION_BLOCKED: ['账本对账未通过', '对账恢复前禁止新开仓和加仓。']
  };
  var actionLabels = {
    BUY:'买入', BUY_READY:'买入条件已确认', OPEN:'买入', HOLD:'观望', WATCH:'观察',
    SELL:'卖出', REDUCE:'减仓', ADD:'加仓', EXIT:'退出',
    RESEARCH_ONLY:'仅研究', DATA_BLOCKED:'数据阻断',
    NO_BUY:'不买入', ACTIVATE_PROBE:'转为小仓试买',
    ACTIVATE_SUBSTITUTE:'龙二/中军小仓套利',
    ACTIVATE_REVERSAL_PROBE:'水下修复小仓试买',
    ACTIVATE_VOLUME_PROBE:'爆量上攻小仓试买', REJECT:'拒绝'
  };
  var statusLabels = {
    ACTIVE:'正常', IDLE:'空闲', PASS:'通过', PASSED:'通过',
    OK:'正常', COMPLETED:'已完成', BLOCK:'阻断', BLOCKED:'已阻断',
    DATA_BLOCKED:'数据阻断', REJECTED:'未通过', ELIGIBLE:'可参与组合',
    PAPER_TRIAL_ELIGIBLE:'可进入模拟竞争', RESEARCH_ONLY:'仅研究',
    PENDING:'等待执行', RUNNING:'执行中', FAILED:'失败',
    ERROR:'异常', QUEUED:'排队中', WAITING:'等待条件',
    FILLED:'已成交', PARTIALLY_FILLED:'部分成交', EXPIRED:'已过期',
    CANCELLED:'已撤销', RISK_APPROVED:'风控通过',
    PAPER_TRIAL:'模拟试运行', PAPER_ACTIVE:'模拟运行',
    SHADOW:'影子观察', OOS_PASSED:'样本外验证通过',
    RESEARCH:'研究验证', DRAFT:'草稿', DRAFT_BLOCKED:'草稿阻断',
    SUSPENDED:'已暂停', CONFIG_BLOCKED:'配置阻断',
    RECONCILIATION_BLOCKED:'对账阻断', REGISTERED:'已登记',
    COLLECTING:'正在积累', DEGRADED:'降级', PARTIAL:'部分完成',
    VALID:'趋势有效', VALID_STRONG:'趋势强，可继续持有',
    VALID_WEAK:'趋势转弱，密切观察', EXIT_PENDING_T1:'趋势失效，等待 T+1 卖出',
    EXIT_PENDING_LIQUIDITY:'等待流动性后卖出',
    EXIT_COMMITTED:'退出指令已提交', CLOSED:'已平仓'
    ,WATCHING:'继续观察', ACTIVATABLE:'满足盘中试仓条件',
    ORDER_CREATED:'模拟订单已创建', RISK_REJECTED:'组合风控未通过'
  };
  var regimeLabels = {
    TREND_UP:'趋势上行', THEME_ROTATION:'题材轮动',
    RANGE:'震荡整理', PANIC_RECOVERY:'恐慌修复',
    RISK_ON:'积极', NEUTRAL:'中性', RISK_OFF:'防守',
    EXTREME:'极端风险', DATA_BLOCKED:'数据阻断'
    ,OBSERVING:'等待盘中确认', BROAD_RALLY_CONFIRMED:'普涨已确认',
    PANIC_RECOVERY_CONFIRMED:'恐慌修复已确认'
  };
  var strategyLabels = {
    ultra_short:'超短确认策略', short_term:'短线策略',
    swing:'波段策略', main_wave:'主升浪策略',
    sector_preheat:'板块预热与龙头梯队',
    intraday_dynamic_activation:'盘中动态激活',
    etf_trend_risk:'ETF 趋势风控组合'
  };
  var reasonLabels = {
    MULTI_STRATEGY_REGIME_EXTREME:'极端风险状态禁止新增股票仓位',
    MULTI_STRATEGY_REGIME_DATA_BLOCKED:'关键数据不足，禁止新增股票仓位',
    MULTI_STRATEGY_NOT_BUY:'该策略本轮没有形成买入方向',
    MULTI_STRATEGY_HARD_GATE_BLOCK:'个股硬风险门槛未通过',
    MULTI_STRATEGY_EVENT_RISK_BLOCK:'个股事件风险过高，禁止买入',
    MULTI_STRATEGY_SECTOR_SIGNAL_NOT_READY:'板块预热信号尚未完成确认',
    MULTI_STRATEGY_DISABLED_FOR_REGIME:'该策略不适合当前市场状态',
    MULTI_STRATEGY_SIGNAL_NOT_CONFIRMED:'策略信号尚未达到当前市场的确认要求',
    MULTI_STRATEGY_REDUCE_NOT_ROUTABLE:'该观察信号不是单纯由市场状态降权，不能升级为模拟买入',
    MULTI_STRATEGY_GATE_NOT_ROUTABLE:'当前策略门槛状态不能参与仓位竞争',
    MULTI_STRATEGY_SCORE_BELOW_REGIME_MIN:'策略评分低于当前市场状态的买入门槛',
    MULTI_STRATEGY_RISK_REWARD_BELOW_MIN:'预期盈亏比不足，不值得承担本次风险',
    MULTI_STRATEGY_DATA_QUALITY_BELOW_MIN:'数据质量不足，暂不进入模拟买入',
    MULTI_STRATEGY_ZERO_REGIME_WEIGHT:'该策略在当前市场状态下的有效权重为零',
    DUPLICATE_PENDING_ENTRY:'已有同一股票的待成交模拟买单，不重复下单',
    SIGNAL_NOT_BUY_READY:'买入信号尚未确认',
    QMT_DAILY_KLINE_NOT_ATTESTED:'QMT 日 K 逐行补证未通过',
    QMT_DAILY_KLINE_ATTESTATION_INCOMPLETE:'QMT 日 K 补证覆盖不完整',
    OOS_EXPECTED_RETURN_LOWER_BOUND_MISSING:'样本外收益下界缺失或不为正',
    LOWER_RANKED_SAME_STOCK_SIGNAL:'同一股票存在排名更高的策略信号',
    FOUR_SLOT_COMPETITION_CAP:'四个仓位名额已被更优候选占用',
    INSTRUMENT_RULE_BLOCKED:'交易规则不允许当前操作',
    THEME_CLASSIFICATION_MISSING:'缺少可靠的题材分类',
    STOP_OR_ENTRY_INVALID:'入场价或保护位无效',
    TARGET_QUANTITY_BELOW_MINIMUM_LOT:'按目标仓位不足一手，未创建无效订单',
    MINIMUM_BOARD_LOT_EXCEEDS_RADAR_CAP:'最少一手也超过小账户异动试仓上限',
    FEE_PROFILE_UNCONFIRMED:'费用规则尚未确认',
    CASH_CHANGED_AFTER_APPROVAL:'风控通过后可用现金发生变化',
    WAIT_T1:'受 T+1 限制，下一交易日才能卖出',
    WAIT_LIMIT_UP:'涨停封单，当前无法买入',
    WAIT_LIMIT_DOWN:'跌停封单，当前无法卖出',
    WAIT_SUSPENDED:'证券停牌，当前无法成交',
    WAIT_STALE_QUOTE:'行情过期，等待新鲜报价',
    WAIT_NO_QUOTE:'没有可靠报价，暂不成交',
    WAIT_LIQUIDITY:'流动性不足，等待可成交机会',
    WAIT_SECTOR_CONFIRMATION:'板块盘中强度尚未确认，暂不成交',
    WAIT_ENTRY_TREND_INVALID:'价格已跌破入场保护线，禁止接下跌单',
    MARKET_REGIME_ALLOWS_RISK:'当前市场状态不允许新增风险仓位',
    ACCOUNT_ACTIVE:'模拟账户未处于正常状态',
    RECONCILIATION_PASS:'账本对账尚未通过',
    FOUR_SLOT_COMPETITION_WINNER:'四仓竞争胜出'
    ,MARKET_NOT_CONFIRMED:'市场盘中修复尚未确认',
    QMT_QUOTE_MISSING:'缺少可靠的 QMT 分钟价格',
    LEADER_LIMIT_LOCKED:'龙头接近涨停，当前不追板',
    SPECIAL_TREATMENT_BLOCKED:'ST 或退市整理股票禁止试仓',
    RAW_SCORE_TOO_LOW:'盘前候选分不足',
    INTRADAY_STRENGTH_TOO_LOW:'个股盘中强度不足',
    INTRADAY_TOO_EXTENDED:'个股盘中涨幅过大，避免追高',
    RELATIVE_STRENGTH_TOO_LOW:'个股没有明显跑赢市场',
    INTRADAY_VOLUME_NOT_CONFIRMED:'当前分钟量能没有放大',
    RISK_REWARD_TOO_LOW:'按当前价格计算的盈亏比不足',
    REFERENCE_PRICE_TOO_EXTENDED:'距离盘前参考价过远',
    THEME_SAMPLE_TOO_SMALL:'板块可观测成员不足',
    THEME_BREADTH_NOT_CONFIRMED:'板块上涨宽度不足',
    THEME_RETURN_NOT_CONFIRMED:'板块平均涨幅不足',
    LEADER_UNAVAILABLE:'龙一不可成交，禁止强追',
    LEADER_UNAVAILABLE_CORE_SUBSTITUTE:'龙一不可成交，改选同板块龙二或中军小仓试错',
    SUBSTITUTE_ROLE_NOT_ALLOWED:'龙一买不到，但该股票不属于允许递补的龙二、中军或低位替补',
    SUBSTITUTE_SCORE_GAP_TOO_LARGE:'递补股票与龙一的盘前评分差距过大',
    SUBSTITUTE_RELATIVE_STRENGTH_TOO_LOW:'递补股票盘中没有明显跑赢市场，暂不套利',
    WATCH_STOCK_INTRADAY_OUTPERFORMANCE:'观察股盘中超预期，转为模拟试仓',
    LOWER_RANKED_INTRADAY_CANDIDATE:'同题材已有更优盘中候选',
    DUPLICATE_ENTRY_SAME_DAY_BLOCKED:'当天已有持仓、买单或激活记录，禁止重复买入',
    OUTSIDE_DAILY_ENTRY_WINDOW:'普通观察池已过 14:45 新开仓时间，只继续观察',
    REVERSAL_MARKET_NOT_SAFE:'市场环境不支持逆势试仓，只提示异动',
    REVERSAL_MINUTE_HISTORY_INCOMPLETE:'当日 QMT 分钟数据不完整，不能确认深水反转',
    REVERSAL_MINUTE_HISTORY_STALE:'分钟 K 更新过慢，暂不试仓',
    REVERSAL_WATERLINE_PATTERN_MISSING:'没有形成可确认的水下修复形态',
    REVERSAL_REBOUND_NOT_CONFIRMED:'从日内低点拉起的幅度还不够',
    REVERSAL_WATERLINE_NOT_RECLAIMED:'尚未有效翻红，只报警观察',
    REVERSAL_TOO_EXTENDED_TO_CHASE:'已经拉得过高，只提示、不追涨',
    REVERSAL_MOMENTUM_NOT_CONFIRMED:'最近 10 分钟上攻速度不足',
    REVERSAL_PULSE_TOO_FAST:'最近 5 分钟拉升过急，等待回踩',
    REVERSAL_VOLUME_NOT_CONFIRMED:'反转时段成交额放大不足',
    REVERSAL_THEME_RELATIVE_STRENGTH_LOW:'板块没有明显跑赢市场，可能只是单票脉冲',
    REVERSAL_RISK_REWARD_TOO_LOW:'翻红后剩余盈亏比不足，不追',
    MARKET_WIDE_DEEP_REVERSAL_CONFIRMED:'深水反转、放量和板块共振确认，模拟小仓试错',
    MARKET_WIDE_WATERLINE_RECOVERY_CONFIRMED:'水下修复、放量和板块共振确认，模拟更小仓试错',
    MARKET_WIDE_ROCKET_ALERT:'短时快速拉升，已触发全市场异动报警',
    MARKET_WIDE_LIMIT_ATTACK_ALERT:'快速冲击涨停，本票不追，立即检查同板块龙二和中军',
    MARKET_WIDE_VOLUME_BURST_CONFIRMED:'价格向上且单位时间成交额爆发，模拟小仓验证',
    VOLUME_BURST_MARKET_NOT_SAFE:'市场环境不足以支持爆量跟随，只报警',
    VOLUME_BURST_CONFIRMATION_POINTS_MISSING:'连续实时快照不足，暂不判断持续买盘',
    VOLUME_BURST_RATIO_TOO_LOW:'单位时间成交额放大倍数不足',
    VOLUME_BURST_AMOUNT_TOO_SMALL:'爆量区间实际成交额太小',
    VOLUME_BURST_PRICE_NOT_UP:'成交额放大但价格没有向上，不能视作主动买盘',
    VOLUME_BURST_PRICE_TOO_FAST:'价格瞬间拉升过快，只报警、不追脉冲',
    VOLUME_BURST_STILL_WEAK:'爆量后个股仍偏弱，观察承接',
    VOLUME_BURST_TOO_EXTENDED:'爆量后涨幅过高，剩余空间不足',
    LOWER_RANKED_REVERSAL_CANDIDATE:'本轮已有更优量价异动候选'
  };
  function el(id) { return document.getElementById(id); }
  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
  function n(v) { var x = Number(v); return Number.isFinite(x) ? x : null; }
  function money(v) { var x = n(v); return x == null ? '¥—' : '¥' + x.toLocaleString('zh-CN', {minimumFractionDigits:2,maximumFractionDigits:2}); }
  function api(path) { return fetch('/api/v2' + path, {headers:{'Accept':'application/json'}}).then(function (r) { if (!r.ok) throw new Error(r.status + ' ' + path); return r.json(); }); }
  function safeApi(path) {
    return api(path).catch(function (err) {
      return {status:'degraded', data:{error:err.message}};
    });
  }
  function unwrap(payload) { return payload && payload.data; }
  function rowEmpty(cols, text) { return '<tr><td colspan="' + cols + '" class="empty">' + esc(text) + '</td></tr>'; }
  function percent(v, digits) {
    var x = n(v);
    return x == null ? '—' : (x * 100).toFixed(digits == null ? 1 : digits) + '%';
  }
  function yesNo(v) { return Number(v) === 1 || v === true ? '是' : '否'; }
  function scheduleText(task) {
    task = task || {};
    return task.cron_time || (task.interval_minutes ? '每 ' + task.interval_minutes + ' 分钟' : '—');
  }
  function statusClass(status) {
    status = String(status || '').toUpperCase();
    if (['PASS','PASSED','COMPLETED','ACTIVE','IDLE','SUCCESS','OK','COLLECTING','REGISTERED'].indexOf(status) >= 0) return 'safe-text';
    if (['FAILED','ERROR','BLOCK','BLOCKED','STALE','DEGRADED'].indexOf(status) >= 0) return 'danger-text';
    return '';
  }
  function statusText(v) {
    var key = String(v || '').toUpperCase();
    return statusLabels[key] || String(v || '—');
  }
  function actionText(v) {
    var key = String(v || '').toUpperCase();
    return actionLabels[key] || String(v || '—');
  }
  function candidateActionText(row) {
    row = row || {};
    var action = String(row.display_action || row.action || '').toUpperCase();
    if (['BUY','OPEN','ADD','BUY_READY'].indexOf(action) >= 0 && row.new_buy_eligible !== true) {
      return String(row.competition_status || '').toUpperCase() === 'RESEARCH_ONLY' ? '仅研究' : '观察';
    }
    var raw = row.raw_features || {};
    var status = String(raw.signal_status || '').toUpperCase();
    if (status === 'WATCH') return '观察';
    if (status === 'BLOCKED') return '暂不买入';
    return actionText(action);
  }
  function regimeText(v) {
    var key = String(v || '').toUpperCase();
    return regimeLabels[key] || String(v || '—');
  }
  function strategyText(v) {
    var raw = String(v || '');
    var key = raw.indexOf(':') >= 0 ? raw.split(':').pop() : raw;
    if (raw.indexOf('etf_trend_risk') === 0) key = 'etf_trend_risk';
    if (raw.indexOf('sector_preheat') === 0) key = 'sector_preheat';
    return strategyLabels[key] || raw || '—';
  }
  function themeText(row) {
    row = row || {};
    var raw = row.raw_features || {};
    var primaryName = raw.theme_name || row.theme_name || row.theme_code || '未分类';
    var themeNames = [primaryName];
    (raw.theme_matches || []).forEach(function (item) {
      var name = String(item.theme_name || '').trim();
      if (name && themeNames.indexOf(name) < 0) themeNames.push(name);
    });
    var shownNames = themeNames.slice(0, 4).join(' / ');
    if (themeNames.length > 4) shownNames += ' 等' + themeNames.length + '个题材';
    var role = raw.sector_role || row.sector_role || '';
    var stage = raw.sector_stage === 'PREHEAT' ? '预热' :
      (raw.sector_stage === 'CONFIRMED' ? '确认' : '');
    var lane = raw.signal_lane === 'DISCOVERY_SHADOW' ? '预判观察' :
      (raw.signal_lane === 'EXECUTION' ? '正式执行' : '');
    return [lane, shownNames, role, stage].filter(Boolean).join(' · ');
  }
  function contextText(row) {
    row = row || {};
    var raw = row.raw_features || {};
    var summary = raw.context_summary || row.context_summary || [];
    if (!Array.isArray(summary)) summary = [summary];
    var adjustment = n(raw.context_adjustment);
    var prefix = adjustment == null
      ? ''
      : ('上下文' + (adjustment >= 0 ? '+' : '') + adjustment.toFixed(1) + '分');
    var detail = summary.filter(Boolean).slice(0, 5).join('；');
    return [prefix, detail].filter(Boolean).join('：') || '上下文暂无可用记录，不加不减';
  }
  function sourceText(v) {
    var map = {
      gj_big_qmt_inner:'国金 Big QMT',
      GJ_BIG_QMT_INNER:'国金 Big QMT 主源',
      PUBLIC_QUOTE_QUORUM_V1:'公共双源替补（新浪+腾讯）',
      qmt_level1:'国金 QMT 买一卖一',
      qmt_snapshot:'国金 QMT 行情快照',
      historical_daily_bar:'历史日 K 模拟撮合'
    };
    var raw=String(v||'');
    if(raw.indexOf('paper_public_quorum_snapshot:')===0)return '公共多源替补模拟成交（非Level-1）';
    if(raw.indexOf('paper_qmt_snapshot:')===0)return '国金QMT快照模拟成交（非Level-1）';
    return map[raw] || String(v || '来源未登记');
  }
  function quoteAgeSeconds(v){if(!v)return null;var t=new Date(String(v).replace(' ','T')),seconds=(Date.now()-t.getTime())/1000;return Number.isFinite(seconds)?Math.max(0,seconds):null}
  function quoteAgeText(seconds){if(seconds==null)return '无时间';if(seconds<60)return Math.round(seconds)+'秒前';if(seconds<3600)return Math.round(seconds/60)+'分钟前';return (seconds/3600).toFixed(1)+'小时前'}
  function tradingSessionNow(){var d=new Date(),day=d.getDay(),n=d.getHours()*100+d.getMinutes();return day>0&&day<6&&((n>=931&&n<=1130)||(n>=1301&&n<=1500))}
  function signalText(v) {
    var map = {
      carry:'继续持有原目标', monthly_rebalance:'月末再平衡',
      daily_vol_stop:'触发波动止损'
    };
    return map[String(v || '')] || String(v || '—');
  }
  function taskText(type, fallback) {
    var map = {
      etf_forward_daily:'ETF 每日前向记录',
      trading_v2_premarket_decision:'盘前组合决策',
      trading_v2_close_decision:'收盘组合决策',
      trading_v2_paper_tick:'模拟盘撮合',
      trading_v2_reconciliation:'日终对账',
      trading_v2_job_worker:'回测与决策任务执行器',
      trading_v2_level1_validation:'Level-1 连续性验收',
      trading_v2_strategy_health:'策略健康检查',
      trading_v2_intraday_activation:'盘中观察池动态激活',
      public_quote_failover:'QMT故障公共多源替补',
      qmt_membership_snapshot:'概念与行业成员快照'
    };
    return map[String(type || '')] || fallback || type || '未命名任务';
  }
  function workerText(v) {
    var map = {
      'trading-v2-job-worker':'V2 回测与决策执行器'
    };
    return map[String(v || '')] || String(v || '未命名执行器');
  }
  function jobText(v) {
    var map = {
      BACKTEST:'策略回测', DECISION_RUN:'组合决策'
    };
    return map[String(v || '').toUpperCase()] || String(v || '后台任务');
  }
  function scopeText(v) {
    var map = {
      A_SHARE:'A 股', ETF:'ETF', A_SHARE_AND_ETF:'A 股与 ETF',
      STOCK:'A 股'
    };
    return map[String(v || '').toUpperCase()] || String(v || '—');
  }
  function reasonText(row) {
    row = row || {};
    var code = String(row.rejection_code || row.waiting_reason || row.reason_code || '');
    var raw = row.raw_features || {};
    var route = raw.paper_trial_route || {};
    var detail = readableDetail(
      route.route_reason || raw.gate_reason || row.invalidation_condition
    );
    if (detail === 'strategy-specific frozen exit formula') detail = '';
    var ratio = n(row.risk_reward_ratio);
    var ratioReason = ratio != null && ratio < 3
      ? '当前盈亏比 ' + ratio.toFixed(2) + '，低于策略最低要求 3.00'
      : '';
    if (code === 'SIGNAL_NOT_BUY_READY') {
      var parts = [];
      if (detail) parts.push(detail);
      if (ratioReason) parts.push(ratioReason);
      if (!parts.length) parts.push('策略当前只有观察信号，入场条件尚未全部确认');
      return parts.join('；');
    }
    if (detail && (code === '' || code === 'RISK_REJECTED')) return detail;
    if (/^STRATEGY_LIFECYCLE_/.test(code)) {
      return '该策略仍处于“' + statusText(code.replace('STRATEGY_LIFECYCLE_', '')) + '”阶段，暂不能进入模拟买入';
    }
    var known = reasonLabels[code] || labels[code] && labels[code][0];
    if (/(深水反转|水下修复|极速拉升|爆量上攻)/.test(String(row.role || '')) &&
        Array.isArray(row.evidence) && row.evidence.length) {
      return [known || code].concat(row.evidence.slice(0, 5)).filter(Boolean).join('；');
    }
    if (known && detail) return known + '；个股信号层面：' + detail;
    return known || detail || code || '没有进入本次组合';
  }
  function readableDetail(v) {
    var value = String(v || '').trim();
    if (!value || value === 'strategy-specific frozen exit formula') return '';
    if (/base status is SUSPENDED/i.test(value)) return '基础信号已暂停，当前不允许入场';
    if (/trend_not_broken/i.test(value)) return '只有趋势未破坏时才允许继续持有';
    if (/medium_trend_break/i.test(value)) return '中期趋势已经破坏';
    if (/signal.*not.*ready/i.test(value)) return '买入信号尚未达到可执行状态';
    if (/^[\x00-\x7F]+$/.test(value)) return '未满足策略冻结的入场或持有条件';
    return value;
  }
  function securityLink(code, name) {
    var clean = String(code || '').split('.', 1)[0];
    var display = String(name || clean || '—');
    if (!clean) return esc(display);
    var href = '/?tab=stock-list&stock_code=' + encodeURIComponent(clean);
    var title = '打开 ' + display + '（' + clean + '）详情';
    return '<span class="security-link"><a href="' + esc(href) + '" title="' +
      esc(title) + '" class="security-name">' + esc(display) +
      '</a><a href="' + esc(href) + '" title="' + esc(title) +
      '" class="security-code">' + esc(clean) + '</a></span>';
  }
  function targetHtml(target, names) {
    if (!target || typeof target !== 'object') return '—';
    var keys = Object.keys(target);
    return keys.length ? keys.map(function (code) {
      return '<span class="target-security">' +
        securityLink(code, names && names[code]) +
        '<b>' + esc(percent(target[code], 1)) + '</b></span>';
    }).join('') : '保持现金';
  }

  function renderChrome() {
    var ready = unwrap(state.readiness) || {};
    var regime = unwrap(state.regime) || {};
    var account = unwrap(state.account) || {};
    var equity = account.latest_equity || {};
    var positions = unwrap(state.positions) || [];
    var recon = account.latest_reconciliation || {};
    var blocks = ready.blocks || [];
    el('snapshotTag').textContent = state.readiness && state.readiness.data_snapshot_id ? '快照 ' + state.readiness.data_snapshot_id.slice(0, 10) : '无数据快照';
    el('regimeValue').textContent = regimeText(regime.market_regime || 'DATA_BLOCKED');
    el('regimeMeta').textContent = regime.trade_date ? regime.trade_date + ' · ' + statusText(regime.status) : '尚无已完成决策';
    el('equityMetric').textContent = money(equity.total_equity || account.cash_balance);
    el('cashMetric').textContent = money(account.cash_balance);
    el('cashMeta').textContent = account.status || '账户未初始化';
    var positionCount = new Set(positions.map(function (row) { return String(row.stock_code || '').slice(0, 6); }).filter(Boolean)).size;
    el('positionMetric').textContent = positionCount + ' / ' + String(account.maximum_positions || ready.maximum_positions || 12);
    el('reconMetric').textContent = recon.status || '尚无日终对账';
    el('reconMetric').className = recon.status === 'PASS' ? 'safe-text' : (recon.status ? 'danger-text' : '');
    if (ready.ready_for_new_positions) {
      el('heroDecision').textContent = 'ProBigA 模拟盘已启用，可进入仓位竞争';
      el('heroReason').textContent = '优先使用 QMT 买一卖一；缺失时仅用 180 秒内 QMT 快照并计入冻结滑点。';
      el('hero').classList.remove('blocked');
    } else if (ready.paper_infrastructure_ready) {
      el('heroDecision').textContent = '模拟盘正常，当前行情纪律暂不开新仓';
      el('heroReason').textContent = blocks.length ? '账户、账本和策略均已就绪；当前仅被市场状态门禁挡住。' : '等待下一次有效市场决策。';
      el('hero').classList.add('blocked');
    } else {
      el('heroDecision').textContent = '模拟盘基础设施尚未就绪';
      el('heroReason').textContent = blocks.length ? '存在 ' + blocks.length + ' 项硬阻断，系统不会拿高分抵消。' : '尚无完整 V2 快照。';
      el('hero').classList.add('blocked');
    }
  }
  function renderTrust() {
    var ready = unwrap(state.readiness) || {};
    var blocks = ready.blocks || [];
    el('readinessBadge').textContent = ready.paper_infrastructure_ready ? '模拟盘已启用' : '模拟盘未就绪';
    el('readinessBadge').className = 'badge ' + (ready.paper_infrastructure_ready ? 'safe' : 'danger');
    var blockHtml = blocks.length ? blocks.map(function (code) {
      var item = labels[code] || [code, '该能力未达到 V2 的确定性门槛。'];
      return '<div class="block-item" title="' + esc(code) + '"><i></i><div><strong>' + esc(item[0]) + '</strong><small>' + esc(item[1]) + '</small></div></div>';
    }).join('') : '<p class="empty safe-text">模拟盘当前没有硬阻断</p>';
    var realBlocks = ready.real_trading_blocks || [];
    if (realBlocks.length) {
      blockHtml += '<div class="block-item"><i></i><div><strong>真实交易仍保持关闭</strong><small>' +
        esc(realBlocks.map(function (code) {
          return labels[code] ? labels[code][0] : reasonText({rejection_code:code});
        }).join('；')) + '</small></div></div>';
    }
    el('blockList').innerHTML = blockHtml;
    var p = state.readiness || {};
    var regime = unwrap(state.regime) || {};
    var facts = [
      ['数据快照', p.data_snapshot_id || '—'],
      ['数据哈希', p.data_snapshot_hash || '—'],
      ['代码版本', p.code_commit_sha || regime.code_commit_sha || '—'],
      ['配置版本', p.config_version || regime.config_version || '—'],
      ['决策批次', regime.run_uid || '—'],
      ['结果哈希', regime.result_hash || '—']
    ];
    el('provenanceList').innerHTML = facts.map(function (x) { return '<dt>' + esc(x[0]) + '</dt><dd>' + esc(x[1]) + '</dd>'; }).join('');
    var schema = ready.schema || {};
    var schemaNames = Object.keys(schema);
    var readyCount = schemaNames.filter(function (name) { return schema[name]; }).length;
    el('schemaSummary').innerHTML = '<strong>V2 独立账本 ' + readyCount + ' / ' +
      schemaNames.length + ' 正常</strong><span>新交易流程只写 V2 独立表；真实交易写入保持关闭。</span>';
    el('schemaGrid').innerHTML = Object.keys(schema).map(function (name) { return '<div class="schema-item ' + (schema[name] ? 'ok' : 'fail') + '">' + esc(name) + '</div>'; }).join('');
  }
  function renderTomorrow() {
    var data = unwrap(state.tomorrow) || {};
    var positions = data.positions || [];
    var watch = data.watch_candidates || [];
    var noBuy = data.action !== 'BUY' || !positions.length;
    el('tomorrowDate').textContent = data.execution_trade_date || '下一交易日未登记';
    el('tomorrowDecision').textContent = noBuy ? '不开新仓，保持现金' : '计划买入 ' + positions.length + ' 只证券';
    el('tomorrowReason').textContent = noBuy
      ? '当前市场状态为“' + regimeText(data.market_regime || 'DATA_BLOCKED') + '”；高分观察股也不能绕过组合门禁。'
      : '开盘前仍会执行停牌、涨跌停、价格新鲜度和账户风险检查，不满足时不会强买。';
    el('tomorrowBanner').className = 'decision-banner ' + (noBuy ? 'blocked' : 'ready');
    el('tomorrowRows').innerHTML = positions.length ? positions.map(function (r) {
      return '<tr><td>' + securityLink(r.stock_code || r.etf_code, r.short_name) + '</td><td>' +
        esc(r.target_quantity == null ? '—' : r.target_quantity + ' 股') + '</td><td>' +
        esc(percent(r.target_weight, 1)) + '</td><td title="' + esc(r.strategy_version || '') + '">' + esc(strategyText(r.strategy_version)) +
        '</td><td>' + esc(themeText(r)) + '</td><td>' +
        esc(r.execution_note || '按模拟盘开盘检查执行') + '</td></tr>';
    }).join('') : rowEmpty(6, '没有计划买入；现金也是明确的仓位决定');
    var facts = [
      ['信号数据日', data.source_trade_date || '—'],
      ['计划执行日', data.execution_trade_date || '—'],
      ['市场状态', regimeText(data.market_regime)],
      ['决策批次', data.run_uid || '—'],
      ['决策状态', statusText(data.run_status)],
      ['目标现金', money(data.target_cash)],
      ['风险仓位', percent(data.target_risk_asset_weight, 1)],
      ['最坏损失', money(data.worst_case_loss)],
      ['待撮合模拟单', (data.pending_order_count || 0) + ' 笔'],
      ['被拒候选', (data.rejected_candidate_count || 0) + ' 条']
    ];
    el('tomorrowFacts').innerHTML = facts.map(function (x) {
      return '<dt>' + esc(x[0]) + '</dt><dd>' + esc(x[1]) + '</dd>';
    }).join('');
    el('tomorrowWatchRows').innerHTML = watch.length ? watch.map(function (r) {
      return '<tr><td>' + securityLink(r.stock_code, r.short_name) + '</td><td title="' +
        esc(r.strategy_version || '') + '">' + esc(strategyText(r.strategy_version)) + '</td><td>' +
        esc(themeText(r)) + '</td><td>' +
        esc(r.raw_score == null ? '—' : n(r.raw_score).toFixed(2)) + '</td><td>' +
        esc(r.risk_reward_ratio == null ? '—' : n(r.risk_reward_ratio).toFixed(2)) + '</td><td>' +
        esc(candidateActionText(r)) + '</td><td class="reason-cell" title="' +
        esc(r.rejection_code || '') + '">' + esc(reasonText(r)) + '</td></tr>';
    }).join('') : rowEmpty(7, '没有可展示的观察候选');
  }
  function renderCandidates() {
    var allRows = unwrap(state.candidates) || [];
    var query = String(state.candidateFilter || '').trim().toLowerCase();
    var rows = query ? allRows.filter(function (r) {
      return [
        r.stock_code, r.short_name, themeText(r), contextText(r),
        strategyText(r.strategy_version), reasonText(r)
      ].join(' ').toLowerCase().indexOf(query) >= 0;
    }) : allRows;
    el('candidateCount').textContent = query
      ? rows.length + ' / ' + allRows.length + ' 条'
      : allRows.length + ' 条';
    if (state.candidateLoading) {
      el('candidateRows').innerHTML = rowEmpty(10, '正在读取所选历史票池…');
      return;
    }
    el('candidateRows').innerHTML = rows.length ? rows.map(function (r) {
      return '<tr><td>' + securityLink(r.stock_code, r.short_name) + '</td><td title="' +
        esc(r.strategy_version || '') + '">' + esc(strategyText(r.strategy_version)) +
        '</td><td>' + esc(themeText(r)) + '</td><td>' + esc(candidateActionText(r)) + '</td><td>' +
        esc(r.raw_score == null ? '—' : n(r.raw_score).toFixed(2)) + '</td><td>' +
        esc(r.expected_return_lower_bound == null ? '尚无可信下界' : percent(r.expected_return_lower_bound, 2)) +
        '</td><td>' + esc(r.risk_reward_ratio == null ? '—' : n(r.risk_reward_ratio).toFixed(2)) +
        '</td><td title="' + esc(r.competition_status || '') + '">' +
        esc(statusText(r.competition_status)) + '</td><td class="reason-cell" title="' +
        esc(contextText(r)) + '">' + esc(contextText(r)) + '</td><td class="reason-cell" title="' +
        esc(r.rejection_code || '') + '">' + esc(reasonText(r)) + '</td></tr>';
    }).join('') : rowEmpty(10, '没有已完成决策的候选快照');
  }
  function candidateDecisionRuns() {
    var rows = unwrap(state.decisionRuns);
    return Array.isArray(rows) ? rows : [];
  }
  function candidateRunTime(run) {
    var raw = String((run || {}).decision_at || '');
    var match = raw.match(/[T ](\d{2}:\d{2}:\d{2})/);
    return match ? match[1] : (raw || '时间未登记');
  }
  function candidateRunsForDate(tradeDate) {
    return candidateDecisionRuns().filter(function (run) {
      return String(run.trade_date || '') === String(tradeDate || '');
    });
  }
  function renderCandidateHistoryControls() {
    var runs = candidateDecisionRuns();
    var dates = [];
    runs.forEach(function (run) {
      var tradeDate = String(run.trade_date || '');
      if (tradeDate && dates.indexOf(tradeDate) < 0) dates.push(tradeDate);
    });
    if (!state.selectedCandidateDate || dates.indexOf(state.selectedCandidateDate) < 0) {
      state.selectedCandidateDate = dates[0] || '';
    }
    el('candidateDate').innerHTML = dates.length
      ? dates.map(function (tradeDate) {
        return '<option value="' + esc(tradeDate) + '">' + esc(tradeDate) + '</option>';
      }).join('')
      : '<option value="">暂无历史日期</option>';
    el('candidateDate').value = state.selectedCandidateDate;
    el('candidateDate').disabled = !dates.length;

    var dateRuns = candidateRunsForDate(state.selectedCandidateDate);
    if (!state.selectedCandidateRunUid || !dateRuns.some(function (run) {
      return String(run.run_uid || '') === String(state.selectedCandidateRunUid || '');
    })) {
      state.selectedCandidateRunUid = String((dateRuns[0] || {}).run_uid || '');
    }
    el('candidateRun').innerHTML = dateRuns.length
      ? dateRuns.map(function (run) {
        var label = candidateRunTime(run) + ' · ' + statusText(run.status) +
          ' · ' + Number(run.signal_count || 0) + ' 条';
        return '<option value="' + esc(run.run_uid || '') + '">' + esc(label) + '</option>';
      }).join('')
      : '<option value="">当日没有已落库批次</option>';
    el('candidateRun').value = state.selectedCandidateRunUid;
    el('candidateRun').disabled = !dateRuns.length;

    var selected = dateRuns.find(function (run) {
      return String(run.run_uid || '') === String(state.selectedCandidateRunUid || '');
    }) || null;
    var latestUid = String((runs[0] || {}).run_uid || '');
    var isLatest = selected && String(selected.run_uid || '') === latestUid;
    el('candidatePoolMeta').textContent = selected
      ? (isLatest ? '最新票池' : '历史票池') + ' · ' +
        String(selected.trade_date || '') + ' ' + candidateRunTime(selected) +
        ' · 批次 ' + String(selected.run_uid || '').slice(0, 12)
      : '暂无已落库票池';
    el('candidatePoolMeta').className = 'badge ' + (isLatest ? 'safe' : 'warning');
  }
  function loadCandidatePool(runUid) {
    runUid = String(runUid || '');
    if (!runUid) {
      state.candidates = {status:'empty', data:[]};
      state.candidateLoading = false;
      renderCandidates();
      renderCandidateHistoryControls();
      return;
    }
    state.candidateLoading = true;
    state.candidateRequestId = Number(state.candidateRequestId || 0) + 1;
    var requestId = state.candidateRequestId;
    renderCandidates();
    api('/candidates?run_uid=' + encodeURIComponent(runUid) + '&limit=500')
      .then(function (payload) {
        if (requestId !== state.candidateRequestId) return;
        state.candidates = payload;
      })
      .catch(function (err) {
        if (requestId !== state.candidateRequestId) return;
        state.candidates = {status:'degraded', data:[]};
        el('candidatePoolMeta').textContent = '票池读取失败 · ' + err.message;
        el('candidatePoolMeta').className = 'badge danger';
      })
      .finally(function () {
        if (requestId !== state.candidateRequestId) return;
        state.candidateLoading = false;
        renderCandidates();
      });
  }
  function renderIntraday() {
    var data = unwrap(state.intraday) || {};
    var realtime = data.current_realtime_state || {};
    var history = data.latest_historical_snapshot || {};
    var market = realtime.snapshot || {};
    var isLive = realtime.status === 'LIVE';
    var liveStale = realtime.status === 'STALE';
    var rows = isLive ? (data.decisions || []) : [];
    var status = data.status || state.intraday && state.intraday.status || 'waiting_first_tick';
    var age=realtime.snapshot_age_seconds,isFallback=String(market.source_provider||'').toUpperCase()==='PUBLIC_QUOTE_QUORUM_V1';
    var actionable = isLive && (Number(market.actionable) === 1 || market.actionable === true);
    el('intradayBadge').textContent = liveStale
      ? '结果已过期 · 禁止下单'
      : actionable
      ? regimeText(market.state) + ' · 可按条件试仓'
      : (isLive ? regimeText(market.state || 'DATA_BLOCKED') : '当前无实时状态');
    el('intradayBadge').className = 'badge ' + (actionable ? 'safe' : (liveStale || market.quality_status === 'BLOCK' ? 'danger' : 'warning'));
    el('intradayRealtimeBadge').textContent = isLive ? '实时有效' : liveStale ? '结果已过期' : '当前不可执行';
    el('intradayRealtimeBadge').className = 'badge ' + (isLive ? 'safe' : liveStale ? 'danger' : 'warning');
    var evidence = market.evidence || [];
    var facts = [
      ['实时状态', isLive ? (isFallback ? '公共替补有效，模拟盘降仓' : 'QMT主源实时') : liveStale ? '结果已过期，禁止下单' : '当前没有实时状态'],
      ['判断时间', isLive ? (market.observed_at || '—') : '—'],
      ['数据年龄', age == null ? '—' : quoteAgeText(age)],
      ['盘中状态', isLive ? regimeText(market.state || 'DATA_BLOCKED') : '—'],
      ['行情来源', isLive ? sourceText(market.source_provider) : '—'],
      ['有效股票', isLive && market.observed_count != null ? market.observed_count + ' / ' + (market.expected_count || '—') : '—'],
      ['有效覆盖率', isLive && market.coverage != null ? percent(market.coverage, 1) : '—'],
      ['当前结论', realtime.reason || (evidence.length ? evidence.join('；') : '等待实时数据')]
    ];
    el('intradayMarketFacts').innerHTML = facts.map(function (x) {
      return '<dt>' + esc(x[0]) + '</dt><dd>' + esc(x[1]) + '</dd>';
    }).join('');
    var historicalFacts = history && history.observed_at ? [
      ['历史快照时间', history.observed_at],
      ['身份', '历史/收盘快照，仅供复盘'],
      ['市场状态', regimeText(history.state || 'DATA_BLOCKED')],
      ['行情来源', sourceText(history.source_provider)],
      ['有效覆盖率', history.coverage == null ? '—' : percent(history.coverage, 1)],
      ['执行权限', '禁止下单']
    ] : [['历史快照', '尚无已落库历史快照']];
    el('intradayHistoricalFacts').innerHTML = historicalFacts.map(function (x) {
      return '<dt>' + esc(x[0]) + '</dt><dd>' + esc(x[1]) + '</dd>';
    }).join('');
    el('intradayRows').innerHTML = rows.length ? rows.map(function (r) {
      return '<tr><td>' + securityLink(r.stock_code, r.short_name) + '</td><td>' +
        esc((r.theme_name || r.theme_code || '未分类') + (r.role ? ' · ' + r.role : '')) +
        '</td><td title="' + esc(r.status || '') + '">' + esc(actionText(r.action)) +
        ' · ' + esc(statusText(r.status)) + '</td><td>' +
        esc(n(r.current_return_pct).toFixed(2) + '%') + '</td><td>' +
        esc(n(r.relative_strength_pct).toFixed(2) + '%') + '</td><td>' +
        esc(n(r.intraday_amount_ratio).toFixed(2) + ' 倍') + '</td><td>' +
        esc(n(r.theme_positive_breadth_pct).toFixed(2) + '%') + '</td><td>' +
        esc(n(r.risk_reward_ratio).toFixed(2)) + '</td><td class="reason-cell" title="' +
        esc(r.reason_code || '') + '">' + esc(reasonText(r)) + '</td></tr>';
    }).join('') : rowEmpty(9, status === 'waiting_first_tick' ? '交易时段开始后产生首个真实盘中判断' : '本次盘中判断没有有效观察候选');
  }
  function renderPlan() {
    var plan = unwrap(state.plan) || {};
    var positions = plan.positions || [];
    var slotCount = Math.max(1, Math.min(12, Number(plan.maximum_positions || 12)));
    el('slotGrid').innerHTML = Array.from({length:slotCount}, function (_, i) {
      var p = positions[i];
      return '<div class="slot"><b>仓位 0' + (i + 1) + '</b><span>' +
        (p ? securityLink(p.stock_code, p.short_name) +
          '<em>' + esc((p.target_quantity || 0) + ' 股 · ' + strategyText(p.strategy_version)) + '</em>'
          : '保持现金，未分配') + '</span></div>';
    }).join('');
    var facts = [
      ['市场状态', regimeText(plan.market_regime)],
      ['目标现金', money(plan.target_cash)],
      ['风险资产仓位', plan.target_risk_asset_weight == null ? '—' : (n(plan.target_risk_asset_weight) * 100).toFixed(1) + '%'],
      ['最坏损失', money(plan.worst_case_loss)],
      ['被拒候选', (plan.rejected_candidates || []).length + ' 条'],
      ['计划版本', plan.plan_version || '—']
    ];
    el('planFacts').innerHTML = facts.map(function (x) { return '<dt>' + esc(x[0]) + '</dt><dd>' + esc(x[1]) + '</dd>'; }).join('');
  }
  function renderPositions() {
    var rows = unwrap(state.positions) || [];
    el('positionRows').innerHTML = rows.length ? rows.map(function (r) {
      return '<tr><td>' + securityLink(r.stock_code, r.short_name) + '</td><td>' +
        esc(r.lot_id) + '</td><td title="' + esc(r.position_state || '') + '">' +
        esc(statusText(r.position_state)) + '</td><td>' + esc(r.remaining_quantity) +
        '</td><td>' + esc(r.settlement_date) + '</td><td>' + esc(r.cost_price) +
        '</td><td>' + esc(r.protective_stop) + '</td><td>' + esc(r.add_count) +
        ' / 0</td><td class="reason-cell">' + esc(reasonText(r)) + '</td></tr>';
    }).join('') : rowEmpty(9, '当前没有 V2 持仓批次');
  }
  function cards(rows, render, empty) { return rows.length ? rows.map(render).join('') : '<p class="empty">' + esc(empty) + '</p>'; }
  function renderOrders() {
    var orders = unwrap(state.orders) || [], fills = unwrap(state.fills) || [];
    el('orderList').innerHTML = cards(orders, function (r) {
      return '<div class="stack-card"><strong>' + securityLink(r.stock_code, r.short_name) +
        '<span>' + esc(actionText(r.side) + ' · ' + statusText(r.status)) +
        '</span></strong><span>' + esc((r.filled_quantity || 0) + '/' +
        (r.quantity || 0) + ' 股 · ' +
        (r.waiting_reason ? reasonText(r) : '当前无需等待')) + '</span></div>';
    }, '没有 V2 订单');
    el('fillList').innerHTML = cards(fills, function (r) {
      return '<div class="stack-card"><strong>' + securityLink(r.stock_code, r.short_name) +
        '<span>' + esc(actionText(r.side) + ' · ' + r.quantity + ' 股') +
        '</span></strong><span>' + esc(r.price + ' · 费用 ' + r.fee_amount +
        ' · ' + sourceText(r.execution_price_source) + ' · ' + r.filled_at) +
        '</span></div>';
    }, '没有模拟成交；将在交易日按真实前向行情产生');
  }
  function renderEtfForward() {
    var data = unwrap(state.etf) || {};
    var strategies = data.strategies || [];
    var observations = data.observations || [];
    var marketData = data.data || {};
    var task = data.task || {};
    var status = data.status || state.etf && state.etf.status || 'degraded';
    var collecting = status === 'collecting';
    el('etfStatusBadge').textContent = collecting ? '正在积累真实记录' :
      (status === 'waiting_first_forward_close' ? '等待首个前向收盘' : statusText(status));
    el('etfStatusBadge').className = 'badge ' + (collecting ? 'safe' : (status === 'degraded' ? 'danger' : 'warning'));
    var strategy = strategies[0] || {};
    var facts = [
      ['冻结版本', strategy.strategy_version || '尚未登记'],
      ['前向起点', strategy.forward_start_date || '—'],
      ['观察记录', (data.observation_count || 0) + ' 条'],
      ['禁止回填', data.backfill === 'prohibited' ? '是' : '未确认'],
      ['自动下单', data.automatic_order_submission === false ? '关闭' : '未确认'],
      ['行情来源', '国金 Big QMT']
    ];
    el('etfFacts').innerHTML = facts.map(function (x) {
      return '<dt>' + esc(x[0]) + '</dt><dd>' + esc(x[1]) + '</dd>';
    }).join('');
    var taskFacts = [
      ['ETF 日 K 行数', marketData.row_count == null ? '—' : marketData.row_count],
      ['ETF 数量', marketData.symbol_count == null ? '—' : marketData.symbol_count],
      ['最新数据日', marketData.latest_trade_date || '—'],
      ['通过校验行', marketData.validated_rows == null ? '—' : marketData.validated_rows],
      ['每日任务', task.task_type || '未注册'],
      ['运行计划', scheduleText(task)],
      ['任务启用', yesNo(task.enabled)],
      ['最近状态', task.last_run_status ? statusText(task.last_run_status) : '尚未运行']
    ];
    el('etfTaskFacts').innerHTML = taskFacts.map(function (x) {
      return '<dt>' + esc(x[0]) + '</dt><dd>' + esc(x[1]) + '</dd>';
    }).join('');
    var latestTarget = observations.length ? observations[0].target : null;
    var securityNames = data.security_names || {};
    el('etfTargetRows').innerHTML = strategies.length ? strategies.map(function (r, index) {
      var target = index === 0 ? latestTarget : null;
      return '<tr><td><strong title="' + esc(r.strategy_version || '') + '">' +
        esc(strategyText(r.strategy_version)) + '</strong></td><td>' +
        esc(r.forward_start_date || '—') + '</td><td title="' + esc(r.status || '') + '">' +
        esc(statusText(r.status)) + '</td><td>' + targetHtml(target, securityNames) +
        (observations.length ? '' : ' · 暂无真实观察，不生成目标') + '</td></tr>';
    }).join('') : rowEmpty(4, 'ETF 前向策略尚未登记');
    el('etfObservationRows').innerHTML = observations.length ? observations.map(function (r) {
      return '<tr><td>' + esc(r.data_date || '—') + '</td><td>' +
        esc(signalText(r.signal_type)) + '</td><td>' + esc(r.execution_date || '不调仓') +
        '</td><td>' + targetHtml(r.target, securityNames) + '</td><td>' +
        esc(sourceText(r.data_source)) + '</td></tr>';
    }).join('') : rowEmpty(5, '0 条是正确状态：首个真实数据日到达后才追加，绝不倒填');
  }
  function renderEvidence() {
    var data = unwrap(state.evidence) || {};
    var qmt = data.qmt_kline_attestation || {};
    var latest = (qmt.runs || [])[0] || {};
    var complete = qmt.status === 'complete' && Number(latest.missing_qmt_rows || 0) === 0 &&
      Number(latest.mismatched_rows || 0) === 0;
    el('qmtEvidenceBadge').textContent = complete ? '逐行补证通过' : (qmt.status || '无记录');
    el('qmtEvidenceBadge').className = 'badge ' + (complete ? 'safe' : 'danger');
    var qmtFacts = [
      ['来源要求', qmt.provider_required || 'gj_big_qmt_inner'],
      ['补证状态', statusText(latest.status || qmt.status)],
      ['日期范围', latest.start_date && latest.end_date ? latest.start_date + ' 至 ' + latest.end_date : '—'],
      ['目标行数', latest.target_rows == null ? '—' : latest.target_rows],
      ['匹配行数', latest.matched_rows == null ? '—' : latest.matched_rows],
      ['QMT 缺失', latest.missing_qmt_rows == null ? '—' : latest.missing_qmt_rows],
      ['数值不一致', latest.mismatched_rows == null ? '—' : latest.mismatched_rows],
      ['覆盖率', latest.coverage_pct == null ? '—' : latest.coverage_pct + '%']
    ];
    el('qmtEvidenceFacts').innerHTML = qmtFacts.map(function (x) {
      return '<dt>' + esc(x[0]) + '</dt><dd>' + esc(x[1]) + '</dd>';
    }).join('');
    var level1 = data.level1 || {};
    var level1Facts = [
      ['验收项目', '买一卖一行情连续性'],
      ['当前状态', statusText(level1.status || 'BLOCK')],
      ['验收规则', '连续五个交易日无关键缺口'],
      ['开始日期', level1.started_at || level1.validation_start_date || '2026-07-27'],
      ['结论日期', level1.finished_at || level1.expected_finish_date || '连续五个交易日后'],
      ['实际影响', '仅阻塞未来实盘；模拟盘可按冻结滑点降级']
    ];
    el('level1Facts').innerHTML = level1Facts.map(function (x) {
      return '<dt>' + esc(x[0]) + '</dt><dd class="' + statusClass(x[1]) + '">' + esc(x[1]) + '</dd>';
    }).join('');
    var membership = data.membership || {};
    el('membershipGrid').innerHTML = ['concept','industry'].map(function (kind) {
      var item = membership[kind] || {};
      var run = (item.runs || [])[0] || {};
      var title = kind === 'concept' ? '概念成员' : '行业成员';
      var groups = kind === 'concept' ? run.concept_count : run.industry_count;
      var relations = kind === 'concept' ? run.concept_relation_count : run.industry_relation_count;
      return '<article class="evidence-card"><span>' + esc(title) + '</span><strong>' +
        esc(item.snapshot_date || run.snapshot_date || '尚无快照') + '</strong><p>状态 · ' +
        esc(statusText(item.status || run.quality_status)) + '</p><p>分组 · ' +
        esc(groups == null ? '—' : groups) + '　成员关系 · ' +
        esc(relations == null ? '—' : relations) + '</p><small>历史批次 ' +
        esc((item.runs || []).length) + ' 个；每天只追加真实快照</small></article>';
    }).join('');
  }
  function renderOperations() {
    var data = unwrap(state.operations) || {};
    var guards = data.real_trading_guards || [];
    var guarded = guards.length >= 2;
    el('guardBadge').textContent = guarded ? '数据库双触发器生效' : '硬约束不完整';
    el('guardBadge').className = 'badge ' + (guarded ? 'safe' : 'danger');
    var guardFacts = [
      ['INSERT 防护', guards.some(function (r) { return r.event_manipulation === 'INSERT' || /_bi$/.test(r.trigger_name || ''); }) ? '已安装' : '缺失'],
      ['UPDATE 防护', guards.some(function (r) { return r.event_manipulation === 'UPDATE' || /_bu$/.test(r.trigger_name || ''); }) ? '已安装' : '缺失'],
      ['真实交易开关', '必须为 0'],
      ['浏览器权限', '本页只读，不创建订单']
    ];
    el('guardFacts').innerHTML = guardFacts.map(function (x) {
      return '<dt>' + esc(x[0]) + '</dt><dd>' + esc(x[1]) + '</dd>';
    }).join('');
    var workers = data.workers || [];
    el('workerList').innerHTML = cards(workers, function (r) {
      return '<div class="stack-card"><strong class="' + statusClass(r.status) + '">' +
        esc(workerText(r.worker_name) + ' · ' + statusText(r.status)) +
        '</strong><span>' + esc('心跳 ' + (r.last_heartbeat_at || r.heartbeat_at || '—') +
        ' · ' + (r.current_job_id || '当前无任务')) + '</span></div>';
    }, '没有执行器心跳');
    var tasks = data.tasks || [];
    el('taskRows').innerHTML = tasks.length ? tasks.map(function (r) {
      return '<tr><td><strong title="' + esc(r.task_type || '') + '">' +
        esc(taskText(r.task_type, r.task_name)) + '</strong></td><td>' + esc(scheduleText(r)) +
        '</td><td>' + esc(yesNo(r.enabled)) + '</td><td class="' +
        statusClass(r.last_run_status) + '">' + esc(r.last_run_status ? statusText(r.last_run_status) : '尚未运行') +
        '</td><td>' + esc(r.last_run_at || r.last_triggered_at || '—') +
        '</td><td title="' + esc(r.last_run_output || '') + '">' +
        esc(r.last_run_output ? '运行输出已记录' : '—') + '</td></tr>';
    }).join('') : rowEmpty(6, '关键调度任务未注册');
    var running = data.running_backtest_count;
    el('runningBacktestBadge').textContent = running == null ? '读取失败' : '执行中 ' + running + ' 条';
    el('runningBacktestBadge').className = 'badge ' + (running === 0 ? 'safe' : 'warning');
    el('backtestList').innerHTML = cards(data.backtests || [], function (r) {
      return '<div class="stack-card"><strong class="' + statusClass(r.status) + '">' +
        esc(strategyText(r.strategy_version) + ' · ' + statusText(r.status)) +
        '</strong><span>' + esc((r.start_date || '—') + ' 至 ' + (r.end_date || '—') +
        ' · 验收 ' + statusText(r.gate_status) + (r.error_code ? ' · ' + reasonText({rejection_code:r.error_code}) : '')) +
        '</span><small title="' + esc(r.error_message || r.backtest_uid || '') + '">' +
        esc(r.error_code ? reasonText({rejection_code:r.error_code}) : '回测编号已记录') +
        '</small></div>';
    }, '没有回测记录');
    el('jobList').innerHTML = cards(data.jobs || [], function (r) {
      return '<div class="stack-card"><strong class="' + statusClass(r.status) + '">' +
        esc(jobText(r.job_type) + ' · ' + statusText(r.status)) +
        '</strong><span>' + esc((r.requested_at || '—') +
        (r.error_code ? ' · ' + reasonText({rejection_code:r.error_code}) : ' · ' + (r.result_ref || '无结果引用'))) +
        '</span><small title="' + esc(r.error_message || r.job_id || '') + '">' +
        esc(r.error_code ? reasonText({rejection_code:r.error_code}) : '任务编号已记录') +
        '</small></div>';
    }, '没有后台任务记录');
  }
  function renderReview() {
    var recon = unwrap(state.reconciliation) || [], daily = unwrap(state.daily) || [];
    el('reconList').innerHTML = cards(recon, function (r) { return '<div class="stack-card"><strong class="' + (r.status === 'PASS' ? 'safe-text' : 'danger-text') + '">' + esc(r.trade_date + ' · ' + statusText(r.status)) + '</strong><span>' + esc('现金差 ' + r.cash_difference + ' · 权益差 ' + r.equity_difference + ' · 股数差 ' + r.position_difference) + '</span></div>'; }, '尚无日终对账记录');
    el('dailyList').innerHTML = cards(daily, function (r) { return '<div class="stack-card"><strong>' + esc(r.trade_date + ' · 权益 ' + r.total_equity) + '</strong><span>' + esc('现金 ' + r.cash_balance + ' · 市值 ' + r.market_value + ' · 回撤 ' + r.drawdown) + '</span></div>'; }, '尚无 V2 日终权益');
  }
  function renderStrategies() {
    var rows = unwrap(state.strategies) || [];
    el('strategyGrid').innerHTML = rows.map(function (r) {
      var validation = r.validation || {};
      var enabled = r.lifecycle_status === 'PAPER_ACTIVE' || r.lifecycle_status === 'PAPER_TRIAL';
      var note = r.lifecycle_status === 'PAPER_TRIAL' ? '真实前向模拟中，尚未证明能够盈利' : (validation.reason || statusText(validation.status) || '冻结清单已登记');
      return '<article class="strategy-card"><h4>' + esc(strategyText(r.strategy_id)) + '</h4><p>版本 · ' + esc(r.version) + '</p><p>范围 · ' + esc(scopeText(r.instrument_scope)) + '</p><span class="badge ' + (enabled ? 'safe' : 'neutral') + '" title="' + esc(r.lifecycle_status || '') + '">' + esc(statusText(r.lifecycle_status)) + '</span><p>' + esc(note) + '</p><code>' + esc(r.config_hash) + '</code></article>';
    }).join('');
  }
  function render() {
    renderChrome();
    renderTrust();
    renderTomorrow();
    renderIntraday();
    renderCandidates();
    renderPlan();
    renderPositions();
    renderOrders();
    renderEtfForward();
    renderEvidence();
    renderOperations();
    renderReview();
    renderStrategies();
    renderCandidateHistoryControls();
    notifyParentResize();
  }
  function notifyParentResize() {
    if (window.parent === window) return;
    window.setTimeout(function () {
      var height = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);
      window.parent.postMessage({ type:'probiga-trading-v2-resize', height:height }, '*');
    }, 0);
  }
  function activateView(view) {
    var selected = null;
    document.querySelectorAll('.nav-item').forEach(function (button) {
      if (button.dataset.view === view) selected = button;
    });
    if (!selected) return false;
    document.querySelectorAll('.nav-item').forEach(function (button) { button.classList.remove('active'); });
    document.querySelectorAll('.view').forEach(function (section) { section.classList.remove('active'); });
    selected.classList.add('active');
    el('view-' + view).classList.add('active');
    el('pageTitle').textContent = selected.textContent.replace(/^\d+\s*/, '');
    notifyParentResize();
    return true;
  }
  function load() {
    el('refreshButton').disabled = true;
    Promise.all([
      api('/system/readiness'), api('/market-regime/latest'), api('/strategies'),
      api('/decision-runs?limit=500'), api('/candidates?limit=500'), api('/accounts/' + accountId),
      api('/accounts/' + accountId + '/plan'), api('/accounts/' + accountId + '/positions'),
      api('/accounts/' + accountId + '/orders'), api('/accounts/' + accountId + '/fills'),
      api('/accounts/' + accountId + '/reconciliation'), api('/reports/daily'),
      safeApi('/operations/tomorrow?account_id=' + encodeURIComponent(accountId)),
      safeApi('/accounts/' + accountId + '/intraday?limit=200'),
      safeApi('/research/etf-forward?limit=100'),
      safeApi('/system/data-evidence'), safeApi('/system/operations')
    ]).then(function (x) {
      [
        'readiness','regime','strategies','decisionRuns','candidates','account','plan',
        'positions','orders','fills','reconciliation','daily','tomorrow','intraday',
        'etf','evidence','operations'
      ].forEach(function (key, i) { state[key] = x[i]; });
      var runs = candidateDecisionRuns();
      state.selectedCandidateDate = String((runs[0] || {}).trade_date || '');
      state.selectedCandidateRunUid = String((runs[0] || {}).run_uid || '');
      render();
      if (state.selectedCandidateRunUid) loadCandidatePool(state.selectedCandidateRunUid);
    }).catch(function (err) {
      el('heroDecision').textContent = 'V2 只读快照暂时不可用';
      el('heroReason').textContent = err.message;
    }).finally(function () { el('refreshButton').disabled = false; notifyParentResize(); });
  }
  document.querySelectorAll('.nav-item').forEach(function (button) {
    button.addEventListener('click', function () {
      activateView(button.dataset.view);
    });
  });
  window.addEventListener('message', function (event) {
    if (event.source !== window.parent || !event.data || event.data.type !== 'probiga-trading-v2-view') return;
    activateView(String(event.data.view || ''));
  });
  if (window.parent !== window && window.ResizeObserver) {
    new ResizeObserver(notifyParentResize).observe(document.body);
  }
  el('refreshButton').addEventListener('click', load);
  el('candidateFilter').addEventListener('input', function (event) {
    state.candidateFilter = event.target.value;
    renderCandidates();
  });
  el('candidateDate').addEventListener('change', function (event) {
    state.selectedCandidateDate = event.target.value;
    state.selectedCandidateRunUid = String(
      (candidateRunsForDate(state.selectedCandidateDate)[0] || {}).run_uid || ''
    );
    renderCandidateHistoryControls();
    loadCandidatePool(state.selectedCandidateRunUid);
  });
  el('candidateRun').addEventListener('change', function (event) {
    state.selectedCandidateRunUid = event.target.value;
    renderCandidateHistoryControls();
    loadCandidatePool(state.selectedCandidateRunUid);
  });
  load();
}());
