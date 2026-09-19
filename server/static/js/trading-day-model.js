/* Pure presentation model. All timestamps are interpreted in Asia/Shanghai.
 * Source kinds: premarket_forecast = frozen 09:08 research;
 * auction_observation = an independent canonical-pool auction observation.
 * No output grants order authority or reconstructs an earlier personal plan. */
(function (root) {
    'use strict';
    function text(v) { return typeof v === 'string' ? v.trim() : typeof v === 'number' && Number.isFinite(v) ? String(v) : ''; }
    function list(v) { return Array.isArray(v) ? v : []; }
    function object(v) { return v && typeof v === 'object' && !Array.isArray(v) ? v : {}; }
    function number(v) {
        if (v == null || !['string', 'number'].includes(typeof v) || (typeof v === 'string' && !v.trim())) return null;
        var n = Number(v); return Number.isFinite(n) ? n : null;
    }
    function day(v) {
        var s = text(v).slice(0, 10);
        if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return '';
        var d = new Date(s + 'T00:00:00Z');
        return Number.isFinite(d.getTime()) && d.toISOString().slice(0, 10) === s ? s : '';
    }
    function stamp(v) {
        var s = text(v), m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2}):(\d{2})(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$/.exec(s);
        if (!m || !day(m[1]) || +m[2] > 23 || +m[3] > 59 || +m[4] > 59) return '';
        if (!m[6]) return m[1] + ' ' + m[2] + ':' + m[3] + ':' + m[4];
        var time = Date.parse(s.replace(' ', 'T'));
        return Number.isFinite(time) ? new Date(time + 28800000).toISOString().slice(0, 19).replace('T', ' ') : '';
    }
    function code(v) { var s = text(v).split('.')[0]; return /^\d{6}$/.test(s) && s !== '000000' ? s : ''; }
    function strings(v) { return list(v).map(function (r) { return text(r) || text(object(r).text) || text(object(r).reason); }).filter(Boolean); }
    function prose(v) { return Array.isArray(v) ? strings(v).join('；') : text(v); }
    function numeric(v, digits, suffix) { var n = number(v); return n === null ? '—' : n.toFixed(digits || 0) + (suffix || ''); }
    function unwrap(v) { v = object(v); return v.data && !Array.isArray(v.data) && typeof v.data === 'object' ? v.data : v; }
    function source(state, key) {
        var s = object(state[key]), v = unwrap(s.value);
        return s.status === 'ready' && !v.error && v.status !== 'error' ? v : null;
    }
    function dateMatches(v, keys, expected) {
        return keys.every(function (key) { return !v[key] || day(v[key]) === expected; });
    }
    function rowTimeValid(row, cutoff) {
        return ['cutoff_at', 'evidence_as_of', 'observed_at', 'generated_at', 'quote_at'].every(function (key) {
            return !row[key] || (!!stamp(row[key]) && stamp(row[key]) <= cutoff);
        });
    }
    function payloadTimeValid(v, maxStamp) {
        return ['generated_at', 'created_at', 'retained_at'].every(function (key) {
            return !v[key] || (!!stamp(v[key]) && (!maxStamp || stamp(v[key]) <= maxStamp));
        });
    }
    function restricted() {
        return Array.from(arguments).some(function (v) {
            return v.decision_scope === 'RESEARCH_DISPLAY_ONLY' || v.actionable_output_allowed === false ||
                ['membership_evidence_status', 'industry_evidence_status', 'news_evidence_status'].some(function (k) {
                    return !v[k] || !['VERIFIED', 'PASS', 'READY'].includes(String(v[k]).toUpperCase());
                });
        });
    }
    function model(state, requestedDate, requestedPhase, clock) {
        state = object(state); clock = object(clock);
        var date = day(requestedDate), phase = ['pre', 'live', 'post'].includes(requestedPhase) ? requestedPhase : 'pre';
        var now = stamp(clock.server_time), today = day(clock.today), activeDate = day(clock.active_trade_date);
        var clockValid = !!now && !!today && day(now) === today && !!activeDate && typeof clock.is_trade_day === 'boolean';
        var historical = !!date && !!today && date < today, future = !!date && !!today && date > today;
        var canPlan = !!date && clockValid && clock.is_trade_day === true && date === today && activeDate === date;
        var result = {date:date, phase:phase, historical:historical, future:future, canPlan:canPlan,
            headline:'', description:'', facts:[], themes:[], holdings:[], review:{text:'', selection:''}, issues:[], sourceStatus:[]};
        var titles = {forecast:'09:08盘前主题', market:'市场行情', auction:'独立竞价观察', holdings:'当前持仓风险', review:'盘后量化复盘', journal:'我的计划与复盘'};
        function issue(s) { if (!result.issues.includes(s)) result.issues.push(s); }
        function status(key, usable, asOf, note) {
            var input = object(state[key]);
            result.sourceStatus.push({key:key,label:titles[key],status:usable ? '可读' : input.status === 'error' ? '读取失败' : input.status === 'loading' ? '读取中' : '待核验',asOf:asOf || '',note:note || ''});
        }
        if (!date) issue('观察日期无效。');
        if (!clockValid) issue('交易时钟待核验，暂不能新增观察计划。');
        if (future) issue('所选日期尚未到达，不展示未来观察或复盘。');
        if (historical && phase === 'live') issue('历史盘中只展示已有竞价账本观察，不使用当前行情回填。');
        var selectedMax = date ? date + ' 23:59:59' : '';
        var knownMax = now && (!selectedMax || now < selectedMax) ? now : selectedMax;
        var seen = Object.create(null), forecast = source(state, 'forecast'), forecastOk = false;
        if (date && clockValid && !future && forecast) {
            var cutoff = stamp(forecast.cutoff_at), generated = stamp(forecast.generated_at);
            forecastOk = forecast.fallback !== true && day(forecast.session_date) === date &&
                dateMatches(forecast,['requested_date'],date) && forecast.stage === 'PREMARKET_0908' &&
                !!text(forecast.run_uid) && !!cutoff && day(cutoff) === date && cutoff.slice(11) >= '09:08:00' && cutoff.slice(11) <= '09:08:59' &&
                (!forecast.source_trade_date || (!!day(forecast.source_trade_date) && day(forecast.source_trade_date) < date)) &&
                cutoff <= now && payloadTimeValid(forecast,knownMax) &&
                !!generated && day(generated) === date && generated >= cutoff && generated <= date + ' 09:30:00';
            if (!forecastOk) issue('盘前主题的日期、冻结时点或来源批次未通过核验，未作为当天主线展示。');
            if (forecastOk) {
                list(forecast.themes).forEach(function (raw, index) {
                    var t = object(raw), key = text(t.theme_key || t.id), name = text(t.theme_name || t.name);
                    if (!name || !dateMatches(t,['session_date','trade_date'],date) || !rowTimeValid(t,cutoff) || (t.run_uid && t.run_uid !== forecast.run_uid)) return;
                    var pending = restricted(forecast,t);
                    var theme = {id:'forecast:' + (key || index),name:name,label:pending ? '盘前线索 · 待核验' : '盘前观察',
                        reason:prose(t.reason) || strings(t.evidence).join('；'),sourceAsOf:cutoff,sourceRunUid:text(forecast.run_uid),
                        sourceKind:'premarket_forecast',stocks:[],extraEvidence:[]};
                    var rows = list(t.stock_candidates).concat(list(forecast.stock_candidates).filter(function (r) {
                        return key && text(object(r).theme_key) === key;
                    }));
                    rows.forEach(function (rawStock) {
                        var row = object(rawStock), stockCode = code(row.stock_code || row.code);
                        if (!stockCode || seen[stockCode] || !dateMatches(row,['session_date','trade_date'],date) || !rowTimeValid(row,cutoff) ||
                            (row.run_uid && row.run_uid !== forecast.run_uid) || (row.source_run_uid && row.source_run_uid !== forecast.run_uid)) return;
                        var needsCheck = pending || restricted(forecast,t,row);
                        var stock = {id:'forecast:' + text(forecast.run_uid) + ':' + stockCode,code:stockCode,name:text(row.stock_name || row.short_name || row.name) || stockCode,
                            role:text(row.role || row.dynamic_role) || '主线观察股',status:needsCheck ? '待核验' : '等待确认',
                            reason:prose(row.reason || row.reasons) || strings(row.evidence).join('；'),
                            trigger:prose(row.trigger || row.trigger_condition || row.watch_conditions),
                            invalidation:prose(row.invalidation || row.invalidation_condition || row.invalidations),
                            sourceAsOf:cutoff,sourceRunUid:text(forecast.run_uid),sourceKind:'premarket_forecast',evidence:strings(row.evidence),changePct:null};
                        theme.stocks.push(stock); seen[stockCode] = theme;
                    });
                    result.themes.push(theme);
                });
            }
        }
        status('forecast',forecastOk,forecastOk ? stamp(forecast.cutoff_at) : '',forecastOk ? '冻结盘前研究，不授予交易权限' : '仅接受所选交易日09:08冻结记录');

        var auction = source(state,'auction'), auctionOk = false, auctionAsOf = auction && stamp(auction.cutoff_at);
        if (date && clockValid && !future && phase !== 'pre' && auction) {
            var assessmentRows = list(auction.assessments), summary = object(auction.summary);
            auctionOk = day(auction.execution_session_date || auction.session_date) === date && dateMatches(auction,['execution_session_date','session_date'],date) &&
                !!text(auction.source_run_uid) && ['COMPLETED','VALID_EMPTY'].includes(auction.status) &&
                ['PERSISTED_IMMUTABLE_RUN','POINT_IN_TIME_REPLAY'].includes(auction.evidence_mode) &&
                !!auctionAsOf && day(auctionAsOf) === date && auctionAsOf.slice(11) >= '09:15:00' && auctionAsOf.slice(11) <= '09:25:59' &&
                (!now || auctionAsOf <= now) && payloadTimeValid(auction,knownMax) &&
                !!day(auction.decision_date) && day(auction.decision_date) < date &&
                (!auction.data_date || (!!day(auction.data_date) && day(auction.data_date) <= day(auction.decision_date))) &&
                (!auction.source_data_date || (!!day(auction.source_data_date) && day(auction.source_data_date) <= day(auction.decision_date))) &&
                number(summary.reviewed_count) === assessmentRows.length && number(summary.candidate_count) === assessmentRows.length;
            if (auction.status === 'VALID_EMPTY' && assessmentRows.length) auctionOk = false;
            if (!auctionOk) issue('竞价观察的执行日、来源批次、数量或时点未通过核验。');
            if (auctionOk) {
                var group = {id:'auction:' + auction.source_run_uid,name:'独立竞价观察',label:auction.evidence_mode === 'POINT_IN_TIME_REPLAY' ? '竞价账本回放' : '已存竞价观察',reason:'来自独立策略批次，仅反映集合竞价时点；盘中仍需继续核对。',sourceAsOf:auctionAsOf,sourceRunUid:text(auction.source_run_uid),sourceKind:'auction_observation',stocks:[],extraEvidence:[]};
                assessmentRows.forEach(function (raw) {
                    var r = object(raw), c = code(r.stock_code), quoteAt = stamp(r.quote_at);
                    if (!c || !rowTimeValid(r,auctionAsOf) || !dateMatches(r,['execution_session_date','session_date'],date) ||
                        (r.source_run_uid && r.source_run_uid !== auction.source_run_uid)) return;
                    var quoteOk = !!quoteAt && day(quoteAt) === date && quoteAt.slice(11) >= '09:15:00' && quoteAt <= auctionAsOf;
                    var gateKnown = ['CONFIRMED','DATA_BLOCKED','UNBUYABLE','REJECT_CHASE','REJECT_WEAK'].includes(r.gate_status);
                    var blocked = ['UNBUYABLE','REJECT_CHASE','REJECT_WEAK'].includes(r.gate_status);
                    var label = !quoteOk || !gateKnown || r.gate_status === 'DATA_BLOCKED' ? '待核验' : blocked ? '暂不观察' : '等待开盘确认';
                    var reason = prose(r.reason || r.reasons);
                    if (seen[c]) {
                        seen[c].extraEvidence.push('独立竞价记录 ' + c + ' · ' + auctionAsOf + ' · 批次 ' + text(auction.source_run_uid) + '：' + label + (reason ? '；' + reason : '') + '。该记录不改变盘前主线的核验状态。');
                        return;
                    }
                    group.stocks.push({id:'auction:' + auction.source_run_uid + ':' + c,code:c,name:text(r.stock_name) || c,role:'竞价观察股',status:label,
                        reason:reason,trigger:prose(r.trigger || r.trigger_condition),invalidation:prose(r.invalidation || r.invalidations),
                        sourceAsOf:auctionAsOf,sourceRunUid:text(auction.source_run_uid),sourceKind:'auction_observation',
                        evidence:strings(r.reasons).concat(['集合竞价观察时点：' + auctionAsOf]),changePct:quoteOk ? number(r.gap_pct) : null});
                    seen[c] = group;
                });
                if (group.stocks.length) result.themes.push(group);
            }
        }
        status('auction',auctionOk,auctionOk ? auctionAsOf : '',phase === 'pre' ? '盘前09:08视图不使用09:25竞价结果' : '竞价结果与盘前主题分开核验');

        var market = source(state,'market'), marketOk = false, marketAsOf = market && stamp(market.data_time);
        if (date && clockValid && !future && market && phase !== 'pre' && !(historical && phase === 'live')) {
            var marketDay = day(market.trade_date), total = number(market.total_count), up = number(market.up_count), down = number(market.down_count);
            marketOk = marketDay === date && dateMatches(market,['requested_date'],date) && !market.is_historical_fallback &&
                (marketAsOf ? day(marketAsOf) === date && marketAsOf <= now : phase === 'post' && text(market.data_time) === date && market.freshness_status === 'close') &&
                (phase !== 'live' || ['fresh','paused','realtime'].includes(market.freshness_status)) &&
                (phase !== 'post' || (market.freshness_status === 'close' && now >= date + ' 15:00:00' && (!marketAsOf || marketAsOf >= date + ' 15:00:00'))) &&
                total !== null && Number.isInteger(total) && total > 0 && up !== null && down !== null && Number.isInteger(up) && Number.isInteger(down) && up >= 0 && down >= 0 && up + down <= total;
            if (marketOk) {
                result.facts.push({label:'上涨占比',value:numeric(up / total * 100,1,'%'),note:'上涨 ' + up + ' / 有报价样本 ' + total});
                result.facts.push({label:'下跌家数',value:String(down),note:'平盘 ' + (total - up - down) + ' 家'});
                var amount = number(market.total_amount);
                result.facts.push({label:'样本成交额',value:numeric(amount !== null && amount >= 0 ? amount / 1e8 : null,0,' 亿'),note:marketAsOf || date + ' 收盘'});
                result.headline = up / total >= 0.6 ? '上涨范围较广，重点核对主线延续' : down / total >= 0.6 ? '多数个股承压，先处理持仓风险' : '市场分化，围绕已记录条件观察';
            }
        }
        status('market',marketOk,marketOk ? marketAsOf || date : '',phase === 'pre' ? '盘前视图不使用当日盘中或收盘行情' : historical && phase === 'live' ? '历史盘中没有实时行情回填' : '仅使用同日、符合当前时段的行情');
        if (!result.facts.length) {
            result.facts = [{label:'观察时点',value:forecastOk ? '09:08 冻结' : '待核验',note:forecastOk ? date : '盘前记录尚未就绪'},
                {label:'主线方向',value:forecastOk ? String(result.themes.filter(function(t){return t.sourceKind === 'premarket_forecast';}).length) : '—',note:'真实来源中的观察线索'},
                {label:'竞价观察',value:auctionOk ? auctionAsOf.slice(11,16) : '—',note:phase === 'pre' ? '09:08视图不读取后续结论' : '独立来源，按时点核对'}];
        }

        var holding = source(state,'holdings'), holdingOk = false, holdingAsOf = holding && stamp(holding.knowledge_cutoff);
        if (date && clockValid && !future && !historical && phase !== 'pre' && holding) {
            holdingOk = day(holding.trade_date) === date && holding.historical_read_only !== true && !!holdingAsOf && day(holdingAsOf) === date && !!now && holdingAsOf <= now;
            if (holdingOk) list(holding.data).forEach(function(raw) {
                var r = object(raw), c = code(r.stock_code), suppliedCutoff = r.knowledge_cutoff || r.evaluated_at, rowCutoff = suppliedCutoff ? stamp(suppliedCutoff) : holdingAsOf;
                if (!c || number(r.shares) === null || number(r.shares) <= 0 || !dateMatches(r,['trade_date'],date) || !rowCutoff || day(rowCutoff) !== date || rowCutoff > now || !rowTimeValid(r,now)) return;
                var priceDate = day(r.price_trade_date), quoteAt = stamp(r.quote_observed_at);
                var priceOk = priceDate === date && r.same_session_price === true && (!r.quote_observed_at || (!!quoteAt && day(quoteAt) === date && quoteAt <= now));
                result.holdings.push({code:c,name:text(r.short_name || r.stock_name) || c,status:text(r.exit_intent) || 'WAIT_DATA',action:text(r.action),reason:text(r.reason),sourceAsOf:rowCutoff,
                    changePct:null,sellPlan:prose(object(r.sell_plan).label),emergencyExit:prose(object(r.emergency_exit).label),nextSessionPlan:text(r.next_session_plan),
                    latestPrice:priceOk ? number(r.latest_price) : null,priceDate:priceDate,sellableShares:number(r.sellable_shares),t1Blocked:r.t1_blocked === true,
                    actionPriority:number(r.action_priority),priceStatus:priceOk ? '同日报价' : '报价待核验',
                    emergencyPrice:number(object(r.emergency_exit).price)});
            });
            result.holdings.sort(function(a,b){return (a.actionPriority === null ? 99 : a.actionPriority) - (b.actionPriority === null ? 99 : b.actionPriority);});
        }
        status('holdings',holdingOk,holdingOk ? holdingAsOf : '',historical || phase === 'pre' ? '当前持仓不能冒充所选历史时点持仓' : '当前账户持仓与同日风险观察');

        var review = source(state,'review'), reviewOk = false, reviewAsOf = '';
        if (date && clockValid && !future && phase === 'post' && review && review.fallback !== true && day(review.date) === date && dateMatches(review,['requested_date'],date)) {
            var row = list(review.data).find(function(r) { return day(r.review_date) === date && r.publish_status === 'ready'; });
            if (row) {
                reviewAsOf = stamp(row.generated_at);
                var dataCutoff = stamp(row.data_cutoff_at);
                reviewOk = !!reviewAsOf && reviewAsOf >= date + ' 15:00:00' && reviewAsOf <= now &&
                    !!dataCutoff && dataCutoff >= date + ' 15:00:00' && dataCutoff <= reviewAsOf;
                if (reviewOk) {
                    result.review.text = text(row.compact_review);
                    var factors = row.factor_validation_json;
                    if (typeof factors === 'string') { try { factors = JSON.parse(factors); } catch (e) { factors = {}; } }
                    var selection = object(object(factors).selection_review);
                    result.review.selection = text(selection.compact_review);
                }
            }
        }
        if (phase === 'post' && review && !reviewOk) issue('同日复盘尚未通过发布状态与时间核验，未使用其他日期正文代替。');
        status('review',reviewOk,reviewOk ? reviewAsOf : '',phase === 'post' ? '只接受所选日已发布的盘后正文' : '盘前与盘中不读取盘后结论');
        var journal = source(state,'journal');
        status('journal',!!journal && day(journal.trade_date) === date && dateMatches(journal,['date'],date),'','计划与复盘按真实记录时间保存');
        if (!result.headline) result.headline = phase === 'pre' ? forecastOk ? '先确定观察主线，再写清确认条件' : '盘前证据待齐，先明确观察计划' : phase === 'live' ? '先对照原计划，再核对盘中变化' : reviewOk ? '核对当天判断与实际执行' : '整理当天记录，等待同日复盘';
        result.description = phase === 'pre' ? '主线与股票来自09:08冻结研究；观察条件缺失时需要补充，不自动生成买卖结论。' : phase === 'live' ? '盘前线索保持原始来源；竞价观察独立展示，不能代表此刻已经确认。' : '复盘保留记录的真实时间；收盘后补记不会变成早盘计划。';
        return result;
    }
    var api = {model:model,number:number,day:day,stamp:stamp};
    root.TradingDayModel = api;
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
