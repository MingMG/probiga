(function (root) {
    'use strict';
    const titles = {seats:'公开席位交易观察', drawdowns:'区间高点回撤研究', breadth:'行业均线宽度', macro:'宏观经济原表', global:'外围与大类资产', futures:'全球期货监测'};
    const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const number = value => value !== null && value !== undefined && Number.isFinite(Number(value)) ? Number(value) : null;
    const format = (value, unit='', digits=2) => number(value) === null ? '—' : Number(value).toLocaleString('zh-CN', {minimumFractionDigits:digits,maximumFractionDigits:digits}) + unit;
    const pct = value => `<span class="${number(value) > 0 ? 'rt-up' : number(value) < 0 ? 'rt-down' : ''}">${number(value) > 0 ? '+' : ''}${format(value,'%')}</span>`;
    const money = value => number(value) === null ? '—' : format(Number(value)/1e8,'亿');
    const metric = (name,value) => `<article class="rt-metric"><span>${escape(name)}</span><strong>${value}</strong></article>`;
    const stock = row => `<button class="rt-stock" type="button" data-rt-stock="${escape(row.stock_code)}" data-name="${escape(row.short_name)}">${escape(row.short_name || row.stock_code)}</button><small>${escape(row.stock_code)}</small>`;
    const table = (headers,rows) => `<div class="rt-scroll"><table><thead><tr>${headers.map(x=>'<th>'+escape(x)+'</th>').join('')}</tr></thead><tbody>${rows.join('') || '<tr><td colspan="'+headers.length+'">所选范围没有符合口径的记录</td></tr>'}</tbody></table></div>`;
    const row = cells => '<tr>'+cells.map(x=>'<td>'+x+'</td>').join('')+'</tr>';

    function render(kind, data) {
        if (kind==='futures') return '<div class="rt-kpis">'+metric('取得报价',format(data.available_count,'项',0))+
            metric('目标品种',format(data.expected_count,'项',0))+metric('观察日期',escape(data.observation_date))+metric('刷新频率','2分钟')+'</div>'+
            '<section class="rt-panel"><h3>当前期货与CFD参考报价</h3>'+table(['品种 / 合约','报价点位','较前参考价','30分钟','1小时','4小时','24小时','市场时间'],(data.rows||[]).map(x=>row([
                escape(x.display_name)+'<small>'+escape(x.contract)+' · '+escape(x.source)+'</small>',format(x.price),pct(x.change_pct),
                ...['30m','1h','4h','24h'].map(key=>pct(x.horizons?.[key]?.change_pct)),
                escape(x.market_time)+'<small>'+(x.freshness==='RECENT'?'最近报价':'最近一次观察，非当前成交')+'</small>'
            ])))+'</section><p class="rt-note">'+escape(data.method)+'<br>'+escape(data.horizon_method)+
            '<br>缺少来源：'+(data.missing_instruments||[]).map(escape).join('；')+'</p>';
        if (kind==='macro') return '<section class="rt-panel"><h3>'+escape(data.title)+'</h3><p>官方发布时间 '+escape(data.published_at)+' · <a href="'+escape(data.source_url)+'" target="_blank" rel="noopener noreferrer">国家统计局原文</a></p>'+
            table(['指标（原单位）',escape(data.headers?.[1] || '当月')+' · 绝对量','当月同比(%)',escape(data.headers?.[2] || '累计')+' · 绝对量','累计同比(%)'],(data.rows||[]).map(x=>row([
                escape(x.indicator),escape(x.current_value),escape(x.current_yoy),escape(x.cumulative_value),escape(x.cumulative_yoy)
            ])))+'</section><p class="rt-note">'+escape(data.method)+'</p>';
        if (kind==='global') return '<div class="rt-kpis">'+metric('有效观察',format(data.available_count,'项',0))+metric('批次标的',format(data.expected_count,'项',0))+
            metric('批次状态',data.acquisition_status==='COMPLETE'?'完整':data.acquisition_status==='PARTIAL'?'部分可用':'不可用')+metric('采集时间',escape(data.known_at))+'</div>'+
            '<section class="rt-panel"><h3>同批次跨市场观察</h3>'+table(['标的','观察价格','较前收变化','市场时间','数据来源 / 缺失原因'],(data.rows||[]).map(x=>row([
                escape(x.display_name),x.availability==='available'?format(x.price):'未取得数据',pct(x.change_pct),escape(x.market_time),x.availability==='available'?escape(x.source):escape(x.missing_reason || '该批次未取得可核验报价')
            ])))+'</section><p class="rt-note">'+escape(data.method)+(data.producer?.state==='FAILED'?' 后台本轮采集未完成；可用记录继续展示，并保留实际采集时间。':'')+'<br>'+escape(data.horizon_note)+'<br>批次 '+escape(data.snapshot_id)+'</p>';
        if (kind === 'seats') {
            const charts = data.distribution || [], maximum = Math.max(1,...charts.map(x=>x.count));
            return '<div class="rt-kpis">' + metric('涉及股票',format(data.security_count,'只',0)) + metric('披露买入额',money(data.buy_amount)) +
                metric('披露净额',money(data.buy_amount-data.sell_amount)) + metric('成熟样本上涨比例',format(data.positive_ratio_pct,'%',1)) + '</div>' +
                '<div class="rt-columns"><section class="rt-panel"><h3>披露后股价表现</h3><p>均值 '+pct(data.mean_change_pct)+' · 中位数 '+pct(data.median_change_pct)+' · 成熟样本 '+format(data.mature_count,'个',0)+'</p><div class="rt-bars">' +
                charts.map((x,i)=>`<div><span>${x.count}</span><i class="${i<2?'rt-negative':'rt-positive'}" style="height:${Math.max(2,x.count/maximum*120)}px"></i><small>${escape(x.label)}</small></div>`).join('')+'</div></section>' +
                '<section class="rt-panel"><h3>共同上榜席位</h3><p>与所选席位在相同披露日、相同股票出现的买入记录。</p>'+
                table(['营业部','共同披露次数'],(data.co_disclosures||[]).map(x=>row([escape(x.seat),format(x.count,'',0)])))+'</section></div>'+
                '<section class="rt-panel"><h3>最近披露追踪</h3>'+table(['披露日','股票','披露买入','披露卖出','观察到期日','收盘价格变化'],(data.rows||[]).map(x=>row([
                    escape(x.trade_date),stock(x),money(x.buy_amount),money(x.sell_amount),escape(x.exit_date || '未成熟'),
                    x.price_change_pct === null ? (x.observation_status==='NOT_MATURE'?'未成熟':'缺少已核验行情') : pct(x.price_change_pct)
                ])))+'</section><p class="rt-note">'+escape(data.method)+'<br>'+escape(data.return_method)+'<br>'+escape(data.identity_basis)+'</p>';
        }
        if (kind === 'drawdowns') return '<div class="rt-kpis">'+metric('完整窗口样本',format(data.complete_sample_count,'只',0))+
            metric('成交额排序范围',format(data.liquidity_top_count,'只',0))+metric('筛选结果',format(data.rows?.length,'只',0))+metric('交易日窗口',format(data.session_count,'日',0))+'</div>'+
            '<section class="rt-panel"><h3>区间高点至所选收盘价</h3>'+table(['股票','一级行业','区间最高价','所选收盘价','高点回撤','区间成交额'],(data.rows||[]).map(x=>row([
                stock(x),escape(x.industry || '未启用行业筛选'),format(x.peak),format(x.price),pct(-x.drawdown_pct),money(x.period_amount)
            ])))+'</section><p class="rt-note">'+escape(data.method)+'<br>'+escape(data.coverage_note)+
            (Object.keys(data.native_partitions||{}).length?'<br>复用已认证原生归档分区：'+Object.keys(data.native_partitions).map(escape).join('、'):'')+'</p>';
        return '<div class="rt-kpis">'+metric('均线窗口',format(data.period,'日',0))+metric('有效股票',format(data.sample_count,'只',0))+
            metric('站上均线',format(data.above_count,'只',0))+metric('全样本宽度',format(data.breadth_pct,'%',1))+'</div>'+
            '<section class="rt-panel"><h3>行业宽度 · 收盘价高于 MA'+data.period+'</h3>'+table(['行业','宽度','站上均线','有效样本 / 行业成员'],(data.rows||[]).map(x=>row([
                escape(x.industry_name),number(x.breadth_pct)===null?'—':`<span class="rt-width"><i style="width:${Math.max(0,Math.min(100,x.breadth_pct))}%"></i><b>${format(x.breadth_pct,'%',1)}</b></span>`,
                format(x.above_count,'只',0),format(x.sample_count,'',0)+' / '+format(x.member_count,'',0)
            ])))+'</section><p class="rt-note">'+escape(data.method)+'<br>'+escape(data.reconstruction_basis)+
            '<br>行业成分：'+escape(data.membership_captured_at)+' · '+escape(data.membership_hash)+'</p>';
    }

    function controls(kind, state, data) {
        if (kind==='global') return '<label>观察方式<select data-rt-control="mode"><option value="latest"'+(state.mode==='latest'?' selected':'')+'>最新观察</option><option value="history"'+(state.mode==='history'?' selected':'')+'>所选日期历史批次</option></select></label>';
        if (kind==='macro' || kind==='futures') return '';
        const select = (key,label,options) => `<label>${label}<select data-rt-control="${key}">${options.map(([value,text])=>`<option value="${escape(value)}"${String(state[key])===String(value)?' selected':''}>${escape(text)}</option>`).join('')}</select></label>`;
        if (kind==='seats') return select('seat','公开营业部',(data?.seats||[]).map(x=>[x.seat,x.seat]))+
            select('sessions','观察窗口',[[5,'近5个交易日'],[10,'近10个交易日']])+select('horizon','披露后观察',[[1,'1个交易日'],[3,'3个交易日'],[5,'5个交易日'],[10,'10个交易日']]);
        if (kind==='drawdowns') return `<label>区间起点<input type="date" data-rt-control="start" value="${escape(state.start)}" max="${escape(state.date)}"></label>`+
            select('tmt','范围',[['false','成交额前100完整样本'],['true','前100中的TMT行业']]);
        return select('period','均线窗口',[[20,'20个交易日'],[30,'30个交易日']]);
    }

    function load(kind,date,container,options) {
        const state = {date,mode:kind==='global'&&container.dataset.rtGlobalMode==='history'?'history':'latest',seat:'',sessions:5,horizon:1,period:20,tmt:'false',start:date.slice(0,4)+(date.slice(5,7)>='07'?'-07-01':'-01-01')};
        const generation = String(Date.now())+Math.random();
        container.dataset.rtGeneration = generation;
        let latest = null, sequence = 0, timer = null;
        function current() {return container.dataset.rtGeneration===generation && (!options.isActive || options.isActive());}
        function shell(body, busy=false) {
            container.innerHTML='<div class="research-tools"><header><div><h2>'+titles[kind]+'</h2><p>'+(kind==='futures'||kind==='global'&&state.mode==='latest'?'当前观察 · 不按全局历史日期回放':'观察日期 '+escape(date)+' · 公开数据研究')+'</p></div><button type="button" data-rt-refresh'+(busy?' disabled':'')+'>刷新</button></header>'+
                '<form class="rt-controls">'+controls(kind,state,latest)+'</form>'+body+'</div>';
        }
        async function refresh() {
            clearTimeout(timer);
            const request = ++sequence;
            const query = new URLSearchParams(kind==='futures'?{}:{date});
            const path = {seats:'seat-observation',drawdowns:'period-drawdowns',breadth:'industry-breadth',macro:'macro-release',global:'global-context',futures:'global-futures'}[kind];
            const keys = {seats:['seat','sessions','horizon'],drawdowns:['start','tmt'],breadth:['period'],macro:[],global:['mode'],futures:[]}[kind];
            keys.forEach(key=>query.set(key,state[key]));
            shell(latest ? render(kind,latest) : '<p class="rt-loading" role="status">正在读取已保存的观察批次…</p>',true);
            try {
                const data = await options.request('/api/research/'+path+'?'+query,kind==='drawdowns'?115000:kind==='breadth'?35000:30000,{cache:'no-store'});
                if (!current() || request!==sequence) return;
                if (kind!=='futures' && data.trade_date!==date) throw new Error('返回的数据日期与所选日期不一致');
                if (data.status==='unavailable') throw new Error('所选范围尚未取得可核验的公开记录');
                latest=data;
                if (kind==='seats') state.seat=data.selected_seat;
                shell(render(kind,data)+'<footer>数据可用时间 '+escape(data.known_at)+(kind==='futures'||kind==='global'&&state.mode==='latest'?' · 最新批次，保留真实市场时间':' · 历史研究按当前取得的证据展示')+'</footer>');
            } catch (error) {
                if (current() && request===sequence) shell((latest?render(kind,latest):'')+'<p class="rt-error" role="alert">'+escape(error.message || '数据读取失败')+'；已显示的数据保留原采集时间。</p>');
            } finally {
                if ((kind==='futures'||kind==='global'&&state.mode==='latest') && current() && request===sequence) timer=setTimeout(()=>{if(current())refresh();},120000);
            }
        }
        container.onchange = event => {
            const field=event.target.closest('[data-rt-control]');
            if (field) {if(field.dataset.rtControl==='mode'){latest=null;container.dataset.rtGlobalMode=field.value;} state[field.dataset.rtControl]=field.value; if(field.dataset.rtControl==='sessions')state.seat=''; refresh();}
        };
        container.onsubmit = event => event.preventDefault();
        container.onclick = event => {
            const button=event.target.closest('button');
            if (!button) return;
            if (button.hasAttribute('data-rt-refresh')) refresh();
            if (button.dataset.rtStock && options.stock) options.stock(button.dataset.rtStock,button.dataset.name);
        };
        return refresh();
    }
    const api = {load,render};
    if (typeof module!=='undefined' && module.exports) module.exports=api;
    root.ResearchTools=api;
})(typeof window==='undefined'?globalThis:window);
