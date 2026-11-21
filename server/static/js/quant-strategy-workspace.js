(function () {
    'use strict';
    const esc = v => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const number = v => v == null || v === '' || !Number.isFinite(Number(v)) ? null : Number(v);
    const metric = (v, suffix='') => number(v) === null ? '未知' : Number(v).toFixed(2)+suffix;
    const stages = {SHADOW:'影子观察',CANDIDATE:'待晋级',ACTIVE:'正式',PAUSED:'暂停',RETIRED:'已淘汰',FORMAL:'正式'};
    const stateNames = {strong_trend:'强趋势',low_range:'低位震荡',high_range:'高位震荡',extreme_event:'极端事件'};
    function render(s) {
        const d=s.data, g=d.governance||{}, tasks=d.tasks||[], e=d.external_observation||{}, catalog=d.simulation_catalog||{};
        const governanceTask=tasks.find(t=>t.task_type==='strategy_governance_daily')||{};
        const current=d.governance_current===true;
        const rows=(d.strategies||[]).slice();
        rows.sort((a,b)=>{
            const field={net:'net_expectancy_pct',drawdown:'max_drawdown_pct',samples:'completed_trades'}[s.order];
            const av=field?number((a.windows[s.window]||{})[field]):null, bv=field?number((b.windows[s.window]||{})[field]):null;
            if(av===null&&bv!==null)return 1;if(bv===null&&av!==null)return -1;
            return av!==null&&bv!==null&&av!==bv ? (s.order==='drawdown'?av-bv:bv-av) : a.strategy_name.localeCompare(b.strategy_name,'zh-CN');
        });
        let h='<header class="qsw-header"><div><small>策略清单 · 选股 · 收益证据</small><h2>策略池与竞技</h2><p>注册 '+esc(d.strategy_count)+' 个策略；QMT 当前模拟 '+esc((catalog.strategies||[]).length)+' 策略、'+esc((catalog.combinations||[]).length)+' 组合。下方任务数量另计。</p></div><button type="button" data-qsw-action="refresh">刷新结果</button></header>';
        h+='<div class="qsw-links">'+[['strategy-center','策略研究与竞技'],['qmt-strategy-results','QMT选股与后续表现'],['strategy-backtest','回测结果'],['recommended','正式股票池']].map(x=>'<button type="button" data-qsw-tab="'+x[0]+'">'+x[1]+'</button>').join('')+'</div>';
        h+='<section class="qsw-context"><div><strong>'+esc(s.market?s.market.market_mode_label:'市场状态')+'</strong><span>'+esc(s.market?s.market.tactical_style:'市场观察在任务状态读回后更新')+'</span></div><div><strong>外围最新观察</strong><span>'+esc(e.captured_at||'暂无批次')+'</span></div><div><strong>策略实际使用依据</strong><span>'+esc(g.trade_date?g.trade_date+' · '+(stateNames[g.market_state]||g.market_state):'尚无治理结果')+'</span></div></section>';
        if(!current)h+='<p class="qsw-warning" role="status">所选日 '+esc(d.trade_date)+' 尚无当前治理结果'+(g.trade_date?'，最近结果停在 '+esc(g.trade_date):'')+'。'+esc(governanceTask.reason||'等待本轮治理通过输入校验')+'。最新外围观察尚不能标记为已用于策略决策。</p>';
        if(s.pool)h+='<p class="qsw-foot">正式股票池：'+esc(s.pool.status==='VERIFIED_EMPTY'?'本轮已分析 '+s.pool.analysis_count+' 只，符合条件 0 只，空池已验真':s.pool.status==='ACTIVE'?s.pool.active_count+' 只入池':s.pool.reason||'等待生成')+'。</p>';
        h+='<div class="qsw-toolbar"><label>观察窗口 <select data-qsw-control="window">'+[20,60,120].map(n=>'<option value="'+n+'"'+(s.window===String(n)?' selected':'')+'>'+n+' 个交易日</option>').join('')+'</select></label><label>比较 <select data-qsw-control="order">'+[['name','策略名称'],['net','扣费单笔均值'],['drawdown','最大回撤'],['samples','完成交易数']].map(x=>'<option value="'+x[0]+'"'+(s.order===x[0]?' selected':'')+'>'+x[1]+'</option>').join('')+'</select></label><span>'+esc(d.metric_basis)+'</span></div>';
        h+='<div class="qsw-table"><div class="qsw-columns"><span>策略 / 当前版本</span><span>阶段</span><span>完成交易</span><span>扣费单笔均值</span><span>胜率</span><span>最大回撤</span><span>市场适配分</span></div>';
        rows.forEach(row=>{
            const m=row.windows[s.window]||{}, picks=row.picks||[];
            h+='<details class="qsw-strategy" data-qsw-key="'+esc(row.strategy_key)+'"><summary class="qsw-columns"><span><strong>'+esc(row.strategy_name)+'</strong><small>'+esc(row.current_version)+'</small></span><span>'+esc(row.enabled?(stages[row.current_status]||row.current_status):'已停用')+'</span><span>'+esc(m.completed_trades==null?'未知':m.completed_trades)+'</span><span>'+metric(m.net_expectancy_pct,'%')+'</span><span>'+metric(m.win_rate_pct,'%')+'</span><span>'+metric(m.max_drawdown_pct,'%')+'</span><span>'+metric(m.market_match_score)+'</span></summary>';
            h+='<div class="qsw-detail"><p>'+esc(row.description||row.status_reason||'')+'</p><p>结果日期 '+esc(m.trade_date||'尚无当前版本结果')+'；'+esc(m.gate_reason||'没有匹配当前版本的已验证交易证据，不能参与收益排名或晋级。')+'</p>';
            h+='<h4>该次治理选股与筛选状态</h4>';
            if(!picks.length)h+='<p>暂无可绑定当前版本的选股结果。可在“QMT选股与后续表现”按执行日期查看独立模拟版本。</p>';
            else h+='<ul>'+picks.map(p=>'<li><button type="button" data-qsw-stock="'+esc(p.stock_code)+'">'+esc(p.stock_code)+' '+esc(p.stock_name)+'</button> · '+esc(p.pool_level)+' · '+esc(p.gate_status)+' · '+esc(Array.isArray(p.reasons)?p.reasons.join('；'):JSON.stringify(p.reasons||[]))+'</li>').join('')+'</ul>';
            h+='</div></details>';
        });
        h+='</div><p class="qsw-foot">影子观察、正式模拟及历史回测分别查看；未知值不会按 0 收益排名。展开策略可查看选股和未晋级原因。</p>';
        return h;
    }
    function paint(s) {
        const opened=new Set(Array.from(s.mount.querySelectorAll('details[open]')).map(e=>e.dataset.qswKey));
        const table=s.mount.querySelector('.qsw-table'), left=table?table.scrollLeft:0;
        const focused=s.mount.contains(document.activeElement)?document.activeElement:null;
        const focusControl=focused&&focused.dataset.qswControl;
        const focusKey=focused&&focused.closest('details')&&focused.closest('details').dataset.qswKey;
        const focusRefresh=focused&&focused.hasAttribute('data-qsw-action');
        s.mount.innerHTML=render(s);
        s.mount.querySelectorAll('details').forEach(e=>e.open=opened.has(e.dataset.qswKey));
        s.mount.querySelector('.qsw-table').scrollLeft=left;
        let restore;
        if(focusControl)restore=Array.from(s.mount.querySelectorAll('[data-qsw-control]')).find(e=>e.dataset.qswControl===focusControl);
        else if(focusKey){const detail=Array.from(s.mount.querySelectorAll('details')).find(e=>e.dataset.qswKey===focusKey);restore=detail&&detail.querySelector('summary');}
        else if(focusRefresh)restore=s.mount.querySelector('[data-qsw-action]');
        if(restore)restore.focus({preventScroll:true});
    }
    function load(day,mount,request,navigate,stock,isActive) {
        const s={mount,window:'20',order:'name',data:null,market:null,timer:null,busy:false};
        mount._strategyWorkspace=s;
        async function refresh() {
            if(s.busy||!mount.isConnected||(isActive&&!isActive()))return;
            s.busy=true;clearTimeout(s.timer);
            try {
                const d=await request('/api/quant-workbench/strategies?trade_date='+encodeURIComponent(day),20000,{cache:'no-store'});
                if(!mount.isConnected)return;
                const comparable=JSON.stringify({...d,registry_observed_at:null});
                if(comparable!==s.version){s.data=d;s.version=comparable;paint(s);}
                const err=mount.querySelector('.qsw-error');if(err)err.remove();
            } catch(e) {
                if(!mount.isConnected)return;
                if(!s.data)mount.innerHTML='';
                let error=mount.querySelector('.qsw-error');if(!error){error=document.createElement('p');error.className='qsw-error';mount.appendChild(error);}
                error.textContent='策略结果读取失败：'+(e.message||e)+'。已显示的结果保留原时间。';
            } finally {s.busy=false;if(mount.isConnected)s.timer=setTimeout(refresh,20000);}
        }
        mount.onclick=e=>{
            const tab=e.target.closest('[data-qsw-tab]'), code=e.target.closest('[data-qsw-stock]');
            if(tab)navigate(tab.dataset.qswTab);else if(code)stock(code.dataset.qswStock);else if(e.target.closest('[data-qsw-action]'))refresh();
        };
        mount.onchange=e=>{const field=e.target.dataset.qswControl;if(field&&s.data){s[field]=e.target.value;paint(s);}};
        refresh();
    }
    window.QuantStrategyWorkspace={load,setMarket(mount,market,pool){const s=mount&&mount._strategyWorkspace;if(s){s.market=market;s.pool=pool;if(s.data)paint(s);}},render};
})();
