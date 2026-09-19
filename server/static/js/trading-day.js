/* Daily observation workspace. Personal notes never submit orders or change watchlists. */
(function (root) {
    'use strict';
    var active = null, timer = null;
    var phases = {pre:'盘前', live:'盘中', post:'盘后'};
    var statuses = {WATCHING:'观察中', WAITING:'等待条件', PAUSED:'暂不观察', REVIEWED:'已复盘'};
    var fields = ['stock_code','stock_name','theme','reason','trigger','invalidation','source_as_of','source_run_uid','status','note'];
    function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];}); }
    function list(v) { return Array.isArray(v) ? v : []; }
    function txt(v) { return typeof v === 'string' ? v : ''; }
    function time(v) { return root.TradingDayModel.stamp(v) || '时点待核验'; }
    function label(text) { return '<span class="td-tag">' + esc(text) + '</span>'; }
    function empty(text) { return '<p class="td-empty">' + esc(text) + '</p>'; }
    function link(tab, text) { return '<button type="button" class="td-textbtn" data-td-tab="'+esc(tab)+'">'+esc(text)+' ↗</button>'; }
    function planPayload(plan) { var p = {}; fields.forEach(function(k){p[k]=txt(plan[k]);}); p.status=p.status || 'WATCHING'; return p; }
    function journalPayload(journal, plans, review) { return {revision:journal.revision,plans:plans.map(planPayload),review:{text:review}}; }
    function journalOf(s) { var j=s.state.journal || {}; return j.status==='ready' && j.value.trade_date===s.date && Number.isSafeInteger(j.value.revision) && Array.isArray(j.value.plans) ? j.value : null; }
    function getModel(s) { return root.TradingDayModel.model(s.state,s.date,s.phase,s.options.clock()); }
    function defaultPhase(date, clock) {
        if (clock.today && date < clock.today) return 'post';
        return clock.phase === 'postmarket' || clock.phase === 'closed' ? 'post' : clock.is_intraday || clock.phase === 'midday_break' ? 'live' : 'pre';
    }
    function renderThemes(m, plans) {
        var h='<div class="td-sectionhead"><h3>'+(m.phase==='post'?'回看主线与观察股':'主线与观察股')+'</h3><span>先看方向，再看个股条件</span></div>';
        if (!m.themes.length) return h+empty('同日主线记录尚未就绪。数据齐备后将在这里列出真实观察股。');
        return h+'<div class="td-watchlist">'+m.themes.map(function(t,i){
            return '<details class="td-theme" data-td-key="theme-'+esc(t.id)+'" '+(i===0?'open':'')+'><summary><span class="td-number">'+String(i+1).padStart(2,'0')+'</span><span><strong>'+esc(t.name)+'</strong><small>'+esc(t.reason || '核对研究依据与个股条件')+'</small></span>'+label(t.label || '观察线索')+'</summary><div class="td-themebody"><div class="td-stockheader"><span>'+esc(t.sourceAsOf || '时点待核验')+'</span><span>'+list(t.stocks).length+' 只观察股</span></div>'+list(t.stocks).map(function(r,n){
                var p=plans.find(function(p){return p.stock_code===r.code;});
                return '<button type="button" class="td-stockrow '+(n>2?'td-morestock':'')+'" data-td-stock="'+esc(r.id)+'"><span class="td-stocktop"><strong>'+esc(r.name)+'</strong><span class="td-stockrole">'+esc(r.code)+' · '+esc(r.role)+'</span><span class="td-stockstate">'+esc(r.status || '待核验')+'</span></span><span class="td-stockcondition">'+esc(r.trigger || r.reason || '观察条件待补充，打开查看来源')+' <span aria-hidden="true">›</span></span>'+(p?'<span class="td-chosen">已在个人计划中'+(p.source_run_uid!==r.sourceRunUid?' · 原计划来自其他批次':'')+'</span>':'')+'</button>';
            }).join('')+(list(t.stocks).length>3?'<button type="button" class="td-textbtn" data-td-more aria-expanded="false">展开其余 '+(t.stocks.length-3)+' 只</button>':'')+(list(t.extraEvidence).length?'<details class="td-extra" data-td-key="extra-'+esc(t.id)+'"><summary>独立竞价补充记录</summary>'+list(t.extraEvidence).map(function(e){return '<p>'+esc(e)+'</p>';}).join('')+'</details>':'')+'</div></details>';
        }).join('')+'</div>';
    }
    function renderPlans(s, j) {
        return '<section class="td-box"><div class="td-sectionhead"><h3>我的观察计划</h3><span>'+ (j?j.plans.length+' 只':'未读取')+'</span></div><p class="td-muted">记录观察条件，按实际保存时间回看。</p>'+(j?(j.plans.length?j.plans.map(function(p){return '<button type="button" class="td-planstock" data-td-plan="'+esc(p.stock_code)+'"><strong>'+esc(p.stock_name || p.stock_code)+'</strong><span>'+esc(statuses[p.status] || '待核验')+'</span><small>'+esc(p.trigger || '尚未填写确认条件')+'</small><small>记录于 '+esc(time(p.created_at))+'</small></button>';}).join(''):empty('从左侧观察股打开详情，写下条件后加入计划。')):empty((s.state.journal||{}).status==='error'?'个人记录读取失败，请重试或使用账户登录。':'正在读取个人记录…'))+'</section>';
    }
    function renderHoldings(m) {
        return '<section class="td-box td-priority"><div class="td-sectionhead"><h3>先看持仓风险</h3>'+link('portfolio','我的自选')+'</div>'+ (m.holdings.length?m.holdings.slice(0,3).map(function(p){return '<button type="button" class="td-holding" data-td-holding="'+esc(p.code)+'"><strong>'+esc(p.name)+'</strong><span>'+esc(p.action || '等待风险核验')+'</span><small>'+esc(p.reason || '暂无同日风险说明')+'</small>'+(p.t1Blocked?'<small>T+1 限制 · 核对可卖数量</small>':'')+'</button>';}).join(''):empty(m.historical?'历史日期不展示当前持仓快照。':m.phase==='pre'?'09:08视图保留盘前信息；进入盘中查看当前持仓风险。':'暂未取得可核验的同日持仓风险记录。'))+'</section>';
    }
    function renderReview(s, m, j) {
        return '<section class="td-box td-review"><div class="td-sectionhead"><h3>当天复盘</h3>'+link('review','完整复盘')+'</div><p class="td-prose">'+esc(m.review.text || '同日盘后复盘尚未就绪。')+'</p>'+(m.review.selection?'<details data-td-key="selection"><summary>选股与执行回顾</summary><p class="td-prose">'+esc(m.review.selection)+'</p></details>':'')+'<label for="td-review-text">我的复盘记录</label><textarea id="td-review-text" maxlength="12000" placeholder="原先观察什么？条件有没有出现？下一次需要改进什么？">'+esc(s.reviewDraft===null?j && j.review && j.review.text || '':s.reviewDraft)+'</textarea><div class="td-reviewfoot"><button type="button" class="td-primary" data-td-save-review '+(!j || s.saving?'disabled':'')+'>保存复盘</button><span>'+esc(s.reviewDraft!==null?'有未保存的修改':j && j.review && j.review.updated_at?'保存于 '+time(j.review.updated_at):'按真实保存时间记录')+'</span></div></section>';
    }
    function render(s, m) {
        var j=journalOf(s), plans=j?j.plans:[];
        return '<div class="td-pagehead"><div><h2>把一天的交易思路连起来</h2><p>'+esc(s.date)+' · '+(m.historical?'历史回看':'交易日观察')+'</p></div><div class="td-phases" role="tablist" aria-label="看盘阶段">'+Object.keys(phases).map(function(p){return '<button type="button" role="tab" id="td-phase-'+p+'" aria-controls="td-phase-panel" aria-selected="'+(p===s.phase)+'" tabindex="'+(p===s.phase?'0':'-1')+'" data-td-phase="'+p+'">'+phases[p]+'<small>'+({pre:'做准备',live:'看变化',post:'做验证'}[p])+'</small></button>';}).join('')+'</div></div><div id="td-phase-panel" role="tabpanel" aria-labelledby="td-phase-'+s.phase+'"><section class="td-hero"><div class="td-eyebrow"><i></i>'+phases[s.phase]+' · 现在关注什么</div><div class="td-heroline"><h2>'+esc(m.headline)+'</h2>'+link('workbench','市场全景')+'</div><p>'+esc(m.description)+'</p><div class="td-facts">'+m.facts.slice(0,3).map(function(f){return '<article><span>'+esc(f.label)+'</span><strong>'+esc(f.value)+'</strong><small>'+esc(f.note)+'</small></article>';}).join('')+'</div></section><div class="td-main"><div>'+renderThemes(m,plans)+(m.phase==='post'?renderReview(s,m,j):'')+'</div><aside>'+renderHoldings(m)+renderPlans(s,j)+'</aside></div></div><div class="td-message" role="status" aria-live="polite">'+esc(s.message || '')+'</div><details class="td-sources" data-td-key="sources"><summary>数据来源与核验状态'+(m.issues.length?' · '+m.issues.length+' 项提示':'')+'</summary>'+m.issues.map(function(i){return '<p>'+esc(i)+'</p>';}).join('')+'<div class="td-sourcegrid">'+m.sourceStatus.map(function(r){return '<article><strong>'+esc(r.label)+'</strong><span>'+esc(r.status)+'</span><small>'+esc(r.asOf || r.note)+'</small>'+((s.state[r.key]||{}).status==='error'?'<button type="button" class="td-textbtn" data-td-retry="'+esc(r.key)+'">重试</button>':'')+'</article>';}).join('')+'</div></details><footer class="td-footline"><span>观察记录 · 不等于交易指令</span>'+link('trading-v3-candidates','研究候选')+link('market-radar','盘中异动')+'</footer>';
    }
    function stop() {
        clearTimeout(timer); timer=null;
        if (active) {
            active.cancelled=true;
            if(active.dialog.open) active.dialog.close();
            active.container.onclick=null; active.container.oninput=null; active.container.onkeydown=null;
        }
        active=null;
    }
    function load(date, container, options) {
        if(active && active.date===date && active.container===container && container.contains(active.base)) return active.refresh();
        stop();
        var s={date:date,container:container,options:options,state:{},phase:defaultPhase(date,options.clock()),reviewDraft:null,saving:false,cancelled:false,message:'',drawerToken:0};
        active=s;
        container.innerHTML='<div class="trading-day"><div class="td-base"></div><dialog class="td-dialog" aria-labelledby="td-dialog-title"></dialog></div>';
        s.base=container.querySelector('.td-base'); s.dialog=container.querySelector('dialog');
        s.dialog.oncancel=function(){s.drawerToken++;s.editPlan=null;};
        var d=encodeURIComponent(date), paths={forecast:'/api/hot-data/premarket-theme-forecast?session_date='+d,market:'/api/monitor/data?date='+d,auction:'/api/v3/premarket/auction-gate?execution_session_date='+d,holdings:'/api/portfolio/holding-strategy?trade_date='+d,review:'/api/hot-data/daily-review/quant?review_date='+d,journal:'/api/trading-day/journal?trade_date='+d};
        Object.keys(paths).forEach(function(k){s.state[k]={status:'loading'};});
        function current(){return active===s && !s.cancelled;}
        function paint() {
            if(!current()) return;
            var opened=new Set(), known=new Set(), expanded=new Set(), focus=s.base.contains(document.activeElement)?document.activeElement:null;
            s.base.querySelectorAll('details').forEach(function(e){known.add(e.dataset.tdKey);});
            s.base.querySelectorAll('details[open]').forEach(function(e){opened.add(e.dataset.tdKey);});
            s.base.querySelectorAll('.td-theme.td-expanded').forEach(function(e){expanded.add(e.dataset.tdKey);});
            var focusAttr=focus && Array.from(focus.attributes).find(function(a){return a.name.indexOf('data-td-')===0;}), focusId=focus && focus.id;
            var selection=focus && typeof focus.selectionStart==='number'?[focus.selectionStart,focus.selectionEnd]:null;
            s.model=getModel(s); s.base.innerHTML=render(s,s.model);
            s.base.querySelectorAll('details').forEach(function(e){if(known.has(e.dataset.tdKey))e.open=opened.has(e.dataset.tdKey);});
            s.base.querySelectorAll('.td-theme').forEach(function(e){if(expanded.has(e.dataset.tdKey)){e.classList.add('td-expanded');var b=e.querySelector('[data-td-more]');if(b){b.textContent='收起其余观察股';b.setAttribute('aria-expanded','true');}}});
            s.painted=true;
            var restored=focusId?s.base.querySelector('#'+focusId):focusAttr?Array.from(s.base.querySelectorAll('['+focusAttr.name+']')).find(function(e){return e.getAttribute(focusAttr.name)===focusAttr.value;}):null;
            if(restored){restored.focus({preventScroll:true});if(selection)restored.setSelectionRange(selection[0],selection[1]);}
            syncSaveButtons();
        }
        function request(key) {
            if(!current() || (key==='journal' && s.saving))return Promise.resolve();
            return options.request(paths[key],20000,{cache:'no-store'}).then(function(v){
                if(!current())return;
                if(!v || v.error || v.status==='error')throw new Error('unavailable');
                if(key==='journal'){
                    if(v.trade_date!==date || !Number.isSafeInteger(v.revision) || !Array.isArray(v.plans))throw new Error('invalid journal');
                    var j=journalOf(s);if(j && j.revision>v.revision)return;
                }
                s.state[key]={status:'ready',value:v};
            }).catch(function(){if(current())s.state[key]={status:'error'};}).finally(paint);
        }
        function syncSaveButtons(){
            var j=journalOf(s), m=getModel(s), exists=j && s.editPlan && j.plans.some(function(p){return p.stock_code===s.editPlan.stock_code;});
            s.dialog.querySelectorAll('[data-td-save-plan],[data-td-remove-plan]').forEach(function(b){b.disabled=s.saving || !j || (!exists && !m.canPlan);});
        }
        function close(){s.drawerToken++;s.dialog.close();s.editPlan=null;}
        function field(name,title,value,max){return '<label for="td-'+name+'">'+title+'</label><textarea id="td-'+name+'" name="'+name+'" maxlength="'+(max||1200)+'" rows="2">'+esc(value)+'</textarea>';}
        function showStock(r, saved) {
            s.drawerToken++;
            var j=journalOf(s), existing=saved || j && j.plans.find(function(p){return p.stock_code===r.code;});
            var p=existing || {stock_code:r.code,stock_name:r.name,theme:r.theme || '',reason:r.reason || '',trigger:r.trigger || '',invalidation:r.invalidation || '',source_as_of:r.sourceAsOf || '',source_run_uid:r.sourceRunUid || '',status:'WATCHING',note:''};
            s.editPlan=planPayload(p);
            var original=existing && existing.original;
            s.dialog.innerHTML='<div class="td-drawerhead"><span>'+esc(existing?'个人观察计划':({premarket_forecast:'盘前研究观察',auction_observation:'独立竞价观察'}[r.sourceKind] || '个股观察'))+'</span><button type="button" class="td-close" data-td-close aria-label="关闭详情">×</button></div><h2 id="td-dialog-title">'+esc(p.stock_name || p.stock_code)+' <small>'+esc(p.stock_code)+'</small></h2><p class="td-muted">'+esc(p.theme)+' · '+esc(existing?statuses[p.status]:r.status)+'</p><section class="td-drawsection"><h3>观察依据与时点</h3><p>'+esc(existing?'记录于 '+time(p.created_at):r.reason || '来源未给出明确观察依据')+'</p><ul>'+list(r.evidence).map(function(e){return '<li>'+esc(e)+'</li>';}).join('')+'</ul><p class="td-muted">来源时点 '+esc(p.source_as_of || '待核验')+'</p><details><summary>查看来源批次'+(original?'与初始条件':'')+'</summary><p>'+esc(p.source_run_uid || '未提供')+'</p>'+(original?'<p>初始理由：'+esc(original.reason || '未填写')+'</p><p>初始确认：'+esc(original.trigger || '未填写')+'</p><p>初始失效：'+esc(original.invalidation || '未填写')+'</p>':'')+'</details></section><form class="td-planform">'+field('reason','为什么观察',p.reason)+field('trigger','出现什么条件再重点关注',p.trigger)+field('invalidation','什么情况停止观察',p.invalidation)+'<label for="td-plan-status">我的观察状态</label><select id="td-plan-status" name="status">'+Object.keys(statuses).map(function(k){return '<option value="'+k+'" '+(p.status===k?'selected':'')+'>'+statuses[k]+'</option>';}).join('')+'</select>'+field('note','补充记录',p.note,2000)+'<p class="td-muted">'+(existing?'修改保留初始条件及真实记录时间。':getModel(s).canPlan?'以现在的时间加入个人计划。':'当前仅可阅读；新增计划需要当日交易时钟通过核验。')+'</p><div class="td-drawerbottom"><button type="submit" class="td-primary" data-td-save-plan>'+(existing?'保存修改':'加入我的计划')+'</button>'+(existing?'<button type="button" class="td-secondary" data-td-remove-plan>移出计划</button>':'')+'<button type="button" class="td-textbtn" data-td-current-stock="'+esc(p.stock_code)+'">当前个股详情 ↗</button></div><p class="td-dialog-message" role="status" aria-live="polite"></p></form>';
            s.dialog.querySelector('form').onsubmit=function(e){e.preventDefault();savePlan(false);};
            if(getModel(s).historical){
                ['reason','trigger','invalidation'].forEach(function(k){s.dialog.querySelector('[name="'+k+'"]').readOnly=true;});
                var remove=s.dialog.querySelector('[data-td-remove-plan]');if(remove)remove.remove();
                s.dialog.querySelector('.td-planform>.td-muted').textContent='历史计划保留原条件，可补充状态、记录与盘后复盘。';
            }
            syncSaveButtons(); if(!s.dialog.open)s.dialog.showModal();
        }
        function dialogMessage(message){var e=s.dialog.querySelector('.td-dialog-message');if(e)e.textContent=message;}
        function save(change, drawerToken) {
            var j=journalOf(s);if(!j || s.saving)return Promise.resolve(false);
            function message(text){if(drawerToken!==undefined && drawerToken===s.drawerToken)dialogMessage(text);}
            s.saving=true; paint(); message('正在保存…');
            var next=change(j), reviewDraft=s.reviewDraft;
            return options.request(paths.journal,15000,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(journalPayload(j,next.plans,next.review))}).then(function(v){
                if(!current())return false;
                if(!v || v.trade_date!==date || !Number.isSafeInteger(v.revision) || v.revision<=j.revision || !Array.isArray(v.plans))throw new Error('unconfirmed');
                s.state.journal={status:'ready',value:v};
                if(next.savedReview && s.reviewDraft===reviewDraft)s.reviewDraft=null;
                s.message='已保存 · '+time(v.updated_at); message('已保存'); return true;
            }).catch(function(error){
                if(!current())return false;
                s.message=error.httpStatus===409?'记录已在其他页面更新。正在重新读取，请对照最新记录后再保存。':'保存未确认，输入已保留。请重试并核对保存时间。';
                message(s.message);
                if(error.httpStatus===409){
                    s.saving=false;
                    return request('journal').then(function(){
                        var latest=journalOf(s), p=latest && s.editPlan && latest.plans.find(function(p){return p.stock_code===s.editPlan.stock_code;});
                        message(s.message+(p?' 最新确认条件：'+p.trigger+'；失效条件：'+p.invalidation+'；补充：'+p.note:''));
                        return false;
                    });
                }
                return false;
            }).finally(function(){if(current()){s.saving=false;paint();}});
        }
        function savePlan(remove) {
            var j=journalOf(s); if(!j || !s.editPlan || s.saving)return;
            var code=s.editPlan.stock_code, exists=j.plans.some(function(p){return p.stock_code===code;});
            if(!exists && !getModel(s).canPlan){dialogMessage('当前不能新增计划，请核对观察日期与交易时钟。');return;}
            var plan=Object.assign({},s.editPlan), drawerToken=s.drawerToken;
            if(!remove){var form=s.dialog.querySelector('form');['reason','trigger','invalidation','status','note'].forEach(function(k){plan[k]=form.elements[k].value;});}
            save(function(latest){var plans=latest.plans.filter(function(p){return p.stock_code!==code;});if(!remove)plans.push(plan);return {plans:plans,review:latest.review.text};},drawerToken).then(function(ok){if(ok && current() && drawerToken===s.drawerToken)close();});
        }
        container.onclick=function(event){
            var b=event.target.closest('button');if(!b || !container.contains(b))return;
            if(b.hasAttribute('data-td-phase')){s.phase=b.dataset.tdPhase;paint();}
            else if(b.hasAttribute('data-td-tab'))options.navigate(b.dataset.tdTab);
            else if(b.hasAttribute('data-td-current-stock')){close();options.stock(b.dataset.tdCurrentStock);}
            else if(b.hasAttribute('data-td-close'))close();
            else if(b.hasAttribute('data-td-more')){var t=b.closest('.td-theme'), expanded=t.classList.toggle('td-expanded');b.setAttribute('aria-expanded',String(expanded));b.textContent=expanded?'收起其余观察股':'展开其余 '+(t.querySelectorAll('.td-morestock').length)+' 只';}
            else if(b.hasAttribute('data-td-retry'))request(b.dataset.tdRetry);
            else if(b.hasAttribute('data-td-stock')){var r;getModel(s).themes.some(function(t){r=t.stocks.find(function(p){return p.id===b.dataset.tdStock;});if(r)r=Object.assign({},r,{theme:t.name});return !!r;});if(r)showStock(r);}
            else if(b.hasAttribute('data-td-plan')){var j=journalOf(s), p=j && j.plans.find(function(p){return p.stock_code===b.dataset.tdPlan;});if(p)showStock({code:p.stock_code,evidence:[]},p);}
            else if(b.hasAttribute('data-td-holding')){
                var p=getModel(s).holdings.find(function(p){return p.code===b.dataset.tdHolding;});if(!p)return;
                s.drawerToken++;
                s.editPlan=null;s.dialog.innerHTML='<div class="td-drawerhead"><span>当前持仓风险</span><button type="button" class="td-close" data-td-close aria-label="关闭详情">×</button></div><h2 id="td-dialog-title">'+esc(p.name)+'</h2><p>'+esc(p.action)+'</p><section class="td-drawsection"><p>'+esc(p.reason)+'</p><dl><dt>常规观察</dt><dd>'+esc(p.sellPlan || '尚未提供')+'</dd><dt>紧急条件</dt><dd>'+esc(p.emergencyExit || '尚未提供')+'</dd><dt>下一交易日</dt><dd>'+esc(p.nextSessionPlan || '尚未提供')+'</dd><dt>可卖数量</dt><dd>'+esc(p.sellableShares===null?'待核验':p.sellableShares)+(p.t1Blocked?' · T+1 限制':'')+'</dd><dt>风险评估时间</dt><dd>'+esc(p.sourceAsOf)+'</dd></dl></section><button type="button" class="td-textbtn" data-td-current-stock="'+esc(p.code)+'">当前个股详情 ↗</button>';s.dialog.showModal();
            }
            else if(b.hasAttribute('data-td-remove-plan'))savePlan(true);
            else if(b.hasAttribute('data-td-save-review')){var text=s.reviewDraft; if(text===null){var j=journalOf(s);text=j && j.review.text || '';} save(function(j){return {plans:j.plans,review:text,savedReview:true};});}
        };
        container.oninput=function(event){if(event.target.id==='td-review-text'){s.reviewDraft=event.target.value;var note=s.base.querySelector('.td-reviewfoot span');if(note)note.textContent='有未保存的修改';}};
        container.onkeydown=function(event){
            var b=event.target.closest('[data-td-phase]');if(!b || !['ArrowLeft','ArrowRight','Home','End'].includes(event.key))return;
            event.preventDefault();var keys=Object.keys(phases), index=keys.indexOf(s.phase);s.phase=keys[event.key==='Home'?0:event.key==='End'?2:(index+(event.key==='ArrowRight'?1:2))%3];paint();s.base.querySelector('[data-td-phase="'+s.phase+'"]').focus();
        };
        s.refresh=function(){if(s.loading)return s.loading;s.loading=Promise.all(Object.keys(paths).map(request)).then(function(){return options.refreshClock?options.refreshClock():null;}).then(function(){paint();var failed=Object.keys(paths).filter(function(k){return s.state[k].status==='error';}).length;return failed?{loadError:failed+' 项数据读取失败，可在来源状态重试'}:{};}).finally(function(){s.loading=null;});return s.loading;};
        function schedule(){if(!current())return;timer=setTimeout(function(){
            if(!current())return;if(document.hidden || options.isActive && !options.isActive()){schedule();return;}
            Promise.resolve(options.refreshClock?options.refreshClock():null).then(function(){if(!current())return;var clock=options.clock();if(clock.is_intraday===true && clock.today===date && clock.active_trade_date===date)return s.refresh();paint();}).catch(function(){}).finally(schedule);
        },60000);}
        paint();return s.refresh().then(function(outcome){if(current()){schedule();if(options.status)options.status(outcome.loadError || '今日看盘已更新',!!outcome.loadError);}return outcome;});
    }
    root.TradingDayDesk={load:load,stop:stop,render:render,journalPayload:journalPayload,defaultPhase:defaultPhase};
    if(typeof module!=='undefined' && module.exports)module.exports=root.TradingDayDesk;
})(typeof window!=='undefined'?window:globalThis);
