const assert = require('node:assert/strict');
const {parseHTML} = require('linkedom');
const {window} = parseHTML('<html><body><div id="home"></div></body></html>');
global.window=window;global.document=window.document;
window.location=new URL('http://unit.test/');
Object.defineProperty(document,'hidden',{value:false,writable:true});
const page=require('../../server/static/js/market-home.js');
const container=document.getElementById('home');
const date='2026-11-23', sourceTime=date+'T10:30:00+08:00';
const sockets=[];
class Socket {
  constructor(url){this.url=url;sockets.push(this);}
  open(){this.onopen?.();}
  emit(components,time=sourceTime){this.onmessage?.({data:JSON.stringify({type:'update',trade_date:date,components,server_time:time})});}
  close(){this.closed=true;}
}
const observation={version:'one',source_at:sourceTime,received_at:sourceTime,age_seconds:0,age_basis:'source',state:'fresh',fresh_seconds:20,expired_seconds:60};
function snapshot(change=3){return {status:'partial',requested_date:date,trade_date:date,as_of:sourceTime,source:'verified',observation,
  as_of_basis:'qmt_snapshot',sample_size:10,unavailable_fields:['limit_ladder'],
  market_overview:{trade_date:date,up_count:6,flat_count:1,down_count:3,total_count:10,data_time:sourceTime,data_source:'verified',observation},
  rankings:{gainers:[{stock_code:'600001',short_name:'verified stock',trade_date:date,change_pct:change}]}};}
function event(id){return {event_type:'SEAL',event_family:'LIMIT_UP_LIFECYCLE',stock_code:String(id).padStart(6,'0'),snapshot_at:sourceTime,
  available_at:sourceTime,detail_json:{short_name:'event '+id}};}
const feed={requested_date:date,events:[event(1)],next_cursor:'cursor',observation};
const requestCalls=[];
let slowTrend;
const request=async url=>{
  requestCalls.push(url);
  if(url.includes('market-trend'))return new Promise(resolve=>{slowTrend=resolve;});
  if(url.includes('&before='))return {requested_date:date,events:[event(2)],next_cursor:null};
  return {};
};
const flush=()=>new Promise(resolve=>setImmediate(resolve));
(async()=>{
 try {
  const loading=page.load(date,container,{request,clock:()=>({active_trade_date:date,today:date}),streamFactory:url=>new Socket(url)});
  const socket=sockets[0];assert.match(socket.url,/^ws:\/\/unit\.test\/api\/monitor\/stream/);
  socket.open();socket.emit({snapshot:snapshot(),events:feed});await loading;
  assert.match(container.textContent,/verified stock/);
  assert.ok(!requestCalls.some(url=>/\/monitor\/(board|liquidity|sector-heatmap|events)/.test(url)),'opening current view acquired data through HTTP');
  const row=container.querySelector('[data-mh-key="stock:600001"]');
  const stock=row.querySelector('[data-mh-stock]');
  const emotion=container.querySelector('[data-mh-key="panel:情绪观察"]');
  const list=container.querySelector('[data-mh-events]');list.scrollTop=3000;
  socket.emit({snapshot:snapshot(5),events:{...feed,events:[event(3),event(1)]}});
  assert.equal(container.querySelector('[data-mh-key="stock:600001"]'),row,'a push replaced the ranking row');
  assert.equal(row.querySelector('[data-mh-stock]'),stock,'a push replaced an interactive stock control');
  assert.equal(container.querySelector('[data-mh-key="panel:情绪观察"]'),emotion);
  assert.equal(container.querySelector('[data-mh-events]'),list);
  assert.equal(list.scrollTop,3000);
  assert.match(row.textContent,/5\.00%/);
  assert.match(container.textContent,/有新事件/);
  assert.ok(!list.textContent.includes('event 3'),'incoming events moved the historical viewport');
  container.querySelector('[data-mh-events-latest]').click();
  assert.equal(list.scrollTop,0);assert.match(list.textContent,/event 3/);
  container.querySelector('[data-mh-events-more]').click();await flush();
  assert.match(list.textContent,/event 2/);
  assert.ok(requestCalls.some(url=>url.includes('&before=')));
  stock.dispatchEvent(new window.Event('pointerdown',{bubbles:true}));
  socket.emit({snapshot:snapshot(8)});
  assert.match(row.textContent,/5\.00%/,'a push repainted during a pointer interaction');
  document.dispatchEvent(new window.Event('pointerup',{bubbles:true}));
  await new Promise(resolve=>setTimeout(resolve,5));assert.match(row.textContent,/8\.00%/);
  socket.emit({},date+'T10:31:01+08:00');
  assert.match(container.textContent,/已过期，保留参考/);
  assert.match(row.textContent,/8\.00%/);
  assert.match(container.querySelector('[data-mh-refresh-status]').textContent,/推送已连接/);
  slowTrend({indices:[]});await flush();
  assert.equal(container.querySelector('[data-mh-key="stock:600001"]'),row);
  document.hidden=true;document.dispatchEvent(new window.Event('visibilitychange'));
  assert.ok(socket.closed);document.hidden=false;document.dispatchEvent(new window.Event('visibilitychange'));
  assert.equal(sockets.length,2);sockets[1].open();sockets[1].emit({snapshot:snapshot(9)});
  assert.match(row.textContent,/9\.00%/);
  assert.equal(requestCalls.filter(url=>/\/monitor\/(board|liquidity|sector-heatmap)/.test(url)).length,0);
  sockets[1].onerror();await new Promise(resolve=>setTimeout(resolve,1050));
  assert.equal(sockets.length,3,'a disconnected stream did not reconnect automatically');
  sockets[2].open();sockets[2].emit({snapshot:snapshot(10)});
  assert.match(row.textContent,/10\.00%/);
  console.log('PASS: independent push, stable DOM, history buffering, expiry and reconnect');
 } finally {page.stop();}
})().catch(error=>{console.error(error);process.exitCode=1;});
