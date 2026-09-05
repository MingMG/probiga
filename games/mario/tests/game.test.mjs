import test from 'node:test';
import assert from 'node:assert/strict';
import { Game, GROUND, FLAG_X, overlap } from '../lib/game.mjs';

const frames = (g,n) => { for(let i=0;i<n;i++) g.tick(); };
const play = () => { const g=new Game();g.start();return g; };
const position = (g,x,y=192) => { Object.assign(g.player,{x,y,vx:0,vy:0,grounded:true});g.camera=Math.max(0,x-205); };

test('session starts with numeric score, coins, timer and three lives',()=>{
  const g=play();assert.equal(g.mode,'playing');assert.equal(g.coins,0);assert.ok(g.worldCoins.length>0);assert.equal(g.lives,3);
  frames(g,60);assert.equal(g.snapshot().time,399);assert.equal(g.player.y,192);
});
test('acceleration, inertia, reversal and running are distinct',()=>{
  const g=play();g.input('right',true);frames(g,20);assert.equal(g.player.vx,1.85);
  g.input('right',false);g.tick();assert.ok(g.player.vx>0&&g.player.vx<1.85);frames(g,20);assert.equal(g.player.vx,0);
  g.input('right',true);g.input('run',true);frames(g,30);assert.equal(g.player.vx,3);
  g.input('right',false);g.input('left',true);g.tick();assert.ok(g.player.vx>0);frames(g,50);assert.ok(g.player.vx<0);
});
test('short and full jumps differ; holding jump does not auto-bounce',()=>{
  const jump = release => {const g=play();g.input('jump',true);let top=192;for(let i=0;i<70;i++){if(i===release)g.input('jump',false);g.tick();top=Math.min(top,g.player.y);}assert.equal(g.player.y,192);return 192-top;};
  const short=jump(5),full=jump(65);assert.ok(full>70&&full<80);assert.ok(short<full*.7);
  const g=play();g.input('jump',true);frames(g,100);assert.equal(g.player.y,192);assert.equal(g.player.vy,0);
});
test('pipe wall stops running without penetration',()=>{
  const g=play();g.enemies=[];position(g,414);g.input('right',true);g.input('run',true);frames(g,80);
  assert.equal(g.player.x,436);assert.equal(g.player.y,192);assert.ok(!g.solids(g.player).some(t=>overlap(t,g.player)));
});
test('a running jump clears the mandatory four-tile-tall pipe',()=>{
  const g=play();g.enemies=[];position(g,57*16-66);g.player.vx=3;g.input('right',true);g.input('run',true);g.input('jump',true);frames(g,52);
  assert.ok(g.player.x>59*16);assert.equal(g.mode,'playing');assert.equal(g.player.y,192);
});
test('releasing jump during pause preserves the frozen velocity',()=>{
  const g=play();g.input('jump',true);g.tick();g.pause();const vy=g.player.vy;g.input('jump',false);assert.equal(g.player.vy,vy);
});
test('a tapped buffered jump remains shorter than a held buffered jump',()=>{
  const buffered=held=>{const g=play();position(g,100,187);g.player.grounded=false;g.player.vy=3;g.coyote=0;g.input('jump',true);if(!held)g.input('jump',false);let top=192;frames(g,3);for(let i=0;i<50;i++){g.tick();top=Math.min(top,g.player.y);}return 192-top;};
  assert.ok(buffered(false)<buffered(true)*.5);
});
test('question block gives one coin through an actual head collision',()=>{
  const g=play();position(g,258);g.input('jump',true);frames(g,25);assert.equal(g.coins,1);assert.equal(g.score,200);assert.equal(g.tiles.get('16,9').kind,'used');
  g.input('jump',false);frames(g,40);g.input('jump',true);frames(g,30);assert.equal(g.coins,1);
});
test('mushroom emerges, moves, grows Mario while preserving his feet',()=>{
  const g=play();g.enemies=[];g.hitBlock(g.tiles.get('21,9'));frames(g,34);assert.equal(g.items.length,1);assert.equal(g.items[0].emerge,0);
  position(g,100);g.items=[{x:100,y:192,w:16,h:16,vx:0,vy:0,grounded:true,emerge:0,got:false}];g.tick();
  assert.ok(g.player.big);assert.equal(g.player.h,32);assert.equal(g.player.y+g.player.h,GROUND);assert.equal(g.score,1000);
});
test('growth waits when a low ceiling would trap Mario',()=>{
  const g=play();position(g,100);g.tiles.set('6,11',{x:96,y:176,w:16,h:16,kind:'brick',bump:0});
  assert.equal(g.grow(),false);assert.equal(g.player.h,16);assert.equal(g.player.y,192);
});
test('big Mario breaks bricks; damage shrinks once then grants invulnerability',()=>{
  const g=play();g.grow();g.hitBlock(g.tiles.get('20,9'));assert.ok(!g.tiles.has('20,9'));assert.equal(g.score,50);
  g.hurt();assert.equal(g.player.big,false);assert.equal(g.player.y+g.player.h,GROUND);g.hurt();assert.equal(g.lives,3);
  g.player.invincible=0;g.hurt();g.hurt();assert.equal(g.lives,2);assert.equal(g.mode,'dying');
});
test('descending on a goomba stomps; a side collision costs one life',()=>{
  const g=play();position(g,100,173);g.player.grounded=false;g.player.vy=4;g.enemies=[{x:99,y:192,w:14,h:16,vx:0,vy:0,grounded:true,dead:0,active:true}];g.tick();
  assert.ok(g.enemies[0].dead>0);assert.ok(g.player.vy<0);assert.equal(g.score,100);assert.equal(g.lives,3);
  const side=play();position(side,100);side.enemies=[{x:109,y:192,w:14,h:16,vx:0,vy:0,grounded:true,dead:0,active:true}];side.tick();assert.equal(side.lives,2);assert.equal(side.mode,'dying');
});
test('pit deaths reset safely and the third death ends the game',()=>{
  const g=play();for(let life=2;life>=0;life--){position(g,69*16+2,274);g.tick();assert.equal(g.lives,life);assert.equal(g.mode,'dying');frames(g,111);if(life){assert.equal(g.mode,'playing');assert.equal(g.player.x,40);assert.equal(g.player.y,192);assert.equal(g.camera,0);}}
  assert.equal(g.mode,'gameover');g.start();assert.equal(g.lives,3);assert.equal(g.score,0);
});
test('stomping two adjacent enemies in one frame does not turn into side damage',()=>{
  const g=play();position(g,850,174);g.player.grounded=false;g.player.vy=2;g.enemies=[849,853].map(x=>({x,y:192,w:14,h:16,vx:0,vy:0,grounded:true,dead:0,active:true}));g.tick();
  assert.equal(g.mode,'playing');assert.equal(g.lives,3);assert.equal(g.score,200);assert.ok(g.enemies.every(e=>e.dead>0));
});
test('pause freezes simulation and restart clears held controls',()=>{
  const g=play();g.input('right',true);frames(g,10);g.pause();const snapshot=JSON.stringify({p:g.player,time:g.timer,enemies:g.enemies,frame:g.frame});frames(g,500);assert.equal(JSON.stringify({p:g.player,time:g.timer,enemies:g.enemies,frame:g.frame}),snapshot);
  g.pause();assert.equal(g.mode,'playing');assert.equal(g.keys.right,false);g.input('jump',true);g.restart();assert.equal(g.keys.jump,false);
});
test('timer expires once, and touching the flag settles into a replayable win',()=>{
  const timed=play();timed.timer=1;timed.tick();assert.equal(timed.mode,'dying');assert.equal(timed.lives,2);
  const g=play();position(g,FLAG_X-11,80);g.player.grounded=false;g.tick();assert.equal(g.mode,'winning');assert.equal(g.score,5000);g.hurt();assert.equal(g.lives,3);frames(g,250);assert.equal(g.mode,'won');assert.equal(g.timer,0);assert.ok(g.score>=24000);g.start();assert.equal(g.mode,'playing');assert.equal(g.player.x,40);
});
test('the complete terrain is traversable from spawn to castle with normal physics',()=>{
  const g=play();g.enemies=[];g.input('right',true);g.input('run',true);
  // Terrain-only route: enemies are independently covered by collision tests.
  const route='00000000000000000000000000000000101000000001010000000100000000000000100100000101000000010000000000000000000000011000000000000000000000000000000000000100100000000100100000000010100000000000000100001000000000000000101000000000000000000101000010010010010000000001120';
  for(const action of route){
    if(action==='1'){g.input('jump',false);g.input('jump',true);}else if(action==='2'||g.player.vy>=0)g.input('jump',false);
    for(let i=0;i<4&&g.mode==='playing';i++)g.tick();
  }
  assert.equal(g.mode,'winning');assert.equal(g.lives,3);frames(g,250);assert.equal(g.mode,'won');
});
