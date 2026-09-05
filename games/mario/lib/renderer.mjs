import { sprites } from './sprites.js';
import { WIDTH, HEIGHT, GROUND, FLAG_X } from './game.mjs';

// Draw all game art at its native pixel resolution; CSS scales the framebuffer.
export class Renderer {
  constructor(canvas) {
    this.ctx = canvas.getContext('2d');
    this.atlas = {};
    for (const [name, sprite] of Object.entries(sprites)) {
      const tile = document.createElement('canvas');
      tile.width = sprite.pixels[0].length; tile.height = sprite.pixels.length;
      const c = tile.getContext('2d');
      sprite.pixels.forEach((row,y) => [...row].forEach((pixel,x) => { if (pixel !== '.') { c.fillStyle = sprite.palette[pixel]; c.fillRect(x,y,1,1); } }));
      this.atlas[name] = tile;
    }
  }
  sprite(name,x,y,scale=1,flip=false) {
    const tile = this.atlas[name]; if (!tile) return;
    const c = this.ctx; c.save(); c.translate(Math.round(x)+(flip?tile.width*scale:0),Math.round(y)); c.scale(flip?-scale:scale,scale); c.drawImage(tile,0,0); c.restore();
  }
  text(text,x,y,size=8,color='#fff8e7',align='left') {
    const c=this.ctx;c.font='bold '+size+'px "Courier New", monospace'; c.textAlign=align;c.textBaseline='top';c.fillStyle='#344899';c.fillText(text,x+1,y+1);c.fillStyle=color;c.fillText(text,x,y);
  }
  pipe(pipe,camera) {
    const c=this.ctx,x=Math.round(pipe.x-camera),y=pipe.y,h=pipe.h;
    c.fillStyle='#183a0a'; c.fillRect(x+2,y+8,28,h-8);
    c.fillStyle='#6bb520'; c.fillRect(x+4,y+8,24,h-8);
    c.fillStyle='#a9e84e'; c.fillRect(x+6,y+8,5,h-8);
    c.fillStyle='#2f6c0b'; c.fillRect(x+22,y+8,5,h-8);
    c.fillStyle='#407f13'; c.fillRect(x+13,y+8,2,h-8);
    c.fillStyle='#183a0a'; c.fillRect(x,y,32,11);
    c.fillStyle='#a2df42'; c.fillRect(x+1,y+1,30,8);
    c.fillStyle='#5fa816'; c.fillRect(x+10,y+2,20,6);
    c.fillStyle='#cff478'; c.fillRect(x+3,y+2,5,6);
    c.fillStyle='#295d0c'; c.fillRect(x+26,y+2,4,6);
  }
  castle(x) {
    const c=this.ctx;
    for(let row=0;row<4;row++)for(let col=0;col<5;col++)this.sprite('brick',x+col*16,GROUND-64+row*16);
    for(let col=0;col<5;col+=2)this.sprite('brick',x+col*16,GROUND-80);
    c.fillStyle='#4c2118';c.fillRect(x+30,GROUND-26,20,26);c.fillRect(x+34,GROUND-30,12,4);
    for(const xx of [12,58]){c.fillRect(x+xx,GROUND-53,10,15);c.fillStyle='#140e16';c.fillRect(x+xx+2,GROUND-52,6,12);c.fillStyle='#4c2118';}
  }
  draw(g) {
    const c=this.ctx,cam=Math.round(g.camera),p=g.player;
    c.imageSmoothingEnabled=false;c.fillStyle='#6385f7';c.fillRect(0,0,WIDTH,HEIGHT);
    // Layered repeating landscape sits behind the collision geometry.
    for(let i=-1;i<10;i++) {
      const base=i*384-Math.round(cam*.36);
      this.sprite('cloud',base+125,44,1.4);this.sprite('cloud',base+289,70,1);
      this.sprite('hill',base+5,GROUND-48,3);this.sprite('hill',base+181,GROUND-24,1.5);
      this.sprite('bush',base+96,GROUND-16);this.sprite('bush',base+273,GROUND-16,1);
    }
    for(const t of g.tiles.values()) {
      if(t.x<cam-16||t.x>cam+WIDTH||t.kind==='pipe')continue;
      const y=t.y-(t.bump?Math.sin(t.bump/12*Math.PI)*5:0);
      if(t.kind==='stair') {
        c.fillStyle='#b75d2d';c.fillRect(t.x-cam,y,16,16);c.fillStyle='#ffba75';c.fillRect(t.x-cam,y,16,2);c.fillRect(t.x-cam,y,2,16);c.fillStyle='#663219';c.fillRect(t.x-cam+14,y+2,2,14);c.fillRect(t.x-cam+2,y+14,14,2);c.fillRect(t.x-cam+5,y+5,6,6);
      } else {this.sprite(t.kind,t.x-cam,y);if(t.kind==='question'&&Math.floor(g.frame/18)%3===1){c.fillStyle='#ffe29a33';c.fillRect(t.x-cam+1,y+1,14,14);}}
    }
    for(const pipe of g.pipes)if(pipe.x>cam-32&&pipe.x<cam+WIDTH)this.pipe(pipe,cam);
    const flagX=FLAG_X-cam;
    if(flagX<WIDTH+100&&flagX>-100){
      c.fillStyle='#fff8da';c.fillRect(flagX,40,2,168);c.fillStyle='#3c911a';c.fillRect(flagX-2,36,6,6);
      let flagY=43;if(['winning','won'].includes(g.mode))flagY=Math.min(180,43+g.winTicks*2);
      c.fillStyle='#fff9eb';for(let n=0;n<15;n++)c.fillRect(flagX-16+n,flagY+n,16-n,1);
      c.fillStyle='#268323';c.fillRect(flagX-7,flagY+3,3,4);this.castle(3232-cam);
    }
    for(const coin of g.worldCoins)if(!coin.got&&coin.x>cam-16&&coin.x<cam+WIDTH)this.sprite('coin',coin.x-cam-4,coin.y);
    for(const item of g.items)if(!item.got)this.sprite('mushroom',item.x-cam,item.y);
    // Occlude the portion of a power-up still inside its question block.
    for(const item of g.items)if(item.emerge>0&&!item.got){const blockY=item.y-(32-item.emerge)*-.5;this.sprite('used',item.x-cam,blockY);}
    for(const e of g.enemies)if(!e.gone&&e.x>cam-20&&e.x<cam+WIDTH){this.sprite(e.dead?'goombaFlat':Math.floor(g.frame/12)%2?'goomba1':'goomba2',e.x-cam-1,e.y);}
    if(!p.invincible||Math.floor(g.frame/5)%2===0){
      let pose='Idle';if(!p.grounded||g.mode==='dying')pose='Jump';else if(Math.abs(p.vx)>.2||(g.mode==='winning'&&g.winTicks>80))pose=Math.floor(g.frame/6)%2?'Run1':'Run2';
      if(!(g.mode==='won'||(g.mode==='winning'&&p.x>=3267)))this.sprite((p.big?'big':'small')+pose,p.x-cam-2,p.y,1,p.facing<0);
    }
    for(const fx of g.particles){if(fx.kind==='coin')this.sprite('coin',fx.x-cam,fx.y);else{c.fillStyle='#b34b21';c.fillRect(Math.round(fx.x-cam),Math.round(fx.y),6,6);c.fillStyle='#fac698';c.fillRect(Math.round(fx.x-cam),Math.round(fx.y),6,1);}}
    for(const label of g.labels)this.text(label.text,Math.round(label.x-cam),Math.round(label.y),7);
    this.text('MARIO',22,13);this.text(String(g.score).padStart(6,'0'),22,24);
    this.sprite('coin',159,16,.7);this.text('x'+String(g.coins).padStart(2,'0'),172,24);
    this.text('WORLD',268,13);this.text('1-1',273,24);
    this.text('TIME',384,13);this.text(String(Math.ceil(g.timer/60)).padStart(3,'0'),389,24);
    this.sprite('smallIdle',455,17,.8);this.text('x'+g.lives,472,24);
    if(g.mode==='dying'&&g.deathTicks>60&&g.lives>0){c.fillStyle='#101319';c.fillRect(0,0,WIDTH,HEIGHT);this.text('WORLD 1-1',256,85,14,'#fff8e7','center');this.sprite('smallIdle',230,117);this.text('x '+g.lives,255,121,11);}
  }
}
