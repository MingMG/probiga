export const TILE = 16, WIDTH = 512, HEIGHT = 240, GROUND = 208, WORLD_WIDTH = 3392, FLAG_X = 3168;
export const overlap = (a, b) => a.x < b.x + b.w && a.x + a.w > b.x && a.y < b.y + b.h && a.y + a.h > b.y;
const clamp = (n, low, high) => Math.max(low, Math.min(high, n));
const approach = (n, target, step) => n < target ? Math.min(target, n + step) : Math.max(target, n - step);

export function makeWorld() {
  const tiles = new Map(), pipes = [], coins = [], enemies = [];
  const put = (col, row, kind = 'brick', item = '') => tiles.set(col + ',' + row, { x: col * 16, y: row * 16, w: 16, h: 16, kind, item, bump: 0 });
  const gaps = new Set([69, 70, 86, 87, 88, 153, 154]);
  for (let col = 0; col < WORLD_WIDTH / 16; col++) if (!gaps.has(col)) for (let row = 13; row < 16; row++) put(col, row, 'ground');
  for (const [col, height] of [[28,2],[38,3],[46,4],[57,4],[163,2],[179,2]]) {
    pipes.push({ x: col * 16, y: GROUND - height * 16, w: 32, h: height * 16 });
    for (let x = col; x < col + 2; x++) for (let row = 13 - height; row < 13; row++) put(x, row, 'pipe');
  }
  for (const [col, row, item] of [[16,9,'coin'],[21,9,'mushroom'],[23,9,'coin'],[22,5,'coin'],[78,9,'mushroom'],[94,5,'coin'],[106,9,'coin'],[109,9,'mushroom'],[112,9,'coin'],[109,5,'coin'],[129,5,'coin'],[130,5,'coin'],[170,9,'coin']]) put(col, row, 'question', item);
  for (const [start,end,row] of [[20,20,9],[22,22,9],[24,24,9],[77,77,9],[79,79,9],[80,87,5],[91,93,5],[94,94,9],[100,101,9],[118,120,9],[127,128,5],[131,131,5],[129,130,9],[168,169,9],[171,171,9]]) for (let x = start; x <= end; x++) put(x,row);
  for (const [start,reverse] of [[134,false],[140,true],[148,false],[155,true]]) for (let step = 0; step < 4; step++) for (let h = 0; h < (reverse ? 4 - step : step + 1); h++) put(start + step, 12 - h, 'stair');
  for (let step = 0; step < 8; step++) for (let h = 0; h <= step; h++) put(181 + step, 12 - h, 'stair');
  for (let h = 0; h < 8; h++) put(189,12-h,'stair');
  for (const [x,y] of [[32,9],[33,8],[34,9],[72,10],[73,9],[74,10],[81,3],[83,3],[85,3],[96,9],[97,8],[98,9],[121,10],[122,9],[123,10],[145,10],[146,10],[175,9],[176,9]]) coins.push({x:x*16+4,y:y*16,w:8,h:12,got:false});
  for (const x of [22,40,50,52,80,82,97,99,114,116,124,128,170,174]) enemies.push({ x: x * 16, y: 192, w: 14, h: 16, vx: -.45, vy: 0, grounded: false, dead: 0, active: false });
  return { tiles, pipes, worldCoins: coins, enemies };
}

export class Game {
  /** @param {(event: string) => void} onEvent */
  constructor(onEvent = () => {}) {
    this.onEvent = onEvent;
    this.mode = 'title'; this.score = 0; this.coins = 0; this.lives = 3;
    this.frame = 0; this.camera = 0; this.timer = 400 * 60;
    this.keys = { left: false, right: false, jump: false, run: false };
    this.resetLevel();
  }
  resetLevel() {
    Object.assign(this, makeWorld());
    this.player = { x: 40, y: 192, w: 12, h: 16, vx: 0, vy: 0, grounded: true, facing: 1, big: false, invincible: 0 };
    this.items = []; this.particles = []; this.labels = [];
    this.camera = 0; this.timer = 400 * 60; this.coyote = 6; this.buffer = 0; this.deathTicks = 0; this.winTicks = 0; this.clearInput();
  }
  start() { if (['title','won','gameover'].includes(this.mode)) this.restart(); else if (this.mode === 'paused') this.pause(); }
  restart() { this.score = 0; this.coins = 0; this.lives = 3; this.resetLevel(); this.mode = 'playing'; }
  pause() { if (this.mode === 'playing') { this.mode = 'paused'; this.clearInput(); } else if (this.mode === 'paused') this.mode = 'playing'; }
  clearInput() { for (const key of Object.keys(this.keys)) this.keys[key] = false; this.buffer = 0; }
  input(key, pressed) {
    if (!(key in this.keys)) return;
    if (this.mode !== 'playing') { this.keys[key] = false; return; }
    if (key === 'jump' && pressed && !this.keys.jump && this.mode === 'playing') this.buffer = 7;
    if (key === 'jump' && !pressed && this.player.vy < -2.7) this.player.vy = -2.7;
    this.keys[key] = pressed;
  }
  snapshot() { return { mode: this.mode, score: this.score, coins: this.coins, lives: this.lives, time: Math.ceil(this.timer / 60), progress: Math.round(clamp(this.player.x / FLAG_X * 100, 0, 100)) }; }
  solids(body) {
    const out = [];
    for (let col = Math.floor(body.x / 16); col <= Math.floor((body.x + body.w - .001) / 16); col++) for (let row = Math.floor(body.y / 16); row <= Math.floor((body.y + body.h - .001) / 16); row++) {
      const t = this.tiles.get(col + ',' + row); if (t) out.push(t);
    }
    return out;
  }
  move(body, isPlayer = false) {
    let wall = false;
    body.x += body.vx;
    for (const tile of this.solids(body)) if (overlap(body, tile)) {
      body.x = body.vx > 0 ? tile.x - body.w : tile.x + tile.w; wall = true;
    }
    if (wall && isPlayer) body.vx = 0;
    body.y += body.vy; body.grounded = false;
    for (const tile of this.solids(body)) if (overlap(body, tile)) {
      if (body.vy > 0) { body.y = tile.y - body.h; body.grounded = true; }
      else if (body.vy < 0) { body.y = tile.y + tile.h; if (isPlayer) this.hitBlock(tile); }
      body.vy = 0;
    }
    return wall;
  }
  addScore(points, x, y) { this.score += points; this.labels.push({ text: String(points), x, y, life: 45 }); }
  collect(x, y) {
    this.coins++; this.addScore(200, x, y); this.onEvent('coin');
    if (this.coins % 100 === 0) { this.lives++; this.onEvent('life'); }
  }
  hitBlock(tile) {
    if (!['question','brick','used'].includes(tile.kind) || tile.bump > 0) return;
    tile.bump = 12;
    for (const enemy of this.enemies) if (!enemy.dead && Math.abs(enemy.y + enemy.h - tile.y) < 5 && enemy.x + enemy.w > tile.x && enemy.x < tile.x + 16) { enemy.dead = 30; this.addScore(100, enemy.x, enemy.y); }
    if (tile.kind === 'question') {
      tile.kind = 'used';
      if (tile.item === 'mushroom') {
        this.items.push({ x: tile.x, y: tile.y, w: 16, h: 16, vx: .8, vy: 0, grounded: false, emerge: 32, got: false }); this.onEvent('sprout');
      } else {
        this.collect(tile.x, tile.y - 16); this.particles.push({ kind: 'coin', x: tile.x + 4, y: tile.y - 12, vx: 0, vy: -3.5, life: 32 });
      }
    } else if (tile.kind === 'brick' && this.player.big) {
      this.tiles.delete(Math.floor(tile.x/16) + ',' + Math.floor(tile.y/16)); this.addScore(50, tile.x, tile.y); this.onEvent('break');
      for (let n = 0; n < 4; n++) this.particles.push({ kind: 'brick', x: tile.x + n % 2 * 8, y: tile.y + Math.floor(n / 2) * 8, vx: n % 2 ? 1.8 : -1.8, vy: -3 - Math.floor(n/2), life: 45 });
    } else this.onEvent('bump');
  }
  grow() {
    const p = this.player;
    if (p.big) return true;
    const expanded = { ...p, y: p.y - 16, h: 32 };
    if (this.solids(expanded).some(t => overlap(expanded,t))) return false;
    p.y -= 16; p.h = 32; p.big = true; return true;
  }
  hurt() {
    const p = this.player;
    if (this.mode !== 'playing' || p.invincible > 0) return;
    if (p.big) { p.big = false; p.y += 16; p.h = 16; p.invincible = 110; this.onEvent('hurt'); }
    else this.die();
  }
  die() {
    if (this.mode !== 'playing') return;
    this.mode = 'dying'; this.deathTicks = 0; this.lives--; this.player.vy = -5.8; this.player.vx = 0; this.clearInput(); this.onEvent('death');
  }
  win() {
    if (this.mode !== 'playing') return;
    this.mode = 'winning'; this.winTicks = 0; this.clearInput();
    this.addScore(this.player.y < 90 ? 5000 : this.player.y < 150 ? 2000 : 1000, FLAG_X, this.player.y);
    this.player.x = FLAG_X - 11; this.player.vx = 0; this.player.vy = 0; this.onEvent('win');
  }
  tick() {
    if (['paused','title','gameover','won'].includes(this.mode)) return;
    this.frame++;
    const p = this.player;
    for (const label of this.labels) { label.y -= .35; label.life--; }
    this.labels = this.labels.filter(l => l.life > 0);
    for (const fx of this.particles) { fx.x += fx.vx; fx.y += fx.vy; fx.vy += .16; fx.life--; }
    this.particles = this.particles.filter(fx => fx.life > 0);
    for (const tile of this.tiles.values()) if (tile.bump > 0) tile.bump--;
    if (this.mode === 'dying') {
      this.deathTicks++;
      if (this.deathTicks > 20) { p.y += p.vy; p.vy += .24; }
      if (this.deathTicks > 110) { if (this.lives > 0) { this.resetLevel(); this.mode = 'playing'; } else this.mode = 'gameover'; }
      return;
    }
    if (this.mode === 'winning') {
      this.winTicks++;
      if (p.y + p.h < GROUND) p.y = Math.min(GROUND - p.h, p.y + 2);
      else if (this.winTicks > 80) { p.facing = 1; p.x = Math.min(3270, p.x + 1.3); }
      if (this.winTicks > 160 && this.timer > 0) { const transfer = Math.min(60 * 20, this.timer); this.score += Math.ceil(transfer/60) * 50; this.timer -= transfer; }
      if (this.winTicks > 235 && this.timer <= 0) this.mode = 'won';
      this.camera = Math.max(this.camera, Math.min(WORLD_WIDTH - WIDTH, p.x - 205));
      return;
    }
    if (--this.timer <= 0) { this.timer = 0; this.die(); return; }
    if (p.invincible > 0) p.invincible--;
    const direction = Number(this.keys.right) - Number(this.keys.left);
    p.vx = approach(p.vx, direction * (this.keys.run ? 3 : 1.85), direction ? (p.grounded ? .14 : .09) : .1);
    if (direction) p.facing = direction;
    if (p.grounded) this.coyote = 6; else this.coyote = Math.max(0, this.coyote - 1);
    if (this.buffer > 0 && this.coyote > 0) { p.vy = this.keys.jump ? -7.2 : -3.6; p.grounded = false; this.coyote = 0; this.buffer = 0; this.onEvent('jump'); }
    this.buffer = Math.max(0, this.buffer - 1);
    const previousBottom = p.y + p.h;
    p.vy = Math.min(8, p.vy + .32);
    this.move(p, true);
    p.x = clamp(p.x, this.camera, WORLD_WIDTH - p.w);
    this.camera = Math.max(this.camera, Math.min(WORLD_WIDTH - WIDTH, p.x - 205));
    if (p.y > HEIGHT + 32) { this.die(); return; }
    for (const coin of this.worldCoins) if (!coin.got && overlap(p, coin)) { coin.got = true; this.collect(coin.x,coin.y); }
    for (const item of this.items) {
      if (item.got) continue;
      if (item.emerge > 0) { item.y -= .5; item.emerge--; continue; }
      item.vy = Math.min(6,item.vy+.24);
      if (this.move(item)) item.vx *= -1;
      if (item.y > 280) item.got = true;
      if (overlap(p,item) && this.grow()) { item.got = true; p.invincible = Math.max(p.invincible,35); this.addScore(1000,p.x,p.y); this.onEvent('power'); }
    }
    const descending = p.vy > 0;
    for (const enemy of this.enemies) {
      if (enemy.dead) { enemy.dead--; if (!enemy.dead) enemy.gone = true; continue; }
      if (enemy.gone) continue;
      if (!enemy.active && enemy.x < this.camera + WIDTH + 24) enemy.active = true;
      if (!enemy.active) continue;
      enemy.vy = Math.min(7,enemy.vy+.3);
      if (this.move(enemy)) enemy.vx *= -1;
      if (enemy.y > 270 || enemy.x < this.camera - 40) { enemy.gone = true; continue; }
      if (overlap(p,enemy)) {
        if (descending && previousBottom <= enemy.y + 5) { enemy.dead = 25; p.vy = this.keys.jump ? -5.1 : -3.8; p.grounded = false; this.addScore(100,enemy.x,enemy.y); this.onEvent('stomp'); }
        else this.hurt();
      }
      if (this.mode !== 'playing') break;
    }
    if (p.x + p.w >= FLAG_X && p.x < FLAG_X + 20) this.win();
  }
}
