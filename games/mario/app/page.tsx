'use client';
import { useEffect, useRef, useState } from 'react';
import { ArrowLeft, ArrowRight, ArrowUp, Expand, Volume2, VolumeX, Pause, Play, RotateCcw, Gamepad2, Flag } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Game } from '@/lib/game.mjs';
import { Renderer } from '@/lib/renderer.mjs';
import { Sound } from '@/lib/sound.mjs';
const INITIAL = { mode: 'title', score: 0, coins: 0, lives: 3, time: 400, progress: 0 };
export default function Home() {
  const canvas = useRef<HTMLCanvasElement>(null);
  const screen = useRef<HTMLDivElement>(null);
  const game = useRef<Game | null>(null);
  const sound = useRef<Sound | null>(null);
  const [state, setState] = useState(INITIAL);
  const [muted, setMuted] = useState(false);
  const [notice, setNotice] = useState('');
  useEffect(() => {
    const audio = new Sound();
    const engine = new Game((event: string) => audio.effect(event));
    const renderer = new Renderer(canvas.current!);
    game.current = engine; sound.current = audio;
    let previous = 0, accumulator = 0, frame = 0, refresh = 0;
    const loop = (now: number) => {
      accumulator += Math.min((now - (previous || now)) / 1000, .08); previous = now;
      while (accumulator >= 1 / 60) { engine.tick(); accumulator -= 1 / 60; }
      renderer.draw(engine); audio.music(engine.mode === 'playing');
      if (++refresh % 6 === 0) setState(engine.snapshot());
      frame = requestAnimationFrame(loop);
    };
    frame = requestAnimationFrame(loop);
    const keys: Record<string, string> = { ArrowLeft: 'left', KeyA: 'left', ArrowRight: 'right', KeyD: 'right', ArrowUp: 'jump', KeyW: 'jump', Space: 'jump', KeyZ: 'jump', ShiftLeft: 'run', ShiftRight: 'run', KeyX: 'run' };
    const heldKeys = new Set<string>();
    const down = (e: KeyboardEvent) => {
      if ((e.target as HTMLElement)?.closest('button, input, select, textarea')) return;
      if (keys[e.code] || ['Enter', 'Escape', 'KeyP', 'KeyR', 'KeyM'].includes(e.code)) e.preventDefault();
      if (e.repeat) return;
      if (e.code === 'Enter' || (e.code === 'Space' && ['title', 'gameover', 'won'].includes(engine.mode))) { audio.unlock(); engine.start(); }
      else if (e.code === 'Escape' || e.code === 'KeyP') engine.pause();
      else if (e.code === 'KeyR') engine.restart();
      else if (e.code === 'KeyM') setMuted(v => !v);
      else if (keys[e.code]) { heldKeys.add(e.code); engine.input(keys[e.code], true); }
      setState(engine.snapshot());
    };
    const up = (e: KeyboardEvent) => { if (keys[e.code]) { heldKeys.delete(e.code); engine.input(keys[e.code], [...heldKeys].some(key => keys[key] === keys[e.code])); } };
    const blur = () => { heldKeys.clear(); engine.clearInput(); if (engine.mode === 'playing') engine.pause(); setState(engine.snapshot()); };
    const visibility = () => { if (document.hidden) blur(); };
    window.addEventListener('keydown', down); window.addEventListener('keyup', up); window.addEventListener('blur', blur); document.addEventListener('visibilitychange', visibility);
    const lifecycle = new AbortController();
    const context = (document as Document & { modelContext?: { registerTool: (tool: unknown, options: unknown) => unknown } }).modelContext;
    if (context?.registerTool) {
      const register = (tool: unknown) => { try { Promise.resolve(context.registerTool(tool, { signal: lifecycle.signal })).catch(() => {}); } catch {} };
      register({ name: 'read_game_state', description: 'Read Mario game status, score, lives and progress.', inputSchema: { type: 'object', properties: {}, additionalProperties: false }, annotations: { readOnlyHint: true }, execute: () => engine.snapshot() });
      register({ name: 'control_game_session', description: 'Start, pause, resume or restart the visible Mario game. Restart discards the current run.', inputSchema: { type: 'object', properties: { action: { type: 'string', enum: ['start', 'pause', 'resume', 'restart'] } }, required: ['action'], additionalProperties: false }, annotations: { readOnlyHint: false }, execute: (input: { action: string }) => {
        if (!input || !['start', 'pause', 'resume', 'restart'].includes(input.action) || Object.keys(input).length !== 1) throw new Error('Invalid game action');
        if (input.action === 'restart') engine.restart(); if (input.action === 'start') engine.start();
        if (input.action === 'pause' && engine.mode === 'playing') engine.pause(); if (input.action === 'resume' && engine.mode === 'paused') engine.pause();
        setState(engine.snapshot()); return engine.snapshot();
      } });
    }
    return () => { cancelAnimationFrame(frame); lifecycle.abort(); audio.dispose(); window.removeEventListener('keydown', down); window.removeEventListener('keyup', up); window.removeEventListener('blur', blur); document.removeEventListener('visibilitychange', visibility); game.current = null; sound.current = null; };
  }, []);
  useEffect(() => { if (sound.current) sound.current.muted = muted; }, [muted]);
  const action = (kind: 'start' | 'pause' | 'restart') => { sound.current?.unlock(); game.current?.[kind](); if (game.current) setState(game.current.snapshot()); (document.activeElement as HTMLElement)?.blur(); };
  const fullScreen = async () => { try { if (document.fullscreenElement) await document.exitFullscreen(); else if (screen.current?.requestFullscreen) await screen.current.requestFullscreen(); else setNotice('此浏览器暂不支持全屏，可横屏游玩。'); } catch { setNotice('暂时无法进入全屏，可继续在当前窗口游玩。'); } (document.activeElement as HTMLElement)?.blur(); };
  const touch = (name: string) => ({ onPointerDown: (e: React.PointerEvent<HTMLButtonElement>) => { e.preventDefault(); e.currentTarget.setPointerCapture(e.pointerId); sound.current?.unlock(); game.current?.input(name, true); }, onPointerUp: () => game.current?.input(name, false), onPointerCancel: () => game.current?.input(name, false), onLostPointerCapture: () => game.current?.input(name, false) });
  const overlay = ['title', 'paused', 'gameover', 'won'].includes(state.mode);
  return <main className="arcade">
    <header className="masthead"><a className="brand" href="/" aria-label="超级马里奥首页"><span className="brand-mark">M</span><span>SUPER MARIO<span className="brand-sub">THE BROWSER EDITION</span></span></a><div className="edition"><span className="live-dot" />经典模式 <span className="edition-divider">/</span> 1 PLAYER</div></header>
    <section className="game-section" aria-label="超级马里奥小游戏">
      <div className="game-heading"><div className="level-heading"><span className="world-tag">WORLD 1–1</span><h1>冒险，从这里开始。</h1></div><span className="pixel-label"><Gamepad2 size={17} /> READY, PLAYER ONE</span></div>
      <div className="console" ref={screen}>
        <div className="console-top"><span className="cartridge"><i /> SUPER MARIO BROS.</span><div className="console-actions">
          <Button className="icon-button" variant="ghost" size="icon" onClick={() => { sound.current?.unlock(); setMuted(v => !v); (document.activeElement as HTMLElement)?.blur(); }} aria-label={muted ? '打开声音' : '静音'} title="声音 · M" aria-pressed={!muted}>{muted ? <VolumeX /> : <Volume2 />}</Button>
          <Button className="icon-button" variant="ghost" size="icon" onClick={() => action('pause')} disabled={!['playing', 'paused'].includes(state.mode)} aria-label={state.mode === 'paused' ? '继续游戏' : '暂停游戏'} title="暂停 · P">{state.mode === 'paused' ? <Play /> : <Pause />}</Button>
          <Button className="icon-button" variant="ghost" size="icon" onClick={() => action('restart')} aria-label="重新开始" title="重新开始 · R"><RotateCcw /></Button>
          <span className="tool-divider" /><Button className="icon-button" variant="ghost" size="icon" onClick={fullScreen} aria-label="切换全屏" title="全屏"><Expand /></Button>
        </div></div>
        <div className="screen"><canvas ref={canvas} width="512" height="240" aria-label="马里奥游戏画面。左右方向键移动，空格跳跃，Shift 加速。">你的浏览器需要支持 Canvas 才能运行游戏。</canvas>
          {overlay && <div className={'game-overlay ' + (state.mode === 'title' ? 'title-overlay' : '')}>
            {state.mode === 'title' ? <><div className="title-plaque"><span className="title-eyebrow">LET’S-A GO!</span><div className="game-title">SUPER<br /><span>MARIO</span></div><span className="title-chinese">超级马里奥</span></div><Button className="start-button" onClick={() => action('start')}><Play fill="currentColor" size={16} />开始冒险 <span>ENTER ↵</span></Button><p className="start-hint">一顶红帽子，一场熟悉的冒险。</p></> : <div className="state-card"><span className="state-kicker">{state.mode === 'won' ? 'COURSE CLEAR!' : state.mode === 'paused' ? 'TAKE A BREAK' : 'TRY AGAIN'}</span><h2>{state.mode === 'won' ? '漂亮，成功通关！' : state.mode === 'paused' ? '冒险暂停中' : '冒险还未结束'}</h2><p>{state.mode === 'won' ? '得分 ' + state.score.toString().padStart(6, '0') + ' · 金币 ' + state.coins : state.mode === 'paused' ? '准备好了，就继续向前。' : '再来一次，城堡就在前方。'}</p><Button className="start-button" onClick={() => action(state.mode === 'paused' ? 'pause' : 'start')}><Play fill="currentColor" size={16} />{state.mode === 'paused' ? '继续冒险' : '再玩一次'}</Button></div>}
          </div>}
        </div>
        <div className="console-bottom"><span><span className={'live-dot ' + (state.mode === 'paused' ? 'amber' : '')} />{state.mode === 'title' ? '等待开始' : state.mode === 'paused' ? '已暂停' : state.mode === 'won' ? '关卡完成' : state.mode === 'gameover' ? '游戏结束' : '冒险进行中'}</span><span className="progress-track"><i style={{ width: state.progress + '%' }} /></span><Flag size={13} /><span className="console-note">8-BIT · BIG ADVENTURE</span></div>
        <div className="touch-controls" aria-label="触控操作"><div><button {...touch('left')} aria-label="向左移动"><ArrowLeft /></button><button {...touch('right')} aria-label="向右移动"><ArrowRight /></button></div><div><button {...touch('run')} className="run-touch" aria-label="按住加速">加速</button><button {...touch('jump')} className="jump-touch" aria-label="跳跃"><ArrowUp /><span>跳跃</span></button></div></div>
      </div>
      <div className="instructions"><div className="instruction-lead"><Gamepad2 /><span>操作指南</span></div><div className="control-pair"><span className="keys"><kbd>←</kbd><kbd>→</kbd><small>/</small><kbd>A</kbd><kbd>D</kbd></span><span>移动</span></div><div className="control-pair"><kbd className="space-key">SPACE</kbd><span>跳跃</span></div><div className="control-pair"><kbd>SHIFT</kbd><span>按住加速</span></div><div className="control-pair"><kbd>P</kbd><span>暂停</span></div></div>
      <div className="game-tip"><span className="tip-dot" />小贴士：长按跳得更高，踩在敌人头上就能击败它。<span>去碰一碰那块「?」砖。</span></div>
      {notice && <p role="status" className="notice">{notice}</p>}<p className="sr-only" aria-live="polite">{state.mode === 'won' ? '成功通关' : state.mode === 'gameover' ? '游戏结束，可以重新开始' : state.mode === 'paused' ? '游戏暂停' : ''}</p>
    </section><footer><span>A LITTLE NOSTALGIA. A LOT OF ADVENTURE.</span><span>非官方同人小游戏 <span className="footer-dot">·</span> 致敬经典</span></footer>
  </main>;
}
