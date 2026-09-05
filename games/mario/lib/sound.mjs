export class Sound {
  constructor() { this.context=null;this.muted=false;this.nextNote=0;this.note=0;this.wasPlaying=false; }
  unlock() { try { if(!this.context)this.context=new (window.AudioContext||window.webkitAudioContext)();if(this.context.state==='suspended')void this.context.resume().catch(()=>{}); } catch {} }
  tone(frequency,duration=.12,delay=0,type='square',volume=.035,end=0) {
    if(this.muted||!this.context||this.context.state!=='running')return;
    const c=this.context,t=c.currentTime+delay,o=c.createOscillator(),gain=c.createGain();
    o.type=type;o.frequency.setValueAtTime(frequency,t);if(end)o.frequency.exponentialRampToValueAtTime(Math.max(20,end),t+duration);
    gain.gain.setValueAtTime(volume,t);gain.gain.exponentialRampToValueAtTime(.0001,t+duration);
    o.connect(gain);gain.connect(c.destination);o.start(t);o.stop(t+duration);o.onended=()=>{o.disconnect();gain.disconnect();};
  }
  effect(name) {
    const sequence=(notes,step=.09)=>notes.forEach((n,i)=>this.tone(n,step,i*step));
    if(name==='jump')this.tone(190,.19,0,'square',.045,680);
    if(name==='coin')sequence([988,1319],.08);
    if(name==='stomp')this.tone(170,.13,0,'triangle',.14,50);
    if(name==='bump'||name==='break')this.tone(120,.09,0,'sawtooth',.04,40);
    if(name==='sprout')sequence([262,330,392,523],.06);
    if(name==='power'||name==='life')sequence([392,494,587,784,988],.08);
    if(name==='hurt')sequence([600,400,200,110],.07);
    if(name==='death')sequence([523,466,440,392,330,262,196],.16);
    if(name==='win')sequence([262,330,392,523,392,523,659,784,1047],.13);
  }
  music(playing) {
    if(!this.context)return;
    if(!playing){this.wasPlaying=false;this.nextNote=0;return;}
    if(!this.wasPlaying){this.note=0;this.nextNote=this.context.currentTime+.2;this.wasPlaying=true;}
    if(this.muted||this.context.currentTime<this.nextNote)return;
    // Short original chiptune, synthesized locally with no audio downloads.
    const melody=[659,0,784,659,523,0,587,659,0,523,392,0,440,494,587,0,659,784,880,784,659,0,523,587,659,0,494,392,523,0,0,0];
    const n=this.note++%melody.length;
    if(melody[n])this.tone(melody[n],.11,0,'square',.012);
    if(n%4===0)this.tone([131,165,175,196][Math.floor(n/8)],.2,0,'triangle',.035);
    this.nextNote=this.context.currentTime+.16;
  }
  dispose(){if(this.context)void this.context.close().catch(()=>{});}
}
