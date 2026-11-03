"""Static, file://-friendly dashboard snapshot writer.

The production process injects its already-connected MT5 feed, strategy, risk,
and terminal objects. This module opens no HTTP socket and does not send orders.
It atomically writes dashboard_state.json and a self-contained HTML snapshot.
"""
from __future__ import annotations

import json
import logging
import math
import os
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


HTML_TEMPLATE = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="2"><title>MT5 Forward Test Dashboard</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#0b1220;color:#eaf1fb;font:14px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}header{padding:18px 24px;border-bottom:1px solid #24344e;font-weight:700}main{display:grid;grid-template-columns:minmax(460px,1fr) 330px;gap:16px;padding:16px;min-height:calc(100vh - 60px)}section{background:#0d1627;border:1px solid #24344e;border-radius:12px;padding:16px}.panel{display:grid;grid-template-columns:1fr 1fr;gap:12px;align-content:start}.label{font-size:10px;color:#8194b0;font-weight:700;letter-spacing:.08em;margin-top:7px}.value{font-size:16px;font-weight:700;margin-top:4px;overflow-wrap:anywhere}.green{color:#31d39a}.amber{color:#ffcf70}.red{color:#ff647c}.wide{grid-column:1/-1}.sub{font-size:11px;color:#9fb0c8;margin-top:4px}canvas{width:100%;height:100%;min-height:480px}.note{color:#8194b0;font-size:10px}@media(max-width:900px){main{grid-template-columns:1fr}.panel{grid-template-columns:1fr 1fr}canvas{height:55vh}}
</style></head><body><header>EURUSD · MT5 DEMO FORWARD TEST · FILE SNAPSHOT</header><main><section><canvas id="chart"></canvas><div class="note">Snapshot time: <span id="stamp"></span> · refreshes every 2 seconds when the file is rewritten.</div></section><section class="panel">
<div class="wide"><div class="label">SYSTEM MODE</div><div class="value" id="mode">—</div><div class="sub" id="feed">—</div></div>
<div><div class="label">ACCOUNT EQUITY</div><div class="value" id="equity">—</div></div><div><div class="label">FLOATING P&amp;L</div><div class="value" id="pnl">—</div></div>
<div><div class="label">OPEN POSITION</div><div class="value" id="position">Flat</div></div><div><div class="label">RISK GATE</div><div class="value" id="risk">—</div></div>
<div class="wide"><div class="label">NEWS GATE</div><div class="value" id="news">—</div></div>
<div><div class="label">ADX(14)</div><div class="value" id="adx">—</div></div><div><div class="label">FAST / SLOW MA</div><div class="value" id="mas">—</div></div>
<div><div class="label">LAST PRICE · BID / ASK</div><div class="value" id="price">—</div></div><div><div class="label">UTC SESSION</div><div class="value" id="session">—</div></div>
<div class="wide"><div class="label">LAST SIGNAL / ORDER</div><div class="value" id="order">—</div></div>
<div class="wide sub">Read-only telemetry. Orders are routed by the production strategy through its risk gate and OMS.</div>
</section></main>
<script id="dashboard-data" type="application/json">__SNAPSHOT_JSON__</script>
<script>
const s=JSON.parse(document.getElementById('dashboard-data').textContent),canvas=document.getElementById('chart'),ctx=canvas.getContext('2d');
function sma(a,n){return a.map((_,i)=>i+1<n?null:a.slice(i+1-n,i+1).reduce((p,v)=>p+v,0)/n)}
function draw(){let d=devicePixelRatio||1,r=canvas.getBoundingClientRect();canvas.width=r.width*d;canvas.height=r.height*d;ctx.setTransform(d,0,0,d,0,0);let w=r.width,h=r.height;ctx.fillStyle='#0b1220';ctx.fillRect(0,0,w,h);ctx.fillStyle='#d8e5f7';ctx.font='bold 12px sans-serif';ctx.fillText('LIVE M15 CANDLES · MOVING AVERAGES',16,22);let a=(s.candles||[]).slice(-120);if(!a.length){ctx.fillStyle='#9fb0c8';ctx.fillText('Waiting for completed M15 candles…',20,55);return}let L=72,R=w-18,T=42,B=h-36,cl=a.map(v=>v.close),lo=Math.min(...a.map(v=>v.low)),hi=Math.max(...a.map(v=>v.high)),pad=Math.max((hi-lo)*.15,.0001);lo-=pad;hi+=pad;let X=i=>L+(i+.5)*(R-L)/a.length,Y=v=>B-(v-lo)/(hi-lo)*(B-T);ctx.strokeStyle='#26344a';ctx.font='10px monospace';for(let j=0;j<5;j++){let f=j/4,y=T+(B-T)*f;ctx.beginPath();ctx.moveTo(L,y);ctx.lineTo(R,y);ctx.stroke();ctx.fillStyle='#9fb0c8';ctx.fillText((hi-(hi-lo)*f).toFixed(5),4,y+3)}for(let i=0;i<a.length;i++){let b=a[i],xx=X(i),col=b.close>=b.open?'#31d39a':'#ff647c';ctx.strokeStyle=col;ctx.fillStyle=col;ctx.beginPath();ctx.moveTo(xx,Y(b.high));ctx.lineTo(xx,Y(b.low));ctx.stroke();let top=Y(Math.max(b.open,b.close)),bot=Y(Math.min(b.open,b.close));ctx.fillRect(xx-2.5,top,5,Math.max(2,bot-top))}for(let [ar,col] of [[sma(cl,s.fast_period),'#21a8ff'],[sma(cl,s.slow_period),'#ff9f43']]){ctx.strokeStyle=col;ctx.lineWidth=2;ctx.beginPath();let begun=false;ar.forEach((v,i)=>{if(v==null)return;if(!begun){ctx.moveTo(X(i),Y(v));begun=true}else ctx.lineTo(X(i),Y(v))});ctx.stroke()}ctx.fillStyle='#9fb0c8';ctx.font='10px monospace';ctx.fillText(a[0].time_label,L,B+18);ctx.textAlign='right';ctx.fillText(a[a.length-1].time_label,R,B+18);ctx.textAlign='left'}
function set(id,value,cls){let el=document.getElementById(id);el.textContent=value;if(cls)el.className='value '+cls}
draw();document.getElementById('stamp').textContent=s.updated_at||'—';set('mode',s.mode,s.trading_enabled?'amber':'green');set('feed',s.feed_connected?'MT5 connected · '+s.symbol:'MT5 feed reconnecting','sub '+(s.feed_connected?'green':'red'));set('equity',s.equity==null?'—':'£'+Number(s.equity).toLocaleString('en-GB',{minimumFractionDigits:2,maximumFractionDigits:2}));set('pnl',s.floating_pnl==null?'—':(s.floating_pnl>=0?'+':'')+'£'+Number(s.floating_pnl).toFixed(2),s.floating_pnl>=0?'green':'red');set('position',s.position?`${s.position.side.toUpperCase()} ${s.position.volume} lot · SL ${s.position.sl||'—'} · TP ${s.position.tp||'—'}`:'Flat');set('risk',s.risk_ready?'CLEAR':'BLOCKED / CHECK',s.risk_ready?'green':'red');set('news',s.news_status,s.news_ready?'green':'amber');set('adx',s.adx==null?'Warming up':Number(s.adx).toFixed(1));set('mas',s.fast_period+' / '+s.slow_period);set('price',s.bid==null?'—':Number(s.last_price).toFixed(5)+' · '+Number(s.bid).toFixed(5)+' / '+Number(s.ask).toFixed(5));set('session',s.session_open?'OPEN':'LOCKED',s.session_open?'green':'amber');set('order',s.last_order||'No signal yet');
</script></body></html>'''


class StaticDashboardWriter:
    """Write atomic JSON and self-contained HTML snapshots from live MT5 state."""

    def __init__(self, feed: Any, engine: Any, risk: Any, mt5: Any, *,
                 trading_enabled: bool, symbol: str,
                 output_dir: str | Path | None = None,
                 logger: Optional[logging.Logger] = None) -> None:
        self.feed, self.engine, self.risk, self.mt5 = feed, engine, risk, mt5
        self.trading_enabled, self.symbol = bool(trading_enabled), symbol
        # Keep snapshots beside this module so the location is portable across
        # Windows service accounts and does not depend on drive permissions.
        self.output_dir = Path(output_dir).expanduser() if output_dir else Path(os.path.dirname(__file__)).resolve()
        self.logger = logger or logging.getLogger("mt5.dashboard")
        self.json_path = self.output_dir / "dashboard_state.json"
        self.html_path = self.output_dir / "dashboard_snapshot.html"

    def snapshot(self) -> dict[str, Any]:
        account = self.mt5.account_info()
        tick = self.mt5.symbol_info_tick(self.symbol)
        positions = self.mt5.positions_get(symbol=self.symbol) or []
        owned = [p for p in positions if int(getattr(p, "magic", -1)) == int(self.engine.magic)]
        position = None
        if owned:
            p = owned[-1]
            is_buy = int(getattr(p, "type", -1)) == int(self.mt5.POSITION_TYPE_BUY)
            position = {
                "ticket": int(p.ticket), "side": "buy" if is_buy else "sell",
                "volume": float(p.volume), "entry": float(getattr(p, "price_open", 0.0)),
                "sl": float(getattr(p, "sl", 0.0) or 0.0),
                "tp": float(getattr(p, "tp", 0.0) or 0.0),
                "profit": float(getattr(p, "profit", 0.0)),
            }
        candles = self.engine.closed_candles[-240:]
        current = getattr(self.engine, "_candle", None)
        if current is not None:
            try:
                candles = [*candles, current.as_dict(self.symbol)]
            except Exception:
                pass
        candles_json = []
        for candle in candles:
            stamp = candle.get("time")
            if isinstance(stamp, datetime):
                stamp = stamp.astimezone(timezone.utc) if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
                time_value = stamp.isoformat()
                label = stamp.strftime("%d %b %H:%M UTC")
            else:
                time_value, label = str(stamp), str(stamp)
            candles_json.append({
                "time": time_value, "time_label": label,
                "open": float(candle["open"]), "high": float(candle["high"]),
                "low": float(candle["low"]), "close": float(candle["close"]),
            })
        bid = float(getattr(tick, "bid", 0.0)) if tick else None
        ask = float(getattr(tick, "ask", 0.0)) if tick else None
        last_price = (bid + ask) / 2 if bid is not None and ask is not None else None
        risk = self.risk.snapshot()
        news_ready = bool(risk.news_calendar_available)
        news_status = ("BLACKOUT · " + str(risk.news_reason) if risk.news_blackout
                       else "CLEAR" if news_ready else "UNAVAILABLE · FAIL CLOSED")
        order_state = self.engine.last_order_state
        last_order = None
        if order_state is not None:
            last_order = (f"{order_state.status.upper()} {order_state.side.upper()} "
                          f"ticket={order_state.ticket or '—'} volume={order_state.filled_volume:g}")
        if self.engine.last_signal is not None:
            signal = self.engine.last_signal
            last_order = f"{signal.side.upper()} signal at {signal.close:.5f}" + (f" · {last_order}" if last_order else "")
        session_open = False
        now = datetime.now(timezone.utc).time().replace(tzinfo=None)
        session_open = self.engine.session_start_utc <= now < self.engine.session_end_utc
        return {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "mode": "DEMO FORWARD TEST · ORDER ROUTING ENABLED" if self.trading_enabled else "DEMO DRY RUN · ORDERS DISABLED",
            "trading_enabled": self.trading_enabled, "symbol": self.symbol,
            "feed_connected": bool(self.feed.connected), "last_price": last_price,
            "bid": bid, "ask": ask,
            "equity": float(account.equity) if account else None,
            "balance": float(account.balance) if account else None,
            "floating_pnl": sum(float(getattr(p, "profit", 0.0)) for p in owned) if account else None,
            "position": position,
            "risk_ready": bool(risk.terminal_connected and not risk.breached and news_ready and not risk.news_blackout),
            "risk_reasons": list(risk.reasons), "news_ready": news_ready,
            "news_status": news_status, "adx": self.engine.last_adx,
            "fast_period": self.engine.fast_period, "slow_period": self.engine.slow_period,
            "session_open": session_open, "candles": candles_json,
            "last_order": last_order,
        }

    @staticmethod
    def _atomic_write(path: Path, data: str) -> None:
        temp = path.with_name(path.name + ".tmp")
        # Flush Python and OS buffers before replacing the visible snapshot.
        # This prevents readers from observing a partially written file.
        with temp.open("w", encoding="utf-8", newline="") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)

    def write_snapshot(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        state = self.snapshot()
        json_text = json.dumps(state, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        embedded = json_text.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
        html_text = HTML_TEMPLATE.replace("__SNAPSHOT_JSON__", embedded)
        self._atomic_write(self.json_path, json_text + "\n")
        self._atomic_write(self.html_path, html_text)
        return state


def main() -> int:
    """Open the latest snapshot locally; production writes it on Windows."""
    path = Path(os.path.dirname(__file__)).resolve() / "dashboard_snapshot.html"
    if not path.is_file():
        print(f"No dashboard snapshot yet: {path}\nStart the production runner on the MT5 Windows host first.")
        return 1
    print(f"Opening static dashboard: {path}")
    webbrowser.open(path.as_uri(), new=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
