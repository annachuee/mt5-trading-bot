"""Visual dry-run cockpit for the existing EURUSD strategy pipeline.

This is an offline GUI: it reuses the project's strategy, ATR sizing, risk, and
OMS path with an in-memory MT5 double. The only network traffic is explicitly
configured Telegram notifications for simulated crossover fills.
"""
from __future__ import annotations

import argparse
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
from dotenv import load_dotenv

from mock_engine_smoke import MockMT5, notify_telegram
from mt5_oms import MT5OMS
from mt5_pretrade_risk import MT5PreTradeRisk
from mt5_strategy_engine import MT5StrategyEngine


class DashboardMT5(MockMT5):
    """Mock terminal that records simulated request fields for the panel."""

    def __init__(self) -> None:
        super().__init__()
        self.last_request: dict | None = None
        self._simulation_lock = threading.RLock()
        self.simulation_balance = 10000.0
        self.mock_positions: dict[int, dict] = {}
        self.last_closed_trade: dict | None = None

    def update_quote(self, event: dict) -> None:
        self._tick = SimpleNamespace(bid=float(event["bid"]), ask=float(event["ask"]))

    def floating_pnl(self, bid: float | None = None, ask: float | None = None) -> float:
        bid = float(self._tick.bid if bid is None else bid)
        ask = float(self._tick.ask if ask is None else ask)
        with self._simulation_lock:
            total = 0.0
            for position in self.mock_positions.values():
                exit_price = bid if position["side"] == "buy" else ask
                direction = 1.0 if position["side"] == "buy" else -1.0
                total += (exit_price - position["entry"]) * direction * 100000 * position["volume"]
            return total

    def account_info(self):
        equity = self.simulation_balance + self.floating_pnl()
        return SimpleNamespace(equity=equity, balance=self.simulation_balance)

    def sync_mock_position(self, state, request: dict) -> None:
        if state is None or state.status != "filled" or state.ticket is None:
            return
        with self._simulation_lock:
            if state.ticket not in self.mock_positions:
                self.mock_positions[state.ticket] = {
                    "ticket": int(state.ticket), "side": state.side,
                    "entry": float(state.average_price),
                    "volume": float(state.filled_volume),
                    "sl": float(request["sl"]), "tp": float(request["tp"]),
                }

    def evaluate_mock_exits(self, bid: float, ask: float) -> list[dict]:
        closed = []
        with self._simulation_lock:
            for ticket, position in list(self.mock_positions.items()):
                side = position["side"]
                exit_price = float(bid if side == "buy" else ask)
                hit_sl = exit_price <= position["sl"] if side == "buy" else exit_price >= position["sl"]
                hit_tp = exit_price >= position["tp"] if side == "buy" else exit_price <= position["tp"]
                if not (hit_sl or hit_tp):
                    continue
                reason = "stop_loss" if hit_sl else "take_profit"
                exit_price = position["sl"] if hit_sl else position["tp"]
                direction = 1.0 if side == "buy" else -1.0
                pnl = (exit_price - position["entry"]) * direction * 100000 * position["volume"]
                self.simulation_balance += pnl
                record = {**position, "exit": exit_price, "pnl": pnl, "reason": reason}
                self.mock_positions.pop(ticket)
                self.last_closed_trade = record
                closed.append(record)
        return closed

    def order_send(self, request):
        self.last_request = dict(request)
        self._tick = SimpleNamespace(
            bid=float(request.get("price", self._tick.bid)) - 0.00005,
            ask=float(request.get("price", self._tick.ask)) + 0.00005,
        )
        return super().order_send(request)


class PacedMockFeed:
    """Continuous deterministic M15 candles with alternating market regimes."""

    def __init__(self, symbol: str, interval: float, logger: logging.Logger) -> None:
        self.symbol = symbol
        self.interval = interval
        self.logger = logger
        self.on_event = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self.candles: deque[dict] = deque(maxlen=240)
        self.last_price = 1.1000
        self._index = 0
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        self._origin = now.replace(minute=(now.minute // 15) * 15)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="paced-mock-feed", daemon=True)
        self._thread.start()
        self.logger.info("Dashboard mock feed started for %s (continuous M15 candles)", self.symbol)

    def _step_price(self) -> float:
        # 40-candle trend blocks alternate up/down. A small deterministic
        # oscillation inside each block adds realistic candle structure.
        phase = (self._index // 40) % 2
        direction = 1.0 if phase == 0 else -1.0
        cycle = self._index % 8
        noise = (0.000035, -0.000015, 0.000020, -0.000030,
                 0.000025, -0.000010, 0.000015, -0.000020)[cycle]
        self.last_price = max(0.5, self.last_price + direction * 0.00012 + noise)
        return self.last_price

    def _run(self) -> None:
        deadline = time.monotonic()
        while not self._stop.is_set():
            close = self._step_price()
            spread = 0.00010
            candle = {
                "symbol": self.symbol,
                "time": self._origin + timedelta(minutes=15 * self._index),
                "open": close - 0.00004,
                "high": close + 0.00008,
                "low": close - 0.00009,
                "close": close,
            }
            with self._lock:
                self.candles.append(candle)
            event = {
                "symbol": self.symbol,
                "event_type": "tick",
                "timestamp": candle["time"],
                "bid": close - spread / 2,
                "ask": close + spread / 2,
            }
            before_event = getattr(self, "before_event", None)
            if before_event:
                before_event(event)
            if self.on_event:
                self.on_event(event)
            self._index += 1
            deadline += self.interval
            self._stop.wait(max(0.0, deadline - time.monotonic()))

    def snapshot(self) -> list[dict]:
        with self._lock:
            return list(self.candles)

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)
        self.logger.info("Dashboard mock feed stopped")


class TradingCockpit:
    PAGE = r"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>EURUSD Quant Cockpit — Safe Dry Run</title><style>
*{box-sizing:border-box}body{margin:0;background:#0b1220;color:#eaf1fb;font:14px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
header{padding:20px 26px;font-size:18px;font-weight:700;letter-spacing:.05em;border-bottom:1px solid #1c2b42}
main{display:grid;grid-template-columns:minmax(550px,1fr) 330px;gap:18px;padding:18px;height:calc(100vh - 70px)}
section{background:#0d1627;border:1px solid #24344e;border-radius:12px;overflow:hidden}.chart{padding:14px;display:flex;flex-direction:column}
#chart{width:100%;height:100%;min-height:500px}.panel{padding:20px;background:#111c2f}.label{font-size:10px;color:#8194b0;font-weight:700;letter-spacing:.08em;margin-top:14px}.value{font-size:17px;font-weight:700;margin-top:4px}.safe{color:#31d39a}.pnl{color:#31d39a}.muted{color:#9fb0c8}.foot{margin-top:26px;color:#ffcf70;font-size:10px;font-weight:700;line-height:1.5}.connected{color:#31d39a}
@media(max-width:900px){main{grid-template-columns:1fr;height:auto}.panel{display:grid;grid-template-columns:1fr 1fr;gap:0 20px}#chart{height:65vh}}
</style></head><body><header>EURUSD · EVENT-DRIVEN STRATEGY COCKPIT</header><main>
<section class="chart"><canvas id="chart"></canvas></section><section class="panel">
<div style="grid-column:1/-1;font-size:14px;font-weight:700;margin-bottom:2px">LIVE ACCOUNT & ENGINE</div>
<div><div class="label">SYSTEM MODE</div><div id="mode" class="value safe">DRY-RUN SIMULATION<br>(SAFE)</div></div>
<div><div class="label">ACCOUNT EQUITY</div><div id="equity" class="value">£10,000.00</div></div>
<div><div class="label">FLOATING P&amp;L</div><div id="pnl" class="value pnl">£0.00</div></div>
<div><div class="label">LAST POSITION SIZE</div><div id="size" class="value">—</div></div>
<div><div class="label">RISK BUDGET / ESTIMATED LOSS</div><div id="sizing-risk" class="value muted">—</div></div>
<div><div class="label">ENTRY / LAST PRICE</div><div id="prices" class="value muted">—</div></div>
<div><div class="label">TARGET STOP-LOSS</div><div id="stop" class="value" style="color:#ff9b70">—</div></div>
<div><div id="tp-label" class="label">TARGET TAKE-PROFIT · 1:2 RR</div><div id="tp" class="value" style="color:#ff9f43">Awaiting order</div></div>
<div><div class="label">ADX TREND GATE</div><div id="adx" class="value muted">Warming up</div></div>
<div><div class="label">UTC ENTRY SESSION</div><div id="session" class="value muted">12:00–17:00 UTC</div></div>
<div><div class="label">RISK GATE</div><div id="risk" class="value connected">CONNECTED · CLEAR</div></div>
<div><div class="label">SIMULATED TICKET</div><div id="ticket" class="value muted">Waiting for signal</div></div>
<div><div class="label">SESSION UPTIME</div><div id="uptime" class="value muted">00:00</div></div>
<div class="foot" style="grid-column:1/-1">MOCK FEED · ORDER REQUESTS STAY IN MEMORY AND NEVER REACH THE BROKER</div></section></main>
<script>
const canvas=document.getElementById('chart'),ctx=canvas.getContext('2d');let latest=null;
function sizeCanvas(){const d=window.devicePixelRatio||1,r=canvas.getBoundingClientRect();canvas.width=r.width*d;canvas.height=r.height*d;ctx.setTransform(d,0,0,d,0,0)}
new ResizeObserver(sizeCanvas).observe(canvas);
function sma(xs,n){return xs.map((_,i)=>i+1<n?null:xs.slice(i+1-n,i+1).reduce((a,b)=>a+b,0)/n)}
function draw(s){latest=s;sizeCanvas();const r=canvas.getBoundingClientRect(),w=r.width,h=r.height;ctx.clearRect(0,0,w,h);ctx.fillStyle='#0b1220';ctx.fillRect(0,0,w,h);ctx.fillStyle='#d8e5f7';ctx.font='bold 12px -apple-system';ctx.fillText('M15 SIMULATED PRICE · MOVING-AVERAGE CROSSOVER',18,24);
const cs=(s.candles||[]).slice(-100);if(!cs.length)return;const L=72,R=w-20,T=50,B=h-45,cl=cs.map(c=>c.close),hi=Math.max(...cs.map(c=>c.high)),lo=Math.min(...cs.map(c=>c.low)),pad=Math.max((hi-lo)*.15,.0002),min=lo-pad,max=hi+pad;const X=i=>L+(i+.5)*(R-L)/cs.length,Y=v=>B-(v-min)/(max-min)*(B-T);
ctx.strokeStyle='#1b2940';ctx.fillStyle='#9fb0c8';ctx.font='10px Menlo';for(let j=0;j<=5;j++){let f=j/5,y=B-f*(B-T),p=min+f*(max-min);ctx.beginPath();ctx.moveTo(L,y);ctx.lineTo(R,y);ctx.stroke();ctx.textAlign='right';ctx.fillText(p.toFixed(5),L-8,y+3)}
for(let i=0;i<cs.length;i++){let c=cs[i],x=X(i),color=c.close>=c.open?'#31d39a':'#ff647c';ctx.strokeStyle=color;ctx.fillStyle=color;ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(x,Y(c.high));ctx.lineTo(x,Y(c.low));ctx.stroke();let top=Y(Math.max(c.open,c.close)),bottom=Y(Math.min(c.open,c.close));ctx.fillRect(x-2.8,top,5.6,Math.max(2,bottom-top))}
const fast=sma(cl,s.fast_period),slow=sma(cl,s.slow_period);for(const [arr,color] of [[fast,'#21a8ff'],[slow,'#ff9f43']]){ctx.strokeStyle=color;ctx.lineWidth=2.2;ctx.beginPath();let begun=false;arr.forEach((v,i)=>{if(v===null)return;if(!begun){ctx.moveTo(X(i),Y(v));begun=true}else ctx.lineTo(X(i),Y(v))});ctx.stroke()}
for(const m of s.markers||[]){let idx=cs.findIndex(c=>c.time===m.time);if(idx<0)continue;let x=X(idx),y=Y(m.price),buy=m.side==='buy';ctx.fillStyle=buy?'#27e58b':'#ff5068';ctx.strokeStyle='#f4f8ff';ctx.lineWidth=1.2;ctx.beginPath();ctx.moveTo(x,y+(buy?-14:14));ctx.lineTo(x-9,y+(buy?2:-2));ctx.lineTo(x+9,y+(buy?2:-2));ctx.closePath();ctx.fill();ctx.stroke()}
ctx.textAlign='left';ctx.fillStyle='#21a8ff';ctx.font='bold 10px sans-serif';ctx.fillText('● FAST SMA ('+s.fast_period+')',L,T-13);ctx.fillStyle='#ff9f43';ctx.fillText('● SLOW SMA ('+s.slow_period+')',L+130,T-13);ctx.fillStyle='#9fb0c8';ctx.font='10px Menlo';ctx.fillText(cs[0].time_label,L,B+22);ctx.textAlign='right';ctx.fillText(cs[cs.length-1].time_label,R,B+22)
}
function updatePanel(s){const p=s.position||{};document.getElementById('equity').textContent='£'+s.equity.toLocaleString('en-GB',{minimumFractionDigits:2,maximumFractionDigits:2});const pnl=document.getElementById('pnl');pnl.textContent=(s.floating_pnl>=0?'+':'')+'£'+s.floating_pnl.toFixed(2);pnl.style.color=s.floating_pnl>=0?'#31d39a':'#ff647c';document.getElementById('size').textContent=p.volume?Number(p.volume).toFixed(2)+' lots':'—';document.getElementById('sizing-risk').textContent=p.risk_budget==null?'—':'£'+Number(p.risk_budget).toFixed(2)+' / £'+Number(p.estimated_loss).toFixed(2);document.getElementById('prices').textContent=p.entry?Number(p.entry).toFixed(5)+' / '+Number(s.last_price).toFixed(5):'—';document.getElementById('stop').textContent=p.stop?Number(p.stop).toFixed(5):'—';document.getElementById('tp-label').textContent='TARGET TAKE-PROFIT · 1:'+s.risk_reward_multiple+' RR';document.getElementById('tp').textContent=p.tp?Number(p.tp).toFixed(5):'Awaiting order';const inBand=s.adx!==null&&s.adx>s.adx_threshold&&s.adx<s.adx_max_threshold;document.getElementById('adx').textContent=s.adx===null?'WARMING UP':('ADX '+s.adx.toFixed(1)+' · '+s.adx_threshold.toFixed(0)+'–'+s.adx_max_threshold.toFixed(0));document.getElementById('adx').style.color=inBand?'#31d39a':'#ffcf70';document.getElementById('session').textContent=s.session_open?'OPEN · '+s.session_window:'LOCKED · '+s.session_window;document.getElementById('session').style.color=s.session_open?'#31d39a':'#ffcf70';document.getElementById('risk').textContent=s.risk_ready?'CONNECTED · CLEAR':'CHECK REQUIRED';document.getElementById('risk').style.color=s.risk_ready?'#31d39a':'#ff647c';document.getElementById('ticket').textContent=p.ticket||'Waiting for signal';document.getElementById('uptime').textContent=String(Math.floor(s.uptime/60)).padStart(2,'0')+':'+String(Math.floor(s.uptime%60)).padStart(2,'0')}
async function poll(){try{let r=await fetch('/state',{cache:'no-store'});let s=await r.json();updatePanel(s);draw(s)}catch(e){document.getElementById('risk').textContent='LOCAL FEED DISCONNECTED'}}setInterval(poll,500);poll();
</script></body></html>"""

    def __init__(self, args: argparse.Namespace) -> None:
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from urllib.parse import urlparse
        self.args = args
        self.root_dir = Path(__file__).resolve().parent
        load_dotenv(self.root_dir / "config" / ".env", override=True)
        if os.getenv("TRADING_ENABLED", "true").strip().lower() not in {"false", "0", "no"}:
            raise RuntimeError("config/.env must have TRADING_ENABLED=false")
        for key in ("MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER"):
            if os.getenv(key, "MOCK_MODE") != "MOCK_MODE":
                raise RuntimeError("Dashboard requires MT5 broker settings to stay MOCK_MODE")
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.telegram_chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if args.telegram and (not self.telegram_token or not self.telegram_chat):
            raise RuntimeError("Telegram requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in config/.env")
        self.logger = logging.getLogger("dashboard")
        self.mt5 = DashboardMT5()
        self.feed = PacedMockFeed("EURUSD", args.interval, self.logger)
        self.feed.before_event = self.mt5.update_quote
        self.oms = MT5OMS(self.mt5)
        self.risk = MT5PreTradeRisk(self.mt5, ["EURUSD"], max_daily_loss_pct=0.05,
                                     spread_min_samples=1, max_spread_points=20.0,
                                     auto_kill_switch=False)
        self.engine = MT5StrategyEngine.from_config(
            self.mt5, self.feed, self.oms, self.risk,
            self.root_dir / "config" / "strategy_params.json", logger=self.logger,
        )
        # The engine installs itself as the primary feed callback and chains
        # this simulator hook after each event so exits run on every mock tick.
        self.feed.on_event = self._on_mock_tick
        self._lock = threading.RLock()
        self.markers: list[dict] = []
        self._last_seen = None
        self._started = time.monotonic()
        self._stop = threading.Event()

        cockpit = self
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                path = urlparse(self.path).path
                if path == "/state":
                    import json
                    body = json.dumps(cockpit.state(), separators=(",", ":")).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Cache-Control", "no-store")
                elif path == "/":
                    body = cockpit.PAGE.encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                else:
                    self.send_error(404)
                    return
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, fmt, *values):
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
        self.server.timeout = 0.25
        self.url = f"http://127.0.0.1:{self.server.server_port}/"
        self.engine.start(start_feed=True, start_risk_monitor=True)
        self.logger.info("Dashboard running at %s — MOCK_MODE; broker orders disabled", self.url)

    def state(self) -> dict:
        candles = self.feed.snapshot()
        state = self.engine.last_order_state
        req = self.mt5.last_request or {}
        last_price = candles[-1]["close"] if candles else 1.1
        active_values = list(self.mt5.mock_positions.values())
        active = active_values[-1] if active_values else None
        entry = float(active["entry"]) if active else 0.0
        volume = float(active["volume"]) if active else 0.0
        floating = self.mt5.floating_pnl()
        size = self.engine.last_position_size
        risk = self.risk.snapshot()
        return {
            "candles": [{**c, "time": c["time"].isoformat(),
                         "time_label": c["time"].strftime("%d %b %H:%M UTC")} for c in candles],
            "markers": list(self.markers), "fast_period": self.engine.fast_period,
            "slow_period": self.engine.slow_period, "last_price": last_price,
            "equity": self.mt5.simulation_balance + floating, "floating_pnl": floating,
            "risk_ready": bool(
                risk.terminal_connected and not risk.breached
                and risk.news_calendar_available and not risk.news_blackout
            ),
            "adx": self.engine.last_adx, "adx_threshold": self.engine.adx_threshold,
            "adx_max_threshold": self.engine.adx_max_threshold,
            "risk_reward_multiple": self.engine.risk_reward_multiple,
            "session_window": f"{self.engine.session_start_utc:%H:%M}–{self.engine.session_end_utc:%H:%M} UTC",
            "session_open": self._session_open(candles),
            "uptime": time.monotonic() - self._started,
            "position": {"ticket": active["ticket"] if active else None, "volume": volume,
                         "entry": entry, "stop": active["sl"] if active else 0.0,
                         "tp": active["tp"] if active else 0.0,
                         "risk_budget": getattr(size, "risk_budget", None),
                         "estimated_loss": getattr(size, "estimated_loss", None),
                         "last_order_status": getattr(state, "status", "idle")},
        }

    def _on_mock_tick(self, event: dict) -> None:
        """Open simulated fills and close them at their stop or target levels."""
        bid, ask = float(event["bid"]), float(event["ask"])
        state = self.engine.last_order_state
        self.mt5.sync_mock_position(state, self.mt5.last_request or {})
        for closed in self.mt5.evaluate_mock_exits(bid, ask):
            self.logger.info(
                "Mock position closed ticket=%s reason=%s exit=%.5f realized_pnl=%.2f equity=%.2f",
                closed["ticket"], closed["reason"], closed["exit"], closed["pnl"],
                self.mt5.simulation_balance,
            )

    def _session_open(self, candles: list[dict]) -> bool:
        if not candles:
            return False
        close_time = candles[-1]["time"] + timedelta(minutes=self.engine.timeframe_minutes)
        seconds = close_time.hour * 3600 + close_time.minute * 60 + close_time.second
        start = self.engine.session_start_utc.hour * 3600 + self.engine.session_start_utc.minute * 60
        end = self.engine.session_end_utc.hour * 3600 + self.engine.session_end_utc.minute * 60
        return start <= seconds < end

    def process_signal(self) -> None:
        signal = self.engine.last_signal
        if signal is None or signal.timestamp == self._last_seen:
            return
        state = self.engine.last_order_state
        if state is None or state.status != "filled":
            return
        self._last_seen = signal.timestamp
        req = self.mt5.last_request or {}
        marker = {"time": signal.timestamp.isoformat(), "price": signal.close,
                  "side": signal.side, "ticket": state.ticket}
        self.markers.append(marker)
        self.logger.info("Visual %s crossover ticket=%s lots=%.2f", signal.side.upper(),
                         state.ticket, state.filled_volume)
        if self.args.telegram:
            text = (f"MOCK EURUSD {signal.side.upper()} crossover\nTicket: {state.ticket} | "
                    f"Size: {state.filled_volume:.2f} lots\nEntry: {state.average_price:.5f} | "
                f"Stop: {float(req.get('sl', 0.0) or 0.0):.5f} | "
                f"Target: {float(req.get('tp', 0.0) or 0.0):.5f} "
                f"(1:{self.engine.risk_reward_multiple:g} RR)\n"
                "Simulation only; no broker order was sent.")
            threading.Thread(target=notify_telegram,
                             args=(self.telegram_token, self.telegram_chat, text, self.logger),
                             daemon=True, name="telegram-mock-trade").start()

    def run(self) -> None:
        import webbrowser
        if not self.args.no_open:
            webbrowser.open(self.url, new=1, autoraise=True)
        try:
            while not self._stop.is_set():
                self.server.handle_request()
                self.process_signal()
        except KeyboardInterrupt:
            self.logger.info("Dashboard stop requested")
        finally:
            self.server.server_close()
            self.engine.stop(stop_feed=True, stop_risk_monitor=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Local browser cockpit for the mock trading pipeline")
    parser.add_argument("--interval", type=float, default=0.65,
                        help="seconds between simulated M15 candles")
    parser.add_argument("--port", type=int, default=0,
                        help="local HTTP port (0 selects an available port)")
    parser.add_argument("--no-telegram", action="store_true",
                        help="disable Telegram notices for simulated crossovers")
    parser.add_argument("--no-open", action="store_true",
                        help="serve dashboard without opening the default browser")
    args = parser.parse_args()
    if args.interval <= 0 or args.port < 0 or args.port > 65535:
        parser.error("interval must be positive and port must be 0..65535")
    args.telegram = not args.no_telegram
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        cockpit = TradingCockpit(args)
        cockpit.run()
    except Exception as exc:
        logging.getLogger("dashboard").exception("Dashboard could not start: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
