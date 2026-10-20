"""Generate deterministic, regime-based synthetic EURUSD ticks for sandboxing.

This is mock GBM/mean-reverting data, not broker history and not suitable for
evaluating live strategy performance. Default output is EURUSD_ticks.csv beside
this script. The default path is 90 continuous days at five-second resolution.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate(
    output: Path,
    *,
    rows: int | None = None,
    duration_days: float = 90.0,
    tick_interval_seconds: float = 5.0,
    seed: int = 20261020,
    start_price: float = 1.08500,
    annual_volatility: float = 0.16,
    spread: float = 0.00008,
    price_tick: float = 0.00001,
) -> dict[str, float | int | str]:
    """Write fixed-cadence ticks with alternating trend and consolidation regimes."""
    if duration_days <= 0 or tick_interval_seconds <= 0:
        raise ValueError("duration and tick interval must be positive")
    if rows is None:
        rows = math.floor(duration_days * 24 * 60 * 60 / tick_interval_seconds) + 1
    if rows < 1:
        raise ValueError("rows must be positive")
    if start_price <= 0 or annual_volatility <= 0:
        raise ValueError("price and volatility must be positive")
    if spread <= 0 or price_tick <= 0:
        raise ValueError("spread and price_tick must be positive")

    rng = random.Random(seed)
    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    end_time = datetime.now(timezone.utc).replace(microsecond=0)
    timestamp = end_time - timedelta(seconds=(rows - 1) * tick_interval_seconds)
    price = start_price
    regime_anchor = price
    half_spread = spread / 2.0
    annual_seconds = 365.25 * 24 * 60 * 60
    regime_seconds = 20 * 24 * 60 * 60
    block_seconds = (6, 4, 6, 4)  # uptrend, consolidation, downtrend, consolidation
    block_cumulative = (0, 6, 10, 16, 20)
    first_time = timestamp
    min_bid = float("inf")
    max_ask = 0.0

    temp_output = output.with_name(output.name + ".tmp")
    with temp_output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("timestamp", "bid", "ask", "volume"))
        for idx in range(rows):
            if idx:
                timestamp += timedelta(seconds=tick_interval_seconds)
                elapsed = (idx - 1) * tick_interval_seconds
                phase = (elapsed % regime_seconds) / (24 * 60 * 60)
                if phase < 6:
                    regime = "uptrend"
                    drift, sigma = 0.45, annual_volatility * 0.85
                elif phase < 10:
                    regime = "consolidation"
                    drift, sigma = 0.0, annual_volatility * 0.55
                elif phase < 16:
                    regime = "downtrend"
                    drift, sigma = -0.45, annual_volatility * 0.85
                else:
                    regime = "consolidation"
                    drift, sigma = 0.0, annual_volatility * 0.55
                if idx == 1 or regime != previous_regime:
                    regime_anchor = price
                previous_regime = regime
                dt_years = tick_interval_seconds / annual_seconds
                if regime == "consolidation":
                    # Ornstein-Uhlenbeck pull around the price at the start of
                    # this block creates a distinct, bounded sideways market.
                    kappa = 2.0  # annualized mean-reversion speed
                    log_price = math.log(price)
                    pull = kappa * (math.log(regime_anchor) - log_price) * dt_years
                    shock = sigma * math.sqrt(dt_years) * rng.gauss(0.0, 1.0)
                    price = math.exp(log_price + pull + shock)
                else:
                    # GBM with positive/negative drift gives explicit trend blocks.
                    price *= math.exp(
                        (drift - 0.5 * sigma**2) * dt_years
                        + sigma * math.sqrt(dt_years) * rng.gauss(0.0, 1.0)
                    )

            bid = math.floor((price - half_spread) / price_tick) * price_tick
            ask = math.ceil((price + half_spread) / price_tick) * price_tick
            bid = round(bid, 5)
            ask = round(ask, 5)
            volume = rng.randint(1, 100)
            writer.writerow((timestamp.isoformat(timespec="milliseconds"),
                             f"{bid:.5f}", f"{ask:.5f}", volume))
            min_bid = min(min_bid, bid)
            max_ask = max(max_ask, ask)
        stream.flush()
        os.fsync(stream.fileno())
    temp_output.replace(output)

    return {
        "path": str(output),
        "rows": rows,
        "seed": seed,
        "start_timestamp_utc": first_time.isoformat(timespec="seconds"),
        "end_timestamp_utc": timestamp.isoformat(timespec="milliseconds"),
        "min_bid": min_bid,
        "max_ask": max_ask,
        "size_bytes": output.stat().st_size,
    }


def main() -> int:
    default_path = Path(__file__).resolve().parent / "EURUSD_ticks.csv"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=default_path)
    parser.add_argument("--rows", type=int, default=None,
                        help="optional explicit row count (overrides duration)")
    parser.add_argument("--duration-days", type=float, default=90.0)
    parser.add_argument("--tick-interval-seconds", type=float, default=5.0)
    parser.add_argument("--seed", type=int, default=20261020)
    parser.add_argument("--start-price", type=float, default=1.08500)
    parser.add_argument("--annual-volatility", type=float, default=0.16)
    parser.add_argument("--spread", type=float, default=0.00008)
    args = parser.parse_args()

    result = generate(
        args.output,
        rows=args.rows,
        duration_days=args.duration_days,
        tick_interval_seconds=args.tick_interval_seconds,
        seed=args.seed,
        start_price=args.start_price,
        annual_volatility=args.annual_volatility,
        spread=args.spread,
    )
    expected_rows = args.rows or math.floor(
        args.duration_days * 24 * 60 * 60 / args.tick_interval_seconds
    ) + 1
    with Path(result["path"]).open("rb") as stream:
        file_rows = sum(1 for _ in stream) - 1  # exclude header
    if result["rows"] != expected_rows or file_rows != expected_rows or result["size_bytes"] <= 0:
        raise RuntimeError("Synthetic tick file verification failed")
    print(f"Created {result['rows']:,} synthetic ticks: {result['path']}")
    print(f"UTC range: {result['start_timestamp_utc']} to {result['end_timestamp_utc']}")
    print(f"Bid/ask range: {result['min_bid']:.5f} to {result['max_ask']:.5f}")
    print(f"Size: {result['size_bytes']:,} bytes")
    print("Data type: synthetic GBM sandbox data; not real MT5 history")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
