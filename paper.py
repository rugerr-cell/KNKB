"""Forward-only paper ledger. No credentials, orders, or retrospective entries."""
import math
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from sqlalchemy import text

POLICY = 'paper-v1'
STARTING_BANK = 1000.0
CONTRACTS = 10
SLIPPAGE = 0.01
FEE_MULTIPLIER = 1.0  # Explicit simulation assumption, not live series verification.
MIN_NET_EDGE = 0.02


def init(con):
    con.execute(text('''CREATE TABLE IF NOT EXISTS paper_trades (
        policy TEXT NOT NULL, ticker TEXT NOT NULL, asset TEXT NOT NULL,
        captured_at TEXT NOT NULL, close_time DOUBLE PRECISION NOT NULL,
        seconds_left DOUBLE PRECISION NOT NULL, side TEXT NOT NULL,
        contracts INTEGER NOT NULL, quote_price DOUBLE PRECISION NOT NULL,
        fill_price DOUBLE PRECISION NOT NULL, fee DOUBLE PRECISION NOT NULL,
        slippage_cost DOUBLE PRECISION NOT NULL, cost DOUBLE PRECISION NOT NULL,
        model_probability DOUBLE PRECISION NOT NULL, net_edge DOUBLE PRECISION NOT NULL,
        setup_score DOUBLE PRECISION NOT NULL, action TEXT NOT NULL,
        outcome TEXT, settled_at TEXT, payout DOUBLE PRECISION, pnl DOUBLE PRECISION,
        PRIMARY KEY (policy, ticker)
    )'''))
    con.execute(text('CREATE INDEX IF NOT EXISTS idx_paper_close ON paper_trades(close_time)'))


def rounded_total(price, quantity):
    """July 2026 general quadratic taker model, total rounded to centicent."""
    p = Decimal(str(price))
    position = p * quantity
    raw_fee = Decimal('0.07') * Decimal(str(FEE_MULTIPLIER)) * quantity * p * (1-p)
    total = (position + raw_fee).quantize(Decimal('0.0001'), rounding=ROUND_CEILING)
    return float(total), float(total-position)


def quote(row, now):
    if row.get('action') not in ('LEAN YES', 'LEAN NO') or row.get('data_quality') != 'COMPLETE':
        return None
    try:
        close = float(row['close_time'])
        updated = datetime.fromisoformat(row['updated_at']).timestamp()
        book_age = now-float(row["book_captured_at"])
        probability = float(row['model_probability'])
        if not (math.isfinite(close) and math.isfinite(probability) and 0 <= probability <= 1
                and 0 <= book_age <= 20 and 12 < close-now <= 905 and 0 <= now-updated <= 20):
            return None
        side = row['action'].split()[-1]
        levels = row.get(side.lower() + '_asks') or []
        # Depth is mandatory. Aggregate top-ten quantity is not enough to assume a fill.
        remaining, spent, best = CONTRACTS, 0.0, None
        for price, qty in sorted(levels):
            price, qty = float(price), float(qty)
            if not (math.isfinite(price) and math.isfinite(qty) and 0 < price < 1 and qty > 0):
                return None
            best = price if best is None else best
            take = min(remaining, qty)
            spent += take*price
            remaining -= take
            if remaining <= 0:
                break
        if remaining > 0 or best is None:
            return None
        average = spent/CONTRACTS
        fill = round(average + SLIPPAGE, 8)
        if fill >= 1:
            return None
        cost, fee = rounded_total(fill, CONTRACTS)
        side_probability = probability if side == 'YES' else 1-probability
        net_edge = side_probability - cost/CONTRACTS
        if net_edge < MIN_NET_EDGE:
            return None
        return dict(policy=POLICY, ticker=row['ticker'], asset=row['asset'],
                    captured_at=datetime.fromtimestamp(now, timezone.utc).isoformat(), close_time=close,
                    seconds_left=close-now, side=side, contracts=CONTRACTS, quote_price=best,
                    fill_price=fill, fee=fee, slippage_cost=round(CONTRACTS*SLIPPAGE, 8), cost=cost,
                    model_probability=probability, net_edge=net_edge,
                    setup_score=row['setup_score'], action=row['action'])
    except (KeyError, ValueError, TypeError, OverflowError):
        return None


def capture(con, rows):
    cash = STARTING_BANK + con.execute(text('''SELECT COALESCE(SUM(COALESCE(payout,0)-cost),0)
        FROM paper_trades WHERE policy=:policy'''), {'policy': POLICY}).scalar_one()
    for row in sorted(rows, key=lambda r: r.get('ticker', '')):
        trade = quote(row, time.time())
        if not trade or trade['cost'] > cash:
            continue
        if con.execute(text('SELECT ticker FROM paper_trades WHERE policy=:policy AND ticker=:ticker'), trade).first():
            continue
        market = con.execute(text('SELECT outcome FROM tracked_markets WHERE ticker=:ticker'), trade).first()
        if not market or market.outcome is not None:
            continue
        fields = list(trade)
        result = con.execute(text('INSERT INTO paper_trades (' + ','.join(fields) + ') VALUES (' +
                                  ','.join(':'+f for f in fields) + ') ON CONFLICT (policy,ticker) DO NOTHING'), trade)
        if result.rowcount:
            cash -= trade['cost']


def settle(con, ticker, outcome, settled_at):
    if outcome not in ('YES', 'NO'):
        return
    con.execute(text('''UPDATE paper_trades SET outcome=:outcome, settled_at=:settled_at,
        payout=CASE WHEN side=:outcome THEN contracts ELSE 0 END,
        pnl=(CASE WHEN side=:outcome THEN contracts ELSE 0 END)-cost
        WHERE ticker=:ticker AND outcome IS NULL'''),
        dict(ticker=ticker, outcome=outcome, settled_at=settled_at))


def stats(rows):
    settled = [r for r in rows if r['pnl'] is not None]
    pnl = sum(r['pnl'] for r in settled)
    cost = sum(r['cost'] for r in settled)
    return dict(trades=len(rows), settled=len(settled), pending=len(rows)-len(settled),
                wins=sum(r['payout'] > 0 for r in settled),
                win_rate=sum(r['payout'] > 0 for r in settled)/len(settled) if settled else None,
                net_pnl=round(pnl, 4), roi=pnl/cost if cost else None,
                fees=round(sum(r['fee'] for r in rows),4),
                slippage_cost=round(sum(r['slippage_cost'] for r in rows),4))


def summary(con):
    rows = [dict(r) for r in con.execute(text('SELECT * FROM paper_trades WHERE policy=:policy ORDER BY captured_at, ticker'),
                                         {'policy': POLICY}).mappings()]
    result = stats(rows)
    locked = sum(r['cost'] for r in rows if r['pnl'] is None)
    result.update(starting_bank=STARTING_BANK, cash=round(STARTING_BANK + result['net_pnl']-locked,4),
                  pending_cost=round(locked,4), realized_equity=round(STARTING_BANK+result['net_pnl'],4))
    # Group simultaneous expiries before computing the path; asset ordering cannot create drawdowns.
    rounds = defaultdict(float)
    for row in rows:
        if row['pnl'] is not None:
            rounds[row['close_time']] += row['pnl']
    equity, peak, drawdown, curve = STARTING_BANK, STARTING_BANK, 0.0, []
    for close, pnl in sorted(rounds.items()):
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak-equity)
        curve.append(dict(close_time=close, equity=round(equity,4), pnl=round(pnl,4)))
    result['max_realized_drawdown'] = round(drawdown,4)
    result['settled_rounds'] = len(rounds)
    groups = {}
    keys = dict(asset=lambda r:r['asset'],
                entry_price=lambda r:'Under 25¢' if r['fill_price'] < .25 else '25–49¢' if r['fill_price'] < .5 else '50–74¢' if r['fill_price'] < .75 else '75¢+',
                time_left=lambda r:'Final 1m' if r['seconds_left'] <= 60 else '1–5m' if r['seconds_left'] <= 300 else '5–10m' if r['seconds_left'] <= 600 else '10–15m')
    for dimension, key in keys.items():
        bins = defaultdict(list)
        for row in rows:
            bins[key(row)].append(row)
        groups[dimension] = [dict(label=label, **stats(items)) for label, items in sorted(bins.items())]
    return dict(policy=POLICY, summary=result, breakdowns=groups, equity_curve=curve[-200:],
                rows=list(reversed(rows[-50:])), assumptions=dict(contracts=CONTRACTS, slippage_per_contract=SLIPPAGE,
                fee_multiplier=FEE_MULTIPLIER, fee_model='General quadratic taker fee; multiplier 1 assumed, not verified per series',
                fee_rounding='Position plus fee rounded up to $0.0001', entry='First fresh LEAN per market, positive net edge ≥2¢, full displayed depth required',
                exit='Hold to explicit settlement', drawdown='Realized P&L grouped by expiry; excludes open mark-to-market',
                collection='Forward only; collection pauses when host sleeps', shared='One shared simulated account'))
