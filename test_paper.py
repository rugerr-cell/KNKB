import time
from datetime import datetime, timezone
import pytest
from sqlalchemy import text
from test_app import a, reset
import paper


def row(**kw):
    now=time.time()
    r=dict(ticker='KXBTC15M-PAPER',asset='BTC',action='LEAN YES',data_quality='COMPLETE',
           close_time=now+200, updated_at=datetime.fromtimestamp(now,timezone.utc).isoformat(),
           book_captured_at=now, model_probability=.8,yes_asks=[(.5,10)],no_asks=[(.5,10)],setup_score=90)
    r.update(kw)
    return r


def track(con,r):
    con.execute(text('INSERT INTO tracked_markets(ticker,asset,close_time) VALUES(:ticker,:asset,:close_time)'),r)


def test_depth_walk_and_current_fee_rounding():
    r=row(yes_asks=[(.5,3),(.6,7)])
    q=paper.quote(r,time.time())
    assert q['fill_price']==pytest.approx(.58)
    assert q['quote_price']==.5
    assert q['fee']==pytest.approx(.1706)
    assert q['cost']==pytest.approx(5.9706)
    assert q['slippage_cost']==.1
    assert q['net_edge']==pytest.approx(.8-.59706)


@pytest.mark.parametrize('kw',[{'action':'WATCH YES'},{'data_quality':'INCOMPLETE'},
    {'yes_asks':[(.5,9)]},{'yes_asks':[]},{'yes_asks':[(.99,10)]},
    {'book_captured_at':time.time()-30},{'close_time':time.time()+5},
    {'model_probability':float('nan')},{'model_probability':.51}])
def test_reject_unfillable_stale_or_unqualified_entries(kw):
    assert paper.quote(row(**kw),time.time()) is None


def test_no_uses_no_depth_and_probability():
    q=paper.quote(row(action='LEAN NO',model_probability=.2,no_asks=[(.4,10)]),time.time())
    assert q['side']=='NO' and q['fill_price']==.41 and q['net_edge']>.3


def test_once_per_market_and_idempotent_settlement():
    r=row()
    with a.engine.begin() as con:
        track(con,r)
        paper.capture(con,[r,r])
        paper.capture(con,[row(action='LEAN NO',model_probability=.1)])
        initial=paper.summary(con)
        assert initial['summary']['trades']==1
        assert initial['summary']['cash']<1000
        paper.settle(con,r['ticker'],'YES','2026-10-08')
        paper.settle(con,r['ticker'],'NO','2026-10-09')
        p=paper.summary(con)
    assert p['rows'][0]['outcome']=='YES'
    assert p['summary']['wins']==1 and p['summary']['pending']==0
    assert p['summary']['net_pnl']==pytest.approx(4.725)
    assert p['summary']['cash']==pytest.approx(1004.725)


def test_simultaneous_round_drawdown_and_breakdowns():
    now=time.time()
    with a.engine.begin() as con:
        for ticker,asset,close,price,result in [('A','BTC',now+30,.8,'NO'),('B','ETH',now+30,.1,'YES'),('C','BTC',now+100,.5,'NO')]:
            r=row(ticker=ticker,asset=asset,close_time=close,yes_asks=[(price,10)],model_probability=.999)
            track(con,r);paper.capture(con,[r]);paper.settle(con,ticker,result,'now')
        p=paper.summary(con)
    assert p['summary']['settled_rounds']==2
    # First round's net is positive, despite one loss: no artificial within-round drawdown.
    assert p['equity_curve'][0]['pnl']>0
    assert p['summary']['max_realized_drawdown']==pytest.approx(5.275)
    assert len(p['breakdowns']['asset'])==2
    assert len(p['breakdowns']['entry_price'])==3
    assert len(p['breakdowns']['time_left'])==2


def test_cash_limit_and_no_retrospective_capture():
    r=row()
    with a.engine.begin() as con:
        track(con,r)
        con.execute(text("UPDATE tracked_markets SET outcome='YES'"))
        paper.capture(con,[r]);assert paper.summary(con)['summary']['trades']==0
        con.execute(text('UPDATE tracked_markets SET outcome=NULL'))
        q=paper.quote(r,time.time());q['ticker']='DRAIN';q['cost']=999
        fields=list(q)
        con.execute(text('INSERT INTO paper_trades('+','.join(fields)+') VALUES('+','.join(':'+f for f in fields)+')'),q)
        paper.capture(con,[r]);assert paper.summary(con)['summary']['trades']==1


def test_collector_integration_and_settlement(monkeypatch):
    r=row();r.update(seconds_left=200,series='KXBTC15M',target=100)
    a.log_signals([r]);a.log_signals([r])
    with a.engine.begin() as con:
        con.execute(text('UPDATE tracked_markets SET close_time=:close'),{'close':time.time()-10})
    monkeypatch.setattr(a,'kalshi_json',lambda *args,**kw:{'market':{'result':'yes'}})
    a.settle_pending()
    p=a.performance_summary()['paper']
    assert p['summary']['settled']==1 and p['summary']['wins']==1
    assert a.paper_performance()['policy']=='paper-v1'
