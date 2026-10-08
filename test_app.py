import importlib
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

_tmp = tempfile.TemporaryDirectory()
os.environ['KNKB_DB_PATH'] = os.path.join(_tmp.name, 'test.db')
os.environ['DATABASE_URL'] = ''
os.environ['KNKB_BACKGROUND'] = '0'
a = importlib.import_module('app')

@pytest.fixture(autouse=True)
def reset(monkeypatch):
    a._scan_cache.update(ts=0, payload=None)
    a._candle_cache.clear()
    with a.engine.begin() as con:
        con.execute(text('DELETE FROM signals'))
        con.execute(text('DELETE FROM tracked_markets'))

def market(seconds=200, **kw):
    return dict(ticker='KXBTC15M-TEST', title='BTC 15 min', floor_strike=100,
                close_time=datetime.fromtimestamp(time.time()+seconds, timezone.utc).isoformat(), **kw)

def sources(monkeypatch, spot=100, yes=.4, no=.4, candles=True):
    monkeypatch.setattr(a,'spot_price',lambda _:spot)
    monkeypatch.setattr(a,'orderbook',lambda _:{'yes_bid':yes,'yes_ask':None if no is None else 1-no,
        'no_bid':no,'no_ask':None if yes is None else 1-yes,'yes_qty':10,'no_qty':10,'imbalance':0})
    monkeypatch.setattr(a,'candle_stats',lambda _:{'momentum_3m':0,'momentum_5m':0,'momentum_10m':0,
        'volatility_10m':.1,'range_5m':.1,'trend':'FLAT','source_time':time.time() if candles else None})

def test_nonfinite_rejected():
    assert a.num('nan',None) is None
    assert a.num('inf',None) is None

def test_target_prefers_structured_and_ignores_title_numbers():
    assert a.parse_target({'floor_strike':100,'subtitle':'Above $101'},100)==100
    assert a.parse_target({'title':'BTC 15 min on October 8 2026'},100) is None
    assert a.parse_target({'subtitle':'Above $0.0958525'},.095)==.0958525
    assert a.parse_target({'floor_strike':100},.1) is None

def test_only_current_markets_selected():
    old=market(-2);future=market(200,open_time=datetime.fromtimestamp(time.time()+20,timezone.utc).isoformat())
    assert a.select_open_markets([old,future])=={}
    assert 'BTC' in a.select_open_markets([market()])

def test_best_levels_not_worst_levels(monkeypatch):
    levels=[[f'{n/100:.2f}','2'] for n in range(1,21)]
    monkeypatch.setattr(a,'kalshi_json',lambda *args,**kw:{'orderbook_fp':{'yes_dollars':levels,'no_dollars':[['0.7','5']]}})
    ob=a.orderbook('TEST')
    assert ob['yes_bid']==.20
    assert ob['yes_qty']==20
    assert ob['yes_ask']==pytest.approx(.30)
    assert ob['no_ask']==pytest.approx(.80)

def test_legacy_orderbook_prices(monkeypatch):
    monkeypatch.setattr(a,'kalshi_json',lambda *args,**kw:{'orderbook':{'yes':[[40,2]],'no':[[55,3]]}})
    assert a.orderbook('TEST')['yes_ask']==pytest.approx(.45)

def test_spot_fallback_is_usd(monkeypatch):
    calls=[]
    def feed(url,params=None,**kw):
        calls.append((url,params))
        if 'coinbase' in url:raise RuntimeError('offline')
        return {'result':{'XXBTZUSD':{'c':['81000','1']}}}
    monkeypatch.setattr(a,'get_json',feed)
    assert a.spot_price('BTC')==81000
    assert calls[-1][1]=={'pair':'XBTUSD'}

def test_stale_candles_rejected(monkeypatch):
    now=time.time()
    candles=[[int(now)-600-i*60,90,110,100,100,1] for i in range(16)]
    monkeypatch.setattr(a,'get_json',lambda *args,**kw:candles)
    assert a.candle_stats('BTC')['source_time'] is None

def test_gapped_candles_rejected(monkeypatch):
    now=int(time.time())
    candles=[[now-i*120,90,110,100,100,1] for i in range(16)]
    monkeypatch.setattr(a,'get_json',lambda *args,**kw:candles)
    assert a.candle_stats('BTC')['source_time'] is None

@pytest.mark.parametrize('kwargs',[{'spot':None},{'no':None},{'candles':False}])
def test_incomplete_inputs_never_signal(monkeypatch,kwargs):
    sources(monkeypatch,**kwargs)
    r=a.analyze('BTC',market())
    assert r['action']=='PASS' and r['model_probability'] is None
    assert r['data_quality']=='INCOMPLETE' and r['quality_notes']

def test_expired_never_signals(monkeypatch):
    sources(monkeypatch,spot=110)
    assert a.analyze('BTC',market(-1))['action']=='PASS'

def test_both_side_costs_and_negative_edge(monkeypatch):
    sources(monkeypatch)
    r=a.analyze('BTC',market())
    assert r['model_probability']==pytest.approx(.5)
    assert r['yes_edge']==pytest.approx(-.1)
    assert r['no_edge']==pytest.approx(-.1)
    assert r['edge']==0 and r['action']=='PASS'

def test_no_edge_uses_no_ask(monkeypatch):
    sources(monkeypatch,spot=99,yes=.9,no=.05)
    r=a.analyze('BTC',market())
    assert r['no_edge']==pytest.approx(1-r['model_probability']-.1)
    assert r['edge']<0 and r['action'].endswith('NO')

def test_explicit_settlement_only():
    assert a.extract_outcome({'market':{'result':'yes'}})=='YES'
    assert a.extract_outcome({'market':{'result':'yes','is_provisional':True}}) is None
    assert a.extract_outcome({'market':{'status':'finalized','settlement_value_dollars':'1.0000'}})=='YES'
    assert a.extract_outcome({'market':{'status':'finalized','settlement_value':100}})=='YES'
    assert a.extract_outcome({'market':{'status':'active','settlement_value_dollars':'0'}}) is None
    assert a.extract_outcome({'market':{'status':'finalized','settlement_value':1}}) is None

def test_unique_market_grading_and_version(monkeypatch):
    sources(monkeypatch,spot=101)
    r=a.analyze('BTC',market());r['action']='LEAN YES'
    a.log_signals([r]);r['seconds_left']=140;a.log_signals([r]);a.log_signals([r])
    with a.engine.begin() as con:
        con.execute(text("UPDATE signals SET outcome='YES',correct=1"))
    p=a.performance_summary()
    assert p['snapshots']==2 and p['unique_graded']==1 and p['win_rate']==1
    assert p['brier_samples']==1 and 0<=p['brier_score']<=1

def test_cache_single_flight(monkeypatch):
    count=[]
    def build():
        count.append(1);time.sleep(.05)
        return {'markets':[],'errors':[],'live':False,'server_time':'now'}
    monkeypatch.setattr(a,'build_scan',build)
    with ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(lambda _:a.scan(),range(8)))
    assert len(count)==1

def test_stale_cache_not_live(monkeypatch):
    a._scan_cache.update(ts=time.time()-100,payload={'markets':[],'live':True})
    a._scan_lock.acquire()
    try:r=a.scan()
    finally:a._scan_lock.release()
    assert r['stale'] and not r['live']

def test_routes_do_not_expose_files():
    with TestClient(a.app) as client:
        assert client.get('/').status_code==200
        assert client.get('/api/health').json()['version']=='9.0'
        for path in ['/app.py','/requirements.txt','/knkb_history.db','/.env','/../app.py']:
            assert client.get(path).status_code==404
        response=client.get('/api/history.csv')
        assert response.status_code==200 and 'text/csv' in response.headers['content-type']
        assert 'signal_version' in response.text
