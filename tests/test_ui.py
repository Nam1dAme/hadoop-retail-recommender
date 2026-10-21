"""T16: actual Streamlit execution + isolated real MariaDB through the W09 API.

Only HTTP transport is bridged to TestClient; the UI never receives DB credentials.
Lifecycle mutations below affect the isolated fixture DB, not the formal release.
"""
from datetime import date
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from streamlit.testing.v1 import AppTest

from app.client import RetailAPI,ApiFault
from app.state import activate_release,pounds,percent,task_lanes,duration
from app.__main__ import ui_environment
from test_api import api,release_fixture
from test_releases import another,install

ROOT=Path(__file__).resolve().parents[1]


@pytest.fixture
def ui(api,monkeypatch):
    calls=[]
    mode={'code':None}
    original=RetailAPI.__init__
    def bridge(request):
        calls.append((request.method,request.url.path,dict(request.url.params)))
        if mode['code']:
            request_id='12345678-1234-1234-1234-123456789abc'
            return httpx.Response(503,headers={'x-request-id':request_id},json={
                'error':{'code':mode['code'],'message':'password=SECRET SELECT * FROM mysql.user'},
                'meta':{'request_id':request_id}})
        response=api['client'].get(request.url.path,params=request.url.params.multi_items())
        return httpx.Response(response.status_code,headers=dict(response.headers),content=response.content)
    def initialize(self,url,release=None,**kwargs):
        original(self,url,release,transport=httpx.MockTransport(bridge))
    monkeypatch.setattr(RetailAPI,'__init__',initialize)
    monkeypatch.setenv('RETAIL_UI_CONFIG',str(ROOT/'config/development.toml'))
    application=AppTest.from_file(str(ROOT/'app/main.py'),default_timeout=25)
    return dict(app=application,api=api,calls=calls,mode=mode)


def run(ui):
    ui['app'].run()
    assert not ui['app'].exception,[e.message for e in ui['app'].exception]
    return ui['app']


def button(app,label):
    return next(b for b in app.button if b.label==label)


def markup(app):
    return '\n'.join(e.proto.body for e in app.get('html'))


def navigate(ui,page):
    ui['app'].radio(key='nav_page').set_value(page)
    return run(ui)


def test_seven_views_real_api_tables_charts_and_release_context(ui):
    app=run(ui)
    pages={'overview':2,'sales':3,'customers':1,'associations':1,'recommendations':0,'experiments':3,'quality':0}
    for name,minimum in pages.items():
        if name!='overview':app=navigate(ui,name)
        assert not app.error,[e.value for e in app.error]
        assert len(app.get('plotly_chart'))>=minimum,name
        assert len(app.dataframe)>=1,name
        assert ui['api']['rid'] in markup(app)
    current_calls=[r for r in ui['calls'] if r[1].endswith('/releases/current')]
    assert len(current_calls)==1
    for method,path,params in ui['calls']:
        assert method=='GET'
        if path.startswith(('/api/v1/summary','/api/v1/sales/','/api/v1/products','/api/v1/customers')):
            assert params['release_id']==ui['api']['rid'],path
        else:assert 'release_id' not in params,path


def test_applied_filters_validation_empty_results_and_clear(ui):
    app=run(ui)
    app.date_input(key='view_overview_start').set_value(date(2026,1,1))
    app.date_input(key='view_overview_end').set_value(date(2026,1,2))
    run(ui) # Unsaved draft must not change applied query conditions.
    summaries=[r for r in ui['calls'] if r[1].endswith('/summary')]
    assert summaries[-1][2]['start_date']=='2011-12-08'
    button(app,'应用').click();run(ui)
    assert '这个范围没有交易' in markup(app)
    assert '£0.00' in markup(app) and '—' in markup(app)
    assert len(app.dataframe)==0 and len(app.get('plotly_chart'))==0
    button(app,'清空').click();run(ui)
    assert app.date_input(key='view_overview_start').value==date(2011,12,8)
    assert len(app.get('plotly_chart'))==2
    before=len([r for r in ui['calls'] if r[1].endswith('/summary')])
    app.date_input(key='view_overview_start').set_value(date(2011,12,10))
    app.date_input(key='view_overview_end').set_value(date(2011,12,8))
    button(app,'应用').click();run(ui)
    assert any('开始日期不能晚于' in e.value for e in app.error)
    assert len([r for r in ui['calls'] if r[1].endswith('/summary')])==before


def test_outage_keeps_inputs_shows_request_id_and_never_reuses_results(ui):
    app=run(ui)
    app.date_input(key='view_overview_start').set_value(date(2011,12,9))
    ui['mode']['code']='DEPENDENCY_UNAVAILABLE'
    button(app,'应用').click();run(ui)
    assert app.date_input(key='view_overview_start').value==date(2011,12,9)
    assert any('暂时不可用' in e.value for e in app.error)
    assert any('12345678-1234-1234-1234-123456789abc' in e.value for e in app.caption)
    assert not app.dataframe and not app.get('plotly_chart')
    assert 'SECRET' not in markup(app) and 'SELECT' not in markup(app)
    ui['mode']['code']=None
    button(app,'重试查询').click();run(ui)
    assert not app.error and len(app.get('plotly_chart'))==2
    assert ui['app'].session_state['applied_overview']['start_date']==date(2011,12,9)


def test_current_change_is_not_automatic_and_explicit_switch_resets_ids(ui):
    app=run(ui)
    app=navigate(ui,'recommendations')
    app.text_input(key='view_recommendations_customer').set_value('draft-user')
    app.selectbox(key='view_recommendations_algorithm').set_value('NCF')
    button(app,'应用').click();run(ui)
    old=ui['api']['rid']
    new=another(ui['api']['manifest'])
    install(ui['api']['c'],new,ui['api']['directory'])
    app=navigate(ui,'sales')
    assert app.session_state['fixed_release']['release_id']==old
    assert len([r for r in ui['calls'] if r[1].endswith('/releases/current')])==1
    app=navigate(ui,'recommendations')
    assert app.text_input(key='view_recommendations_customer').value=='draft-user'
    button(app,'切换到当前版本').click();run(ui)
    assert app.session_state['fixed_release']['release_id']==new['release_id']
    assert app.text_input(key='view_recommendations_customer').value=='U'
    assert app.selectbox(key='view_recommendations_algorithm').value=='ALS'
    assert new['release_id'] in markup(app)
    calls=[r for r in ui['calls'] if '/customers/' in r[1] and r[1].endswith('/recommendations')]
    assert calls[-1][2]['release_id']==new['release_id']


def test_no_release_disables_business_but_experiments_and_tasks_still_work(ui):
    with ui['api']['c'].cursor() as q:q.execute('UPDATE current_release SET release_id=NULL')
    ui['api']['c'].commit()
    app=run(ui)
    assert '尚无可用数据版本' in markup(app)
    assert button(app,'应用').disabled
    for page in ('experiments','quality'):
        app=navigate(ui,page)
        assert not app.error and len(app.dataframe)>=1
    assert len([r for r in ui['calls'] if r[1].endswith('/releases/current')])==1
    assert all('release_id' not in params for _,path,params in ui['calls'])


def test_unknown_customer_four_algorithms_short_list_and_reason_text(ui):
    run(ui);app=navigate(ui,'recommendations')
    app.text_input(key='view_recommendations_customer').set_value('unknown')
    for algorithm in ('POP','ITEMCF','ALS','NCF'):
        app.selectbox(key='view_recommendations_algorithm').set_value(algorithm)
        button(app,'应用').click();run(ui)
        assert not app.error
        assert any('没有记录这个客户' in e.value for e in app.info)
        html=markup(app)
        assert '2 条推荐' in html and '可选未购商品 2 个' in html
        assert 'FULLFIT' in '\n'.join(e.value for e in app.caption)
        assert ('训练/服务历史热门商品' if algorithm=='POP' else '当前模型信息不足，使用热门商品补足') in html


def test_missing_product_and_existing_product_with_no_associations(ui):
    run(ui);app=navigate(ui,'associations')
    app.text_input(key='view_associations_stock').set_value('absent')
    button(app,'应用').click();run(ui)
    assert any('没有找到对应记录' in e.value for e in app.error)
    app.text_input(key='view_associations_stock').set_value('00002')
    button(app,'应用').click();run(ui)
    assert not app.error and '这个商品没有达到阈值的关联' in markup(app)
    assert not app.get('plotly_chart')


def test_customer_who_bought_entire_catalog_remains_an_empty_list(ui):
    with ui['api']['c'].cursor() as q:
        q.execute('INSERT INTO customer_seen_item VALUES (%s,%s,%s)',(ui['api']['rid'],'U','00002'))
        q.execute('DELETE FROM customer_recommendation WHERE release_id=%s',(ui['api']['rid'],))
    ui['api']['c'].commit()
    run(ui);app=navigate(ui,'recommendations')
    assert not app.error
    assert '没有可推荐的未购商品' in markup(app)
    assert '0 条推荐' in markup(app) and '可选未购商品 0 个' in markup(app)


def test_retired_version_requires_explicit_user_switch(ui):
    app=run(ui)
    old=ui['api']['rid'];new=another(ui['api']['manifest'])
    install(ui['api']['c'],new,ui['api']['directory'])
    with ui['api']['c'].cursor() as q:q.execute("UPDATE data_release SET status='RETIRED' WHERE release_id=%s",(old,))
    ui['api']['c'].commit()
    run(ui)
    assert any('版本已不可用' in e.value for e in app.error)
    assert app.session_state['fixed_release']['release_id']==old
    assert not app.get('plotly_chart')
    button(app,'切换到当前版本').click();run(ui)
    assert not app.error and app.session_state['fixed_release']['release_id']==new['release_id']


def test_invalid_benchmark_never_claims_speedup(ui):
    with ui['api']['c'].cursor() as q:q.execute("DELETE FROM gpu_benchmark WHERE device='CUDA' AND repeat_index=3")
    ui['api']['c'].commit()
    run(ui);app=navigate(ui,'experiments')
    assert any('对照尚未验收' in e.value for e in app.warning)
    assert '加速比' not in markup(app)


def test_money_state_reset_and_ui_credential_boundary():
    assert pounds('1234567890123456.785')=='£1,234,567,890,123,456.79'
    assert pounds(None)=='—' and percent('3.00','0.00')=='—'
    assert percent('3','21')=='14.29%'
    state={'nav_page':'customers','fixed_release':{'release_id':'old'},'view_customers_id':'U',
           'applied_recommendations':{'customer_id':'U'},'pager_sales_products':3,'lookup_countries':['UK']}
    activate_release(state,{'release_id':'new'})
    assert state=={'nav_page':'customers','fixed_release':{'release_id':'new'},'release_initialized':True}
    env=ui_environment('config/development.toml',{'RETAIL_DB_USER':'writer','RETAIL_DB_PASSWORD':'SECRET',
        'MYSQL_PWD':'SECRET','PATH':'/bin','RETAIL_API_URL':'http://127.0.0.1:8000/api/v1'})
    assert not any(k.startswith('RETAIL_DB_') for k in env) and 'MYSQL_PWD' not in env
    assert env['PATH']=='/bin' and env['RETAIL_UI_CONFIG']==str(ROOT/'config/development.toml')


def test_http_client_rejects_mixed_versions_and_error_body_secrets():
    release={'release_id':'old','batch_id':'B'}
    request_id=str(uuid4())
    def transport(request):
        assert request.method=='GET' and request.url.params['release_id']=='old'
        return httpx.Response(200,headers={'x-request-id':request_id},json={'data':{},'meta':{
            'schema_version':'1.0','request_id':request_id,'release_id':'new','batch_id':'B'}})
    with RetailAPI('http://127.0.0.1:8000/api/v1',release,transport=httpx.MockTransport(transport)) as client:
        with pytest.raises(ApiFault,match='VERSION_MISMATCH'):client.get('/summary')
        with pytest.raises(ValueError):client.get('/runs',release_id='old')
    def no_provenance(request):
        return httpx.Response(200,json={'data':{},'meta':{'schema_version':'1.0','request_id':None}})
    with RetailAPI('http://127.0.0.1:8000/api/v1',transport=httpx.MockTransport(no_provenance)) as client:
        with pytest.raises(ApiFault,match='INVALID_RESPONSE'):client.get('/runs')
    for url in ('http://example.com/api/v1','http://user:SECRET@127.0.0.1:8000/api/v1','http://127.0.0.1:8000/other'):
        with pytest.raises(ValueError):RetailAPI(url)


def test_pipeline_uses_latest_attempt_and_does_not_erase_history():
    runs=[dict(task_name='ingest',attempt=1,status='SUCCEEDED'),
          dict(task_name='eval_POP',attempt=1,status='FAILED'),dict(task_name='eval_POP',attempt=2,status='SUCCEEDED')]
    lanes=task_lanes(runs)
    assert lanes[0]['status']=='SUCCEEDED' and lanes[3]['status']=='SUCCEEDED'
    assert lanes[1]['status'] is None and len(runs)==3
    assert duration({'status':'SUCCEEDED','started_at':'2026-01-01T00:00:00Z','finished_at':None})=='—'
