#!/usr/bin/env python3
"""T16 browser audit of the real local Streamlit UI and W09 API (GET only).

Requires the locked dev Playwright dependency and an installed Chromium headless
shell. Screenshots stay under var by default; no fixtures or DB writes are used.
"""
import argparse
from datetime import datetime,timezone
from decimal import Decimal
import hashlib
import importlib.metadata
import json
from pathlib import Path
import subprocess
import platform

import httpx
from playwright.sync_api import sync_playwright

from orchestration.settings import load_config

PAGES={'overview':'总览','sales':'销售分析','customers':'客户分层','associations':'商品关联',
       'recommendations':'客户推荐','experiments':'模型实验','quality':'数据质量与任务'}
ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--screenshots',type=Path,default=ROOT/'var/w10/screenshots')
    args=parser.parse_args()
    args.screenshots=args.screenshots.resolve()
    config=load_config(args.config)
    address=f"http://{config['ui']['host']}:{config['ui']['port']}"
    api=config['api']['url']
    with httpx.Client(timeout=6,trust_env=False) as http:
        response=http.get(api+'/releases/current');response.raise_for_status()
        release=response.json()['data']
        summary=http.get(api+'/summary',params={'release_id':release['release_id']}).json()['data']
        sample_customer=http.get(api+'/customers',params={'release_id':release['release_id'],'page_size':1}).json()['data']['items'][0]['customer_id']
        all_runs=http.get(api+'/runs',params={'batch_id':release['batch_id'],'page_size':100}).json()['data']['items']
        sample_run=next(r['run_id'] for r in all_runs if r['application_id'])
        batches=http.get(api+'/batches',params={'page_size':100}).json()['data']['items']
        source_sha256=next(r['source_sha256'] for r in batches if r['batch_id']==release['batch_id'])
    checks=[];screenshots=[];js_errors=[];external=[];layouts=[]
    def check(label,condition):
        assert condition,label
        checks.append(label)
    args.screenshots.mkdir(parents=True,exist_ok=True)
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True,args=['--disable-gpu'])
        browser_version=browser.version

        def opened(width,height=1080):
            page=browser.new_page(viewport={'width':width,'height':height},device_scale_factor=1,reduced_motion='reduce')
            page.on('pageerror',lambda exc:js_errors.append(str(exc)))
            page.on('request',lambda req:external.append(req.url) if req.url.startswith(('http://','https://')) and not req.url.startswith(address) else None)
            page.goto(address+'/?page=overview',wait_until='domcontentloaded')
            ready(page,'overview')
            return page

        def ready(page,slug,previous=None):
            page.wait_for_function('''([slug,previous])=>{
                const node=document.querySelector('[data-retail-view="'+slug+'"]');
                return node && node.dataset.render && node.dataset.render!==previous;
            }''',arg=[slug,previous],timeout=45000)
            page.evaluate('document.fonts.ready')
            page.evaluate('new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
            marker=page.locator('[data-retail-view="'+slug+'"]')
            check(f'{slug} fixed release at {page.viewport_size["width"]}',marker.get_attribute('data-release')==release['release_id'])
            check(f'{slug} no uncaught Streamlit error',page.get_by_test_id('stException').count()==0)
            check(f'{slug} no generic error',page.get_by_text('页面未能完成展示，请保留查询条件并重试。',exact=True).count()==0)
            return marker.get_attribute('data-render')

        def sidebar(page,open_it):
            target=page.get_by_test_id('stSidebarCollapsedControl') if open_it else page.get_by_test_id('stSidebarCollapseButton')
            # Streamlit keeps the collapsed sidebar mounted off-screen, where
            # is_visible() alone is true. Only click a control inside the viewport.
            box=target.bounding_box() if target.count() else None
            if box and 0 <= box['x']+box['width']/2 <= page.viewport_size['width']:
                target.get_by_role('button').click()

        def nav(page,slug):
            if page.viewport_size['width']<768:sidebar(page,True)
            page.get_by_test_id('stSidebar').get_by_text(PAGES[slug],exact=True).click()
            ready(page,slug)
            if page.viewport_size['width']<768:sidebar(page,False)
            page.locator('[data-testid="stMain"]').evaluate('(el)=>el.scrollTop=0')

        def action(page,slug,callback):
            previous=page.locator('[data-retail-view="'+slug+'"]') .get_attribute('data-render')
            callback()
            ready(page,slug,previous)

        def apply(page,slug):
            action(page,slug,lambda:page.get_by_test_id('stForm').get_by_role('button',name='应用',exact=True).click())

        def select(page,label,value):
            combo=page.get_by_test_id('stSelectbox').filter(has=page.get_by_text(label,exact=True)).get_by_role('combobox')
            combo.click()
            combo.fill(value) # The native menu virtualizes options beyond its first ten rows.
            page.get_by_role('option',name=value,exact=True).click()

        def snapshot(page,slug,width):
            # Sidebar state changes before Plotly's ResizeObserver updates the
            # charts. Wait for the same overflow condition we assert below,
            # rather than sampling a transient stale chart width. Persistent
            # overflow still fails after this bounded wait.
            page.wait_for_function('''()=>{
                const main=document.querySelector('[data-testid="stMain"]');
                return main && main.scrollWidth<=main.clientWidth+1;
            }''',timeout=5000)
            check(f'{slug} outer responsive {width}',page.evaluate('document.documentElement.scrollWidth<=innerWidth+1'))
            check(f'{slug} main responsive {width}',page.locator('[data-testid="stMain"]').evaluate('(el)=>el.scrollWidth<=el.clientWidth+1'))
            geometry=page.evaluate('''()=>{
                const box=el=>{const r=el.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height,bottom:r.bottom}};
                const side=document.querySelector('[data-testid="stSidebar"]');
                const main=document.querySelector('[data-testid="stMain"]');
                const content=document.querySelector('[data-testid="stMainBlockContainer"]');
                const forms=[...document.querySelectorAll('[data-testid="stForm"]')].map(form=>{
                    const buttons=[...form.querySelectorAll('button')];
                    const apply=buttons.find(el=>el.textContent.trim()==='应用');
                    const clear=buttons.find(el=>el.textContent.trim()==='清空');
                    if(!apply||!clear)return null;
                    const inner=apply.closest('[data-testid="stColumn"]');
                    const group=inner?.parentElement.closest('[data-testid="stColumn"]');
                    const row=group?.closest('[data-testid="stHorizontalBlock"]');
                    const peers=row?[...row.querySelectorAll('[data-baseweb="select"],[data-baseweb="input"]')].map(box):[];
                    return {apply:box(apply),clear:box(clear),peers};
                }).filter(Boolean);
                return {main:box(main),content:box(content),sidebar:box(side),expanded:side.getAttribute('aria-expanded')==='true',forms};
            }''')
            layouts.append(dict(page=slug,viewport_width=width,**geometry))
            main,content=geometry['main'],geometry['content']
            check(f'{slug} content centered {width}',abs(content['x']+content['width']/2-main['x']-main['width']/2)<=1)
            if not geometry['expanded']:
                check(f'{slug} collapsed sidebar releases width {width}',geometry['sidebar']['width']<=1 and main['x']<=1 and abs(main['width']-width)<=1)
            for form in geometry['forms']:
                apply_box,clear_box=form['apply'],form['clear']
                check(f'{slug} action buttons align {width}',abs(apply_box['bottom']-clear_box['bottom'])<=1 and abs(apply_box['height']-clear_box['height'])<=1)
                peers=[r for r in form['peers'] if width>=768 or abs(r['y']-apply_box['y'])<40]
                for peer in peers:
                    check(f'{slug} actions align with inputs {width}',abs(peer['bottom']-apply_box['bottom'])<=1 and abs(peer['height']-apply_box['height'])<=1)
            path=args.screenshots/f'{slug}-{width}.png'
            page.screenshot(path=str(path),full_page=False)
            recorded=path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
            screenshots.append(dict(page=slug,width=width,path=str(recorded),sha256=hashlib.sha256(path.read_bytes()).hexdigest()))

        page=opened(1440)
        expected=['£'+format(Decimal(summary[key]),',.2f') for key in ('gross_amount','return_amount','net_amount')]
        visible=page.locator('.metric-strip .metric strong').all_text_contents()
        check('real API money appears exactly',visible[:3]==expected)
        check('exactly seven sidebar choices',page.get_by_test_id('stSidebar').get_by_role('radio').count()==7)
        check('no auto-discovered implementation navigation',page.get_by_test_id('stSidebarNav').count()==0)
        loaded=page.evaluate('Array.from(document.fonts).filter(f=>f.status==="loaded").map(f=>f.family)')
        check('self-hosted Chinese and numeric fonts loaded','Retail Chinese' in loaded and 'Retail Numerals' in loaded)
        for slug in PAGES:
            if slug!='overview':nav(page,slug)
            snapshot(page,slug,1440)
            if slug=='experiments':
                check('EVAL and FULLFIT are separated',page.get_by_text('页面实际使用的服务模型',exact=True).count()==1)
                check('GPU slower is stated',page.get_by_text('这个规模下 GPU 用时更长。',exact=False).count()>=1)
                check('four real charts present',page.get_by_test_id('stPlotlyChart').count()==4)
            if slug=='quality':
                check('five task lanes',page.locator('.pipeline-step').count()==5)

        nav(page,'overview')
        old=page.locator('[data-retail-view="overview"]').get_attribute('data-render')
        dates=page.get_by_test_id('stDateInput').locator('input')
        dates.nth(0).fill('2026/01/01');dates.nth(0).press('Tab')
        dates.nth(1).fill('2026/01/02');dates.nth(1).press('Tab')
        check('draft does not change metrics',page.locator('.metric-strip .metric strong').all_text_contents()[:3]==expected)
        check('draft does not trigger a render',page.locator('[data-retail-view="overview"]').get_attribute('data-render')==old)
        apply(page,'overview')
        check('out of source range is an honest empty result',page.get_by_text('这个范围没有交易',exact=True).count()==1)
        check('empty money is zero and AOV unknown',page.locator('.metric-strip .metric strong').all_text_contents()==['£0.00','£0.00','£0.00','0','0','—'])
        action(page,'overview',lambda:page.get_by_role('button',name='清空',exact=True).click())
        check('clear restores full range',page.locator('.metric-strip .metric strong').all_text_contents()[:3]==expected)

        nav(page,'sales')
        select(page,'国家或地区','France')
        select(page,'趋势粒度','按月')
        apply(page,'sales')
        check('country query and month chart applied',len(page.get_by_test_id('stPlotlyChart').all())==3)
        country_labels=page.get_by_test_id('stPlotlyChart').nth(1).locator('.js-plotly-plot').evaluate('(el)=>el.data[0].y')
        check('country ranking respects the same country filter',country_labels==['France'])
        action(page,'sales',lambda:page.get_by_role('button',name='下一页',exact=True).last.click())
        check('product pagination advances',page.get_by_text('第 2 /',exact=False).count()>=1)
        nav(page,'customers')
        action(page,'customers',lambda:page.get_by_role('button',name='下一页',exact=True).click())
        check('customer pagination advances',page.get_by_text('第 2 /',exact=False).count()==1)
        page.get_by_label('客户编号',exact=True).fill(sample_customer)
        apply(page,'customers')
        check('RFM customer search returns one record',page.get_by_text('共 1 条记录',exact=False).count()==1)
        nav(page,'quality')
        page.get_by_label('任务编号（可选）',exact=True).fill(sample_run)
        apply(page,'quality')
        check('actual YARN task detail visible',page.get_by_text('所选任务详情',exact=True).count()==1)
        nav(page,'recommendations')
        page.get_by_label('客户编号',exact=True).fill('__W10_UNKNOWN_CUSTOMER__')
        select(page,'推荐算法','NCF')
        apply(page,'recommendations')
        check('unknown customer is identified',page.get_by_text('没有记录这个客户',exact=False).count()==1)
        check('fallback reasons are visible',page.locator('.rec-reason.fallback').count()==10)
        check('ten real eligible recommendations',page.locator('.rec-list li').count()==10)
        action(page,'recommendations',lambda:page.get_by_role('button',name='切换到当前版本',exact=False).click())
        check('explicit version switch clears customer choice',page.get_by_label('客户编号',exact=True).input_value()!='__W10_UNKNOWN_CUSTOMER__')
        # Native controls remain reachable with visible keyboard focus.
        target=page.get_by_role('button',name='应用',exact=True)
        page.keyboard.press('Tab')
        target.focus()
        check('keyboard focus is visible',target.evaluate('(el)=>getComputedStyle(el).outlineStyle')!='none')
        page.close()

        for width in (1440,1920):
            page=opened(width)
            snapshot(page,'overview-expanded',width)
            sidebar(page,False)
            page.wait_for_function('document.querySelector(\'[data-testid="stSidebar"]\').getAttribute("aria-expanded")==="false"')
            snapshot(page,'overview-collapsed',width)
            sidebar(page,True)
            page.wait_for_function('document.querySelector(\'[data-testid="stSidebar"]\').getAttribute("aria-expanded")==="true"')
            snapshot(page,'overview-restored',width)
            page.close()

        for width in (1024,768,360):
            page=opened(width,1080)
            if width<768:sidebar(page,False)
            snapshot(page,'overview',width)
            page.close()
        page=opened(390,1080)
        sidebar(page,False)
        for slug in PAGES:
            if slug!='overview':nav(page,slug)
            snapshot(page,slug,390)
        page.close()
        browser.close()

    check('no browser JavaScript failures',not js_errors)
    check('no external runtime HTTP assets',not external)
    report=dict(status='PASS',test_id='T-16',baseline_version='HL-DOC-1.0.0',
        observed_at=datetime.now(timezone.utc).isoformat().replace('+00:00','Z'),
        code_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        tracked_code_dirty=bool(subprocess.check_output(['git','diff','HEAD','--name-only'],cwd=ROOT,text=True).strip()),
        implementation_untracked=[name for name in subprocess.check_output(['git','ls-files','--others','--exclude-standard'],cwd=ROOT,text=True).splitlines()
            if name.startswith(('app/','scripts/','tests/')) and not name.startswith('app/demo/')],
        release_id=release['release_id'],batch_id=release['batch_id'],experiment_id=release['experiment_id'],
        config_sha256=hashlib.sha256(args.config.read_bytes()).hexdigest(),
        lock_sha256=hashlib.sha256((ROOT/'uv.lock').read_bytes()).hexdigest(),
        source_sha256=source_sha256,
        browser=dict(version=browser_version,engine='Chromium headless',gpu_disabled=True),
        environment=dict(python=platform.python_version(),packages={n:importlib.metadata.version(n) for n in ('streamlit','plotly','pandas','pyarrow','numpy','httpx','playwright')}),
        checks=checks,check_count=len(checks),screenshots=screenshots,layouts=layouts,javascript_errors=js_errors,
        scope='Real loopback Streamlit and W09 API. No database writes, no model training, no fixture substitution. Lifecycle/outage cases are covered separately by isolated AppTest integration tests.')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('status','test_id','check_count','release_id')},ensure_ascii=False))


if __name__=='__main__':main()
