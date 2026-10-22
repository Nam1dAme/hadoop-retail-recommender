"""Streamlit components and a small, scoped visual system."""
import base64
from datetime import date
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from app.client import ApiFault
from app.state import PAGES, SEGMENTS, count, pounds, clear_page

ROOT = Path(__file__).resolve().parent


def h(value):
    return escape(str(value), quote=True)


def install_style():
    css = (ROOT/'assets/theme.css').read_text()
    for name in ('plex', 'noto'):
        path = ROOT/'assets'/(name+'.woff2')
        if path.exists():
            css = css.replace('{{'+name+'}}',base64.b64encode(path.read_bytes()).decode())
    st.html('<style>'+css+'</style>')


def section(title, description=None):
    st.html(f'<div class="section-title"><h2>{h(title)}</h2>'+
            (f'<p>{h(description)}</p>' if description else '')+'</div>')


def empty(title, text):
    st.html(f'<div class="empty-state"><span class="empty-symbol" aria-hidden="true">∅</span><h3>{h(title)}</h3><p>{h(text)}</p></div>')


def fault(error, key):
    if error.code == 'RESULT_NOT_READY':
        st.info(error.message)
    else:
        st.error(error.message)
    st.caption('请求 ID：'+(error.request_id or '未取得（请求未完成或未到达服务）'))
    st.button('重试查询',key='retry_'+key,icon=':material/refresh:')


def table(rows, columns, *, height=None, key=None, select=False):
    # Monetary columns are already formatted decimal strings, never binary floats.
    frame = pd.DataFrame([{label:row.get(field) for field,label in columns.items()} for row in rows],columns=list(columns.values()))
    return st.dataframe(frame,use_container_width=True,hide_index=True,
        height=height or min(640,38+35*len(rows)),key=key,
        on_select='rerun' if select else 'ignore',selection_mode='single-row')


def plot(fig, key):
    st.plotly_chart(fig,use_container_width=True,theme=None,key=key,
        config={'displayModeBar':False,'responsive':True,'scrollZoom':False})


def plot_and_table(fig, rows, columns, key):
    if not rows:
        empty('没有匹配的数据','尝试调整查询条件；这里不会自动回退到全量数据。')
        return
    tabs = st.tabs(['图表','数据表'])
    with tabs[0]:plot(fig,key+'_chart')
    with tabs[1]:table(rows,columns,key=key+'_table')


def metrics(items):
    cells = ''.join(f'<div class="metric"><span>{h(label)}</span><strong>{h(value)}</strong><small>{h(note)}</small></div>'
                    for label,value,note in items)
    st.html('<div class="metric-strip">'+cells+'</div>')


def sales_metrics(data):
    metrics([('正向销售额',pounds(data['gross_amount']),'仅统计正向交易'),
             ('退货金额',pounds(data['return_amount']),'退货按正金额展示'),
             ('销售净额',pounds(data['net_amount']),'正向销售额减退货'),
             ('有效订单',count(data['order_count']),'正向订单去重'),
             ('有效客户',count(data['customer_count']),'正向且 CustomerID 有效'),
             ('客单价',pounds(data['average_order_value']),'正向销售额 ÷ 订单数')])


def clear_callback(page):
    clear_page(st.session_state,page)


def seed(key,value):
    """Keep widget identity stable when a newly applied default changes.

    In Streamlit 1.40, changing value/index recreates a keyed widget and can
    discard the next submitted edit. Seed state, then use constant defaults.
    """
    if key not in st.session_state:st.session_state[key]=value
    return key


def apply_values(page, defaults, submitted, values):
    if submitted:
        st.session_state['applied_'+page] = values
        for key in list(st.session_state):
            if key.startswith('pager_'+page+'_'):
                del st.session_state[key]
    return st.session_state.get('applied_'+page,defaults)


def sales_filters(api, release, page, *, extended=False):
    from app.state import sales_defaults
    defaults = sales_defaults(release)
    current = st.session_state.get('applied_'+page,defaults)
    if 'lookup_countries' not in st.session_state:
        st.session_state['lookup_countries']=sorted({r['country'] for r in api.all_items('/sales/countries',sort='NET_DESC')})
    # Immutable, fixed-version filter vocabulary only. Query results are never
    # reused across renders, and an outage still produces a visible query error.
    names = ['全部国家']+st.session_state['lookup_countries']
    with st.form(page+'_filters',border=False):
        a,b,c,d = st.columns([1.2,1.2,1.7,1.4],gap='medium',vertical_alignment='bottom')
        with a:start = st.date_input('开始日期',value=None,min_value=date(1000,1,1),max_value=date.max,format='YYYY/MM/DD',key=seed('view_'+page+'_start',current['start_date']))
        with b:end = st.date_input('结束日期',value=None,min_value=date(1000,1,1),max_value=date.max,format='YYYY/MM/DD',key=seed('view_'+page+'_end',current['end_date']))
        with c:country = st.selectbox('国家或地区',names,key=seed('view_'+page+'_country',current['country'] or names[0]))
        with d:
            x,y = st.columns([1,1])
            with x:submitted = st.form_submit_button('应用',type='primary',use_container_width=True)
            with y:st.form_submit_button('清空',on_click=clear_callback,args=(page,),use_container_width=True)
        values = dict(start_date=start,end_date=end,country=None if country==names[0] else country,granularity=current['granularity'],
                      sort=current['sort'],country_sort=current['country_sort'])
        if extended:
            a,b,c = st.columns([1,1.5,1.5])
            with a:values['granularity'] = st.selectbox('趋势粒度',['DAY','MONTH'],
                format_func=lambda x:'按日' if x=='DAY' else '按月',key=seed('view_'+page+'_granularity',current['granularity']))
            with b:values['sort'] = st.selectbox('商品排序',['GROSS_DESC','QUANTITY_DESC'],
                format_func=lambda x:'正向销售额从高到低' if x=='GROSS_DESC' else '正向销量从高到低',key=seed('view_'+page+'_sort',current['sort']))
            with c:values['country_sort'] = st.selectbox('国家排序',['NET_DESC','GROSS_DESC'],
                format_func=lambda x:'销售净额从高到低' if x=='NET_DESC' else '正向销售额从高到低',key=seed('view_'+page+'_csort',current['country_sort']))
    if submitted and (not start or not end):
        st.session_state['validation_'+page] = '请选择完整的开始日期与结束日期。'
    elif submitted and start > end:
        st.session_state['validation_'+page] = '开始日期不能晚于结束日期，请修改后重新应用。'
    elif submitted:
        st.session_state.pop('validation_'+page,None)
    if st.session_state.get('validation_'+page):
        st.error(st.session_state['validation_'+page])
        return None
    return apply_values(page,defaults,submitted,values)


def date_params(values):
    return {k:(v.isoformat() if isinstance(v,date) else v) for k,v in values.items() if k in ('start_date','end_date','country')}


def set_page(key, value):
    st.session_state[key] = value


def pager(data, page, kind):
    key = f'pager_{page}_{kind}'
    n,total,size = data['page'],data['total'],data['page_size']
    a,b,c = st.columns([1,2,1])
    with a:st.button('上一页',key=key+'_prev',disabled=n<=1,on_click=set_page,args=(key,n-1),use_container_width=True)
    with b:st.caption(f"第 {n} / {max(1,(total+size-1)//size)} 页，共 {total:,} 条记录")
    with c:st.button('下一页',key=key+'_next',disabled=not data['has_next'],on_click=set_page,args=(key,n+1),use_container_width=True)


def go_to(page, values):
    clear_page(st.session_state,page)
    st.session_state['applied_'+page] = values
    st.session_state['nav_page'] = page
    st.query_params['page'] = page


def product_rows(rows):
    return [dict(row,gross_amount=pounds(row['gross_amount']),return_amount=pounds(row['return_amount']),
                 net_amount=pounds(row['net_amount']),description=row['description'] or row['stock_code'],
                 merchandise='商品' if row['is_merchandise'] else '其他交易项') for row in rows]


PRODUCT_COLUMNS = {'stock_code':'商品编码','description':'商品描述','gross_amount':'正向销售额 / GBP',
                   'return_amount':'退货金额 / GBP','net_amount':'销售净额 / GBP','sale_quantity':'正向销量',
                   'return_quantity':'退货数量','order_count':'订单数','merchandise':'类型'}
