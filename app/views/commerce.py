"""Overview and sales analysis. All metrics/rankings share applied filters."""
import streamlit as st

from app import charts
from app.components import (section,empty,sales_filters,date_params,sales_metrics,plot_and_table,
    table,product_rows,PRODUCT_COLUMNS,pager,h)
from app.state import pounds, count, percent

TREND_COLUMNS = {'period_start':'来源日期','gross_amount':'正向销售额 / GBP',
                 'return_amount':'退货金额 / GBP','net_amount':'销售净额 / GBP','order_count':'订单数'}


def trend_rows(points):
    return [dict(row,**{key:pounds(row[key]) for key in ('gross_amount','return_amount','net_amount')}) for row in points]


def overview(api, release):
    filters = sales_filters(api,release,'overview')
    if filters is None:return
    params = date_params(filters)
    summary = api.get('/summary',**params)
    points = api.get('/sales/trend',**params)['points']
    products = api.get('/products',**params,page_size=20,sort='GROSS_DESC')['items']
    sales_metrics(summary)
    if not summary['sale_quantity'] and not summary['return_quantity']:
        empty('这个范围没有交易','试着扩大来源日期范围，或清空国家筛选。')
        return
    with st.container(key='panel_overview_trend'):
        section('销售走势','正向销售、退货与净额使用同一来源日期。悬停查看金额。')
        a,b = st.columns([3.5,1.15],gap='large')
        with a:plot_and_table(charts.trend(points),trend_rows(points),TREND_COLUMNS,'overview_trend')
        with b:
            with st.container(key='overview_story'):
                st.html('<p class="story-title">这段时间的交易</p>'+
                    f'<div class="story-stat">{h(count(summary["sale_quantity"]))} 件</div><p class="story-note">正向销售数量。退货数量单独统计，不在这里相减。</p>'+
                    f'<div class="story-stat">{h(percent(summary["return_amount"],summary["gross_amount"]))}</div><p class="story-note">退货金额占正向销售额的比例。退货不一定能匹配同一时期的原订单。</p>')
    with st.container(key='panel_overview_products'):
        section('销售额最高的 20 个交易项','按正向销售额降序；同额按商品编码排序。包含运费等合法非商品交易项。')
        plot_and_table(charts.ranking(products),product_rows(products),PRODUCT_COLUMNS,'overview_products')


def sales(api, release):
    filters = sales_filters(api,release,'sales',extended=True)
    if filters is None:return
    params = date_params(filters)
    summary = api.get('/summary',**params)
    points = api.get('/sales/trend',**params,granularity=filters['granularity'])['points']
    countries = api.get('/sales/countries',**params,sort=filters['country_sort'],page=st.session_state.get('pager_sales_countries',1),page_size=20)
    products = api.get('/products',**params,sort=filters['sort'],page=st.session_state.get('pager_sales_products',1),page_size=20)
    sales_metrics(summary)
    with st.container(key='panel_sales_trend'):
        section('销售走势','按月时汇总到来源月份；金额单位为 GBP。')
        plot_and_table(charts.trend(points),trend_rows(points),TREND_COLUMNS,'sales_trend')
    with st.container(key='panel_sales_countries'):
        label = '销售净额' if filters['country_sort']=='NET_DESC' else '正向销售额'
        section('国家与地区',f'按{label}降序，每页最多 20 个。与上方使用相同的国家及日期条件。')
        rows = [dict(r,**{k:pounds(r[k]) for k in ('gross_amount','return_amount','net_amount')}) for r in countries['items']]
        chart = charts.ranking(countries['items'],country=True)
        if filters['country_sort']=='GROSS_DESC':
            chart.data[0].x = [float(r['gross_amount']) for r in countries['items'][::-1]]
            chart.data[0].customdata = [pounds(r['gross_amount']) for r in countries['items'][::-1]]
            chart.data[0].hovertemplate = '%{y}<br>正向销售额：%{customdata}<extra></extra>'
        plot_and_table(chart,rows,{'country':'国家或地区','gross_amount':'正向销售额 / GBP',
            'return_amount':'退货金额 / GBP','net_amount':'销售净额 / GBP','order_count':'订单数'},'sales_countries')
        pager(countries,'sales','countries')
    with st.container(key='panel_sales_products'):
        section('商品与交易项排行','商品订单数分别去重，不能把各商品订单数相加当作总订单数。')
        plot_and_table(charts.ranking(products['items'],quantity=filters['sort']=='QUANTITY_DESC'),
            product_rows(products['items']),PRODUCT_COLUMNS,'sales_products')
        pager(products,'sales','products')
