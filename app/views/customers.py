"""RFM, co-purchases and stored customer recommendations."""
import streamlit as st

from app import charts
from app.client import identifier
from app.components import (section,empty,table,plot_and_table,plot,metrics,apply_values,
    clear_callback,pager,go_to,h,seed)
from app.state import SEGMENTS,ALGORITHMS,REASONS,pounds,count


def customers(api, release):
    defaults = dict(segment=None,customer_id='')
    current = defaults | st.session_state.get('applied_customers',{})
    with st.form('customer_filters',border=False):
        a,b,c = st.columns([1.3,1.8,1.4],vertical_alignment='bottom')
        with a:segment = st.selectbox('客户分层',['ALL',*SEGMENTS],
            format_func=lambda x:SEGMENTS.get(x,'全部客户分层'),key=seed('view_customers_segment',current['segment'] or 'ALL'))
        with b:customer_id = st.text_input('客户编号',placeholder='输入公开数据中的 CustomerID',key=seed('view_customers_id',current['customer_id']),max_chars=32)
        with c:
            x,y = st.columns(2)
            with x:submitted = st.form_submit_button('应用',type='primary',use_container_width=True)
            with y:st.form_submit_button('清空',on_click=clear_callback,args=('customers',),use_container_width=True)
    values = apply_values('customers',defaults,submitted,dict(segment=None if segment=='ALL' else segment,customer_id=customer_id))
    data = api.get('/customers',segment=values['segment'],customer_id=values['customer_id'] or None,
        page=st.session_state.get('pager_customers_rows',1),page_size=20)
    segments = api.get('/customers/segments')
    with st.container(key='panel_customer_segments'):
        section('五类客户，一份全历史快照',f"RFM 基准日：{segments['as_of_date'] or '尚未记录'}。不随销售页的日期与国家条件重算。")
        plot_and_table(charts.segments(segments['items']),
            [dict(row,segment=SEGMENTS[row['segment']]) for row in segments['items']],
            {'segment':'客户分层','customer_count':'客户数'},'customer_segments')
    with st.container(key='panel_customer_rows'):
        section('客户明细','客户编号是公开数据标识，不代表本系统内的个人档案。R 越小表示最近购买越近。')
        if not data['items']:
            empty('没有匹配的客户','修改编号或分层，或清空筛选条件。')
        else:
            rows = [dict(r,monetary=pounds(r['monetary']),segment=SEGMENTS[r['segment']]) for r in data['items']]
            event = table(rows,{'customer_id':'客户编号','segment':'分层','last_purchase':'最后购买日期',
                'recency_days':'R / 间隔天数','frequency':'F / 订单数','monetary':'M / 正向消费 GBP',
                'r_score':'R 分','f_score':'F 分','m_score':'M 分'},key='selection_customers_rows',select=True)
            selection = event.selection.rows
            selected = data['items'][selection[0]]['customer_id'] if selection and selection[0] < len(data['items']) else None
            st.button('查看所选客户推荐',key='customers_open_recs',disabled=selected is None,
                on_click=go_to,args=('recommendations',dict(customer_id=selected)),icon=':material/playlist_add_check:')
        pager(data,'customers','rows')


def associations(api, release):
    current = st.session_state.get('applied_associations')
    if current is None:
        if 'lookup_default_stock' not in st.session_state:
            products = api.get('/products',page_size=20)['items']
            st.session_state['lookup_default_stock']=next((p['stock_code'] for p in products if p['is_merchandise']),products[0]['stock_code'] if products else '')
        candidate=st.session_state['lookup_default_stock']
        current = dict(stock_code=candidate,k=10)
    defaults = dict(stock_code='',k=10)
    with st.form('association_filters',border=False):
        a,b,c = st.columns([2,1,1.4],vertical_alignment='bottom')
        with a:stock = st.text_input('商品编码',placeholder='例如来源数据中的 StockCode',key=seed('view_associations_stock',current['stock_code']),max_chars=32)
        with b:k = st.number_input('关联数量 K',min_value=1,max_value=10,step=1,key=seed('view_associations_k',current['k']))
        with c:
            x,y = st.columns(2)
            with x:submitted = st.form_submit_button('应用',type='primary',use_container_width=True)
            with y:st.form_submit_button('清空',on_click=clear_callback,args=('associations',),use_container_width=True)
    values = apply_values('associations',current,submitted,dict(stock_code=stock,k=k))
    if not values['stock_code']:
        empty('输入商品编码','查询商品在完整历史订单中的共购关联。')
        return
    data = api.get('/products/'+identifier(values['stock_code'])+'/similar',k=values['k'])
    st.html('<div class="recommendation-head"><div><strong>'+h(data['description'] or data['stock_code'])+
        '</strong><p>商品编码 '+h(data['stock_code'])+'</p></div><div><strong>'+str(len(data['items']))+
        ' 个关联</strong><p>完整历史共购；不应用销售日期筛选</p></div></div>')
    if not data['items']:
        empty('这个商品没有达到阈值的关联','商品存在，但没有满足共现次数与相似度规则的关联项。可查询其他商品。')
        return
    with st.container(key='panel_associations'):
        section('哪些商品经常一起购买','中心是当前商品，连线连接实际关联项；关系不等同于客户推荐。')
        tabs = st.tabs(['关系图','数据表'])
        with tabs[0]:plot(charts.associations(data),'association_chart')
        with tabs[1]:table(data['items'],{'rank':'排名','related_stock_code':'关联商品编码','description':'商品描述',
            'similarity':'相似度','co_count':'共现订单数','confidence':'置信度','lift':'提升度'},key='association_table')
        st.caption('共现按同一订单内商品去重计算。相似度越大表示共购关系越强；不是购买概率。')


def recommendations(api, release):
    defaults = dict(customer_id='',algorithm=release['champion_algorithm'],k=10)
    current = defaults | st.session_state.get('applied_recommendations',{})
    if not current['customer_id'] and 'applied_recommendations' not in st.session_state:
        if 'lookup_default_customer' not in st.session_state:
            users = api.get('/customers',page_size=1)['items']
            st.session_state['lookup_default_customer']=users[0]['customer_id'] if users else ''
        current['customer_id'] = st.session_state['lookup_default_customer']
    with st.form('recommendation_filters',border=False):
        a,b,c,d = st.columns([1.5,1.1,.8,1.4],vertical_alignment='bottom')
        with a:customer = st.text_input('客户编号',placeholder='输入 CustomerID',max_chars=32,key=seed('view_recommendations_customer',current['customer_id']))
        with b:algorithm = st.selectbox('推荐算法',ALGORITHMS,key=seed('view_recommendations_algorithm',current['algorithm']))
        with c:k = st.number_input('推荐数量 K',min_value=1,max_value=10,step=1,key=seed('view_recommendations_k',current['k']))
        with d:
            x,y = st.columns(2)
            with x:submitted = st.form_submit_button('应用',type='primary',use_container_width=True)
            with y:st.form_submit_button('清空',on_click=clear_callback,args=('recommendations',),use_container_width=True)
    values = apply_values('recommendations',current,submitted,dict(customer_id=customer,algorithm=algorithm,k=k))
    if not values['customer_id']:
        empty('输入客户编号','选择一种算法，查看已经计算好的未购商品推荐。')
        return
    data = api.get('/customers/'+identifier(values['customer_id'])+'/recommendations',algorithm=values['algorithm'],k=values['k'])
    st.html(f'<div class="recommendation-head"><div><strong>客户 {h(data["customer_id"])}</strong>'+
        f'<p>当前算法 {h(data["algorithm"])}；默认算法 {h(release["champion_algorithm"])}</p></div><div><strong>'+h(count(data['returned_count']))+
        f' 条推荐</strong><p>目标 {values["k"]} 条 / 可选未购商品 {h(count(data["eligible_candidates"]))} 个</p></div></div>')
    if not data['known_customer']:
        st.info('没有记录这个客户，以下使用热门商品推荐；没有创建或推测客户画像。')
    elif data['fallback_used']:
        st.info('个性化结果不足，已在结果末尾补入未购买的热门商品，并逐项标注。')
    st.caption('服务模型 FULLFIT · 使用完整历史生成；切换算法只读取已有结果，不重新训练。')
    if not data['items']:
        empty('没有可推荐的未购商品','当前候选目录已耗尽，或客户已购买全部候选商品；不会补入已购或重复项。')
    else:
        with st.container(key='panel_recommendations'):
            section('为这个客户排列的商品','顺序由各算法生成。分数只在当前算法内用于排序，不是购买概率。')
            tabs = st.tabs(['推荐清单','数据表'])
            with tabs[0]:
                rows = ''
                for item in data['items']:
                    rows += (f'<li><span class="rec-rank">{item["rank"]:02d}</span><div><p class="rec-name">'+h(item['description'] or item['stock_code'])+
                        '</p><p class="rec-meta">商品编码 '+h(item['stock_code'])+'</p><span class="rec-reason'+(' fallback' if item['is_fallback'] else '')+'">'+
                        h(REASONS[item['reason_code']])+'</span></div><div class="rec-score">'+h(f'{item["score"]:.6g}')+'<small>模型分数</small></div></li>')
                st.html('<ol class="rec-list">'+rows+'</ol>')
            with tabs[1]:
                table([dict(r,reason_text=REASONS[r['reason_code']],is_fallback='是' if r['is_fallback'] else '否') for r in data['items']],
                    {'rank':'排名','stock_code':'商品编码','description':'商品描述','score':'本算法分数','reason_text':'推荐原因','is_fallback':'热门补足'},key='recs_table')
    with st.expander('查看模型与数据依据'):
        st.write('服务模型编号：'+data['model_run_id'])
        st.write('固定版本：'+release['release_id'])
        st.caption('离线质量指标属于对应实验的 EVAL 模型，不能把这里的 FULLFIT 结果当作未来预测验证。')
