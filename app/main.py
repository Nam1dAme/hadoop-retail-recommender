"""Chinese Streamlit application. Configuration is supplied by the launcher."""
import logging
import os
from pathlib import Path
from uuid import uuid4

import streamlit as st

from orchestration.settings import load_config, ConfigError
from app.client import RetailAPI,ApiFault
from app.components import install_style,h,fault,empty
from app.state import PAGES,activate_release
from app.views.commerce import overview,sales
from app.views.customers import customers,associations,recommendations
from app.views.experiments import experiments
from app.views.quality import quality


def switch_requested():
    st.session_state['switch_requested'] = True


def navigation_changed():
    st.query_params['page'] = st.session_state['nav_page']


def main():
    st.set_page_config(page_title='零售观察 · Retail Lab',page_icon='◫',layout='wide',initial_sidebar_state='auto')
    install_style()
    config_path=os.environ.get('RETAIL_UI_CONFIG')
    try:
        if not config_path:raise ConfigError('Explicit UI config is required')
        config=load_config(Path(config_path))
    except ConfigError:
        st.error('缺少或无法读取显式配置，请使用 python -m app --config 启动页面。')
        st.stop()
    if 'nav_page' not in st.session_state:
        requested=st.query_params.get('page','overview')
        st.session_state['nav_page']=requested if requested in PAGES else 'overview'
    with st.sidebar:
        st.html('<div class="brand"><div class="brand-mark" aria-hidden="true"><i></i><i></i><i></i></div><div><strong>零售观察</strong><p>Retail Lab</p></div></div>')
        with st.container(key='navigation'):
            st.radio('页面',list(PAGES),format_func=lambda x:PAGES[x][0],key='nav_page',label_visibility='collapsed',on_change=navigation_changed)
        st.html('<div class="sidebar-source"><strong>从交易到洞察</strong><p>公开交易数据的销售分析<br>与推荐实验工作台</p>'+
            '<p><a href="https://archive.ics.uci.edu/dataset/352/online+retail" target="_blank" rel="noreferrer">UCI Online Retail</a><br>CC BY 4.0 · 历史交易快照</p></div>')
    page=st.session_state['nav_page']
    with RetailAPI(config['api']['url'],st.session_state.get('fixed_release')) as api:
        if not st.session_state.get('release_initialized') or st.session_state.pop('switch_requested',False):
            try:
                with st.spinner('正在读取数据版本…'):
                    release=api.get('/releases/current')
                activate_release(st.session_state,release)
                st.session_state['release_notice']='已固定到当前可用版本。'
            except ApiFault as exc:
                st.session_state['release_initialized']=True
                st.session_state['release_fault']=exc
        release=st.session_state.get('fixed_release')
        api.release=release
        a,b=st.columns([4,1.2],vertical_alignment='center')
        with a:
            st.html(f'<div class="page-crumb">工作台 / {h(PAGES[page][0])}</div><div class="page-heading"><h1>{h(PAGES[page][1])}</h1><p>{h(PAGES[page][2])}</p></div>')
        with b:st.button('切换到当前版本',key='switch_current',on_click=switch_requested,use_container_width=True,icon=':material/sync:')
        if release:
            st.html('<div class="context-bar"><span class="context-status"><i class="context-dot"></i>数据版本已固定</span>'+
                f'<span>版本 <b>{h(release["release_id"])}</b></span><span>批次 <b>{h(release["batch_id"])}</b></span>'+
                f'<span>{h(release["source_start"] or "—")} 至 {h(release["source_end"] or "—")}</span>'+
                f'<span>GBP 英镑</span><span>来源当地时间 source_local</span><span>默认推荐 <b>{h(release["champion_algorithm"])}</b></span></div>')
        else:
            st.html('<div class="context-bar"><span>尚未固定可用版本</span><span>GBP 英镑</span><span>来源当地时间 source_local</span></div>')
        if st.session_state.pop('release_notice',None):
            # A brief toast communicates a user-triggered version switch without cluttering the dashboard.
            if st.session_state.get('version_seen'):
                st.toast('已固定到当前可用版本。')
            st.session_state['version_seen']=True
        if st.session_state.get('release_fault'):
            error=st.session_state['release_fault']
            if error.code!='NO_ACTIVE_RELEASE':
                st.error(error.message)
                st.caption('请求 ID：'+(error.request_id or '未取得（连接未完成）'))
                st.button('重试版本查询',key='retry_version',on_click=switch_requested)
        if not release and page not in ('experiments','quality'):
            with st.form('no_release',border=False):
                st.text_input('查询条件',value='',placeholder='可用版本发布后启用',disabled=True)
                st.form_submit_button('应用',disabled=True)
            empty('尚无可用数据版本','数据发布完成后，点击“切换到当前版本”。模型实验和数据质量与任务仍可独立查看。')
        else:
            try:
                with st.spinner('正在读取数据…'):
                    {'overview':overview,'sales':sales,'customers':customers,'associations':associations,
                     'recommendations':recommendations,'experiments':experiments,'quality':quality}[page](api,release)
            except ApiFault as exc:fault(exc,page)
            except ValueError:
                st.error('输入格式不符合要求，请检查编号、日期与数值后重新应用。')
            except Exception as exc:
                logging.getLogger('retail.ui').error('page=%s exception_type=%s',page,type(exc).__name__)
                st.error('页面未能完成展示，请保留查询条件并重试。')
                st.button('重试页面',key='retry_page')
        st.html(f'<span data-retail-view="{h(page)}" data-release="{h(release["release_id"] if release else "")}" data-render="{uuid4()}" aria-hidden="true"></span>')


if __name__=='__main__':main()
