"""Source quality and actual task attempts, without job submission controls."""
import streamlit as st

from app.components import section,empty,table,metrics,apply_values,clear_callback,pager,h,seed
from app.state import STATUS,task_lanes,count,percent,duration

STATES = ['ALL','PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED']


def quality(api,release):
    bid = release['batch_id'] if release else ''
    if not bid and 'applied_quality' not in st.session_state:
        if 'lookup_default_batch' not in st.session_state:
            batches=api.get('/batches',page_size=1)['items']
            st.session_state['lookup_default_batch']=batches[0]['batch_id'] if batches else ''
        bid=st.session_state['lookup_default_batch']
    defaults=dict(batch_id=bid,run_id='',status=None)
    current=defaults | st.session_state.get('applied_quality',{})
    with st.form('quality_filters',border=False):
        a,b=st.columns([2,2])
        with a:batch_id=st.text_input('数据批次',key=seed('view_quality_batch',current['batch_id']),placeholder='输入 batch_id')
        with b:run_id=st.text_input('任务编号（可选）',key=seed('view_quality_run',current['run_id']),placeholder='输入 run_id 查看详情')
        a,b=st.columns([2,1.4])
        with a:status=st.selectbox('任务状态',STATES,format_func=lambda x:STATUS.get(x,'全部状态'),key=seed('view_quality_status',current['status'] or 'ALL'))
        with b:
            st.html('<div class="button-spacer"></div>')
            x,y=st.columns(2)
            with x:submitted=st.form_submit_button('应用',type='primary',use_container_width=True)
            with y:st.form_submit_button('清空',on_click=clear_callback,args=('quality',),use_container_width=True)
    values=apply_values('quality',defaults,submitted,dict(batch_id=batch_id,run_id=run_id,status=None if status=='ALL' else status))
    if not values['batch_id']:
        empty('尚无批次记录','数据导入后，可以在这里查看来源和质量；页面不提供导入或执行任务的操作。')
        return
    import re
    if not re.fullmatch(r'B[0-9]{14}[0-9a-f]{8}',values['batch_id']):
        st.error('数据批次应是 B 开头的完整 batch_id。')
        return
    bid=values['batch_id']
    report=api.get('/batches/'+bid+'/quality')
    source=next((row for row in api.all_items('/batches') if row['batch_id']==bid),None)
    all_runs=api.all_items('/runs',batch_id=bid)
    runs=api.get('/runs',batch_id=bid,status=values['status'],page=st.session_state.get('pager_quality_runs',1),page_size=20)
    facts={r['metric_name']:r['metric_value'] for r in report['metrics']}
    metrics([
        ('来源行数',count(facts.get('RAW_ROWS')),'来源记录均保留追溯'),
        ('正向交易',count(facts.get('SALE_ROWS')),'用于销售与推荐'),
        ('退货记录',count(facts.get('RETURN_ROWS')),'金额单独统计'),
        ('拒绝记录',count(facts.get('REJECTED_ROWS')),'保留原因，不静默删除'),
        ('疑似重复超额行',count(facts.get('DUPLICATE_EXCESS_ROWS')),'原交易保留，未直接去重'),
        ('批次状态',STATUS.get(report['status'],report['status']),'按数据库记录展示')])
    with st.container(key='panel_quality_classification'):
        section('每一行数据，去向清楚','正向、退货与拒绝三类应与来源行数守恒；warning 不参与相加。')
        if all(k in facts for k in ('RAW_ROWS','SALE_ROWS','RETURN_ROWS','REJECTED_ROWS')):
            parts=[];legend=[]
            for key,label,color in [('SALE_ROWS','正向交易','#3159D6'),('RETURN_ROWS','退货记录','#B66A16'),('REJECTED_ROWS','拒绝记录','#8797B1')]:
                ratio=facts[key]/facts['RAW_ROWS']*100 if facts['RAW_ROWS'] else 0
                if facts[key]:parts.append(f'<span style="flex:{ratio};background:{color}" title="{label} {facts[key]:,}"></span>')
                legend.append(f'<span><i style="background:{color}"></i>{label} {facts[key]:,}</span>')
            st.html('<div class="quality-band" aria-label="交易分类比例">'+''.join(parts)+'</div><div class="quality-legend">'+''.join(legend)+'</div>')
            if sum(facts[k] for k in ('SALE_ROWS','RETURN_ROWS','REJECTED_ROWS'))!=facts['RAW_ROWS']:
                st.error('分类计数未守恒，请检查数据质量报告。')
            st.caption('拒绝比例 '+percent(facts['REJECTED_ROWS'],facts['RAW_ROWS'])+'；正常发布阈值为 5%。')
        else:
            empty('分类统计尚未生成','任务运行或失败时只显示已经记录的指标，不将缺失统计填成 0。')
        with st.expander('全部质量指标：缺失、重复、拒绝与大篮子截断'):
            table(report['metrics'],{'metric_name':'指标','metric_value':'实际记录值','description':'说明'},key='quality_metrics')
        st.caption('质量报告：'+('已登记' if report['report_available'] else '未登记')+'。页面不读取或展示原始敏感日志。')
        if source:
            with st.expander('查看数据来源与批次依据'):
                table([source],{'batch_id':'批次','source_name':'来源文件','source_sha256':'源文件 SHA-256',
                    'policy_version':'清洗策略','raw_rows':'来源行数','created_at':'登记时间 / UTC','updated_at':'更新时间 / UTC'},key='source_evidence')
    with st.container(key='panel_quality_pipeline'):
        section('处理链','由实际任务的最新尝试汇总。历史失败尝试仍保留在下方任务明细中。')
        markup=[]
        for lane in task_lanes(all_runs):
            cls='done' if lane['status']=='SUCCEEDED' else 'failed' if lane['status']=='FAILED' else 'running' if lane['status']=='RUNNING' else ''
            markup.append(f'<div class="pipeline-step {cls}"><strong>{h(lane["name"])}</strong><span>{h(STATUS.get(lane["status"],"未记录"))}</span><small>{lane["tasks"]} 项任务</small></div>')
        st.html('<div class="pipeline">'+''.join(markup)+'</div>')
    with st.container(key='panel_quality_runs'):
        section('任务明细','运行审计时间为 UTC。状态来自已有记录；刷新查询不会启动或重试任务。')
        if not runs['items']:
            empty('没有匹配的任务','切换任务状态或检查批次编号。')
        else:
            rows=[dict(r,status=STATUS.get(r['status'],r['status']),elapsed=duration(r)) for r in runs['items']]
            table(rows,{'task_name':'任务','attempt':'尝试','status':'状态','elapsed':'耗时',
                'started_at':'开始时间 / UTC','finished_at':'结束时间 / UTC','application_id':'YARN 应用','error_code':'错误码','run_id':'任务编号'},key='quality_runs')
        pager(runs,'quality','runs')
    if values['run_id']:
        from uuid import UUID
        try:UUID(values['run_id'])
        except ValueError:
            st.error('任务编号应为有效 UUID。')
            return
        detail=api.get('/runs/'+values['run_id'])
        with st.container(key='panel_run_detail'):
            section('所选任务详情',detail['task_name'])
            if detail['batch_id']!=bid:st.info('这个任务属于另一个批次；上方质量统计仍属于所选批次。')
            table([dict(detail,status=STATUS.get(detail['status'],detail['status']),elapsed=duration(detail))],
                {'run_id':'任务编号','batch_id':'所属批次','parent_run_id':'父任务','status':'状态','attempt':'尝试',
                 'elapsed':'耗时','heartbeat_at':'最近心跳 / UTC','application_id':'YARN 应用编号','error_code':'错误码','error_message':'错误说明'},key='quality_run_detail')
            st.caption('日志：'+('已登记' if detail['log_available'] else '未登记')+'；产物清单：'+('已登记' if detail['output_manifest_path'] else '未登记'))
