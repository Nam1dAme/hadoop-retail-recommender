"""Read actual selected EVAL metrics, NCF histories and CPU/CUDA measurements."""
import json

import streamlit as st

from app import charts
from app.client import ApiFault, identifier
from app.components import section,empty,table,plot,plot_and_table,metrics,apply_values,clear_callback,seed
from app.state import ALGORITHMS,count


def pending_get(api,path,**params):
    try:return api.get(path,**params)
    except ApiFault as exc:
        if exc.code != 'RESULT_NOT_READY':raise
        st.info(exc.message)
        st.caption('请求 ID：'+(exc.request_id or '未取得'))
        return None


def history(data,key):
    if not data['items']:
        empty('没有逐轮训练记录','非 NCF 模型不记录神经网络训练曲线；NCF 只展示实际执行过的轮次。')
        return
    plot_and_table(charts.epochs(data['items'],data['purpose']),data['items'],
        {'epoch':'轮次','train_loss':'训练损失','validation_ndcg':'验证 NDCG@10',
         'elapsed_seconds':'本轮用时 / 秒','peak_gpu_mib':'GPU 峰值分配 / MiB'},key)


def experiments(api,release):
    default_id = release['experiment_id'] if release else ''
    if not default_id and 'applied_experiments' not in st.session_state:
        if 'lookup_default_experiment' not in st.session_state:
            available=api.get('/experiments',page_size=1)['items']
            st.session_state['lookup_default_experiment']=available[0]['experiment_id'] if available else ''
        default_id=st.session_state['lookup_default_experiment']
    defaults = dict(experiment_id=default_id,algorithm='全部算法',split='TEST',cohort='WARM')
    current = defaults | st.session_state.get('applied_experiments',{})
    with st.form('experiment_filters',border=False):
        a,b = st.columns([3,1.4])
        with a:eid = st.text_input('实验编号',key=seed('view_experiments_id',current['experiment_id']),placeholder='输入已记录实验的 UUID')
        with b:algorithm = st.selectbox('关注算法',['全部算法',*ALGORITHMS],key=seed('view_experiments_algorithm',current['algorithm']))
        a,b,c = st.columns([1,1.4,1.4],vertical_alignment='bottom')
        with a:split = st.selectbox('评价窗口',['TEST','VALIDATION'],
            format_func=lambda x:'测试集 TEST' if x=='TEST' else '验证集 VALIDATION',key=seed('view_experiments_split',current['split']))
        with b:cohort = st.selectbox('评价人群',['WARM','ALL_ELIGIBLE'],
            format_func=lambda x:'已知客户 WARM' if x=='WARM' else '全部可评价客户 ALL_ELIGIBLE',key=seed('view_experiments_cohort',current['cohort']))
        with c:
            x,y=st.columns(2)
            with x:submitted=st.form_submit_button('应用',type='primary',use_container_width=True)
            with y:st.form_submit_button('清空',on_click=clear_callback,args=('experiments',),use_container_width=True)
    values = apply_values('experiments',defaults,submitted,dict(experiment_id=eid,algorithm=algorithm,split=split,cohort=cohort))
    if not values['experiment_id']:
        empty('尚无可查看的实验','实验通过命令行运行后，可在这里查看评价与训练记录。')
        return
    from uuid import UUID
    try:UUID(values['experiment_id'])
    except ValueError:
        st.error('请输入有效的实验 UUID。')
        return
    eid = values['experiment_id']
    with st.container(key='panel_eval_metrics'):
        section('四种算法，同一把尺子','EVAL 只在训练窗口拟合。默认算法由验证集选择，测试集只作封存后的报告。')
        data = pending_get(api,'/experiments/'+eid+'/metrics',split=values['split'],cohort=values['cohort'])
        if data:
            first = data['items'][0]
            metrics([('参与排名评价',count(first['user_count']),'四算法共同评价人群'),
                     ('候选商品',count(first['catalog_count']),'训练窗口完整目录'),
                     ('窗口客户',count(first['window_user_count']),'当前评价窗口有效客户')])
            plot_and_table(charts.metrics(data['items']),data['items'],
                {'algorithm':'算法','precision_at_k':'Precision@10','recall_at_k':'Recall@10','ndcg_at_k':'NDCG@10',
                 'coverage':'商品覆盖率','user_count':'评价人数','catalog_count':'候选数','cold_user_count':'冷启动客户',
                 'cold_item_count':'目录外商品','cold_interaction_count':'目录外交互','excluded_user_count':'排除客户'},'eval_metrics')
            if values['algorithm'] != '全部算法':
                focus = next(r for r in data['items'] if r['algorithm']==values['algorithm'])
                st.caption(f"关注算法 {focus['algorithm']}：NDCG@10 {focus['ndcg_at_k']:.4f}，Recall@10 {focus['recall_at_k']:.4f}。")
            st.caption('覆盖率表示推荐结果涉及的商品比例。无可评价样本不能记成 0；排名指标的 K 固定为 10。')
            with st.expander('查看 EVAL 模型与评价规模'):
                table(data['items'],{'algorithm':'算法','model_run_id':'选定 EVAL 模型','user_count':'参与评价人数',
                    'catalog_count':'候选商品数','window_user_count':'窗口客户数','cold_user_count':'冷启动客户',
                    'cold_item_count':'冷启动商品','cold_interaction_count':'冷启动交互','excluded_user_count':'排除客户数'},key='eval_evidence')
    if data:
        ncf = next(row['model_run_id'] for row in data['items'] if row['algorithm']=='NCF')
        with st.container(key='panel_eval_history'):
            section('NCF 的学习过程','训练损失与验证质量使用不同纵轴。只展示实际轮次，不补齐虚构曲线。')
            curve = api.get('/models/'+ncf+'/epochs')
            if curve['purpose'] != 'EVAL':raise ApiFault('INVALID_RESPONSE')
            history(curve,'eval_history')
            st.caption('模型用途 EVAL。验证质量用于早停与选参；训练损失下降不等于推荐效果一定提高。')
    with st.container(key='panel_benchmarks'):
        section('CPU 与 GPU，实测用时','同一批训练样本与配置，预热后固定 5 轮，CPU/CUDA 各独立重复 3 次。')
        benchmark = pending_get(api,'/experiments/'+eid+'/benchmarks')
        if benchmark:
            summary = benchmark['summary']
            if summary['comparison_valid']:
                metrics([('CPU 中位数',f"{summary['cpu_median_seconds']:.2f} 秒",'三次独立训练'),
                         ('GPU 中位数',f"{summary['gpu_median_seconds']:.2f} 秒",'CUDA 同步计时'),
                         ('加速比',f"{summary['speedup']:.2f}×",'CPU 中位数 ÷ GPU 中位数')])
                if summary['speedup'] < 1:
                    st.info('这个规模下 GPU 用时更长。小模型的调度与数据传输开销可能超过并行计算收益。')
            else:
                st.warning('CPU/GPU 对照尚未验收：重复次数不足或样本、配置不一致，不计算加速比。')
            plot_and_table(charts.benchmarks(benchmark['items']),benchmark['items'],
                {'device':'设备','repeat_index':'重复次数','train_seconds':'训练用时 / 秒','samples_per_second':'样本 / 秒',
                 'batch_size':'Batch 大小','epochs':'训练轮次','warmup_steps':'预热步数','peak_memory_mib':'峰值分配 / MiB'},'benchmark')
            with st.expander('查看真实设备与运行参数'):
                rows=[]
                for row in benchmark['items']:
                    try:hardware=json.loads(row['hardware'])
                    except (ValueError,TypeError):hardware={}
                    rows.append(dict(row,hardware_name=hardware.get('gpu_name') if row['device']=='CUDA' else hardware.get('cpu_model'),
                        cpu_rss=hardware.get('cpu_peak_rss_mib')))
                table(rows,{'device':'设备','repeat_index':'次数','hardware_name':'硬件型号','torch_version':'PyTorch',
                    'cuda_runtime':'CUDA 运行库','driver_version':'驱动','peak_memory_mib':'GPU 分配峰值 / MiB','cpu_rss':'进程 RSS 峰值 / MiB'},key='benchmark_hardware')
                st.caption('GPU 是 PyTorch 分配峰值，CPU 是进程 RSS 高水位，二者口径不同；用时不含预热与负采样生成。')
    with st.container(key='panel_fullfit'):
        section('页面实际使用的服务模型','FULLFIT 使用完整历史重新训练。与 EVAL 评价隔离，不提供 VALIDATION/TEST 指标。')
        if not release or release['experiment_id'] != eid:
            st.info('当前固定版本没有关联这个实验的服务模型。这里仍可独立查看离线评价与任务。')
        else:
            users = api.get('/customers',page_size=1)['items']
            if not users:
                empty('没有服务客户','当前版本尚无可用于查询服务模型的客户记录。')
                return
            selected = ALGORITHMS if values['algorithm']=='全部算法' else (values['algorithm'],)
            models=[]
            for method in selected:
                context=api.get('/customers/'+identifier(users[0]['customer_id'])+'/recommendations',algorithm=method,k=1)
                models.append(dict(algorithm=method,model_run_id=context['model_run_id'],purpose='FULLFIT'))
            table(models,{'algorithm':'算法','purpose':'用途','model_run_id':'固定版本绑定的服务模型'},key='fullfit_models')
            full_ncf=next((r['model_run_id'] for r in models if r['algorithm']=='NCF'),None)
            if full_ncf:
                curve=api.get('/models/'+full_ncf+'/epochs')
                if curve['purpose']!='FULLFIT':raise ApiFault('INVALID_RESPONSE')
                section('NCF 全历史训练曲线','沿用 EVAL 选定参数与轮数；不使用全量未来数据重新早停。')
                history(curve,'fullfit_history')
