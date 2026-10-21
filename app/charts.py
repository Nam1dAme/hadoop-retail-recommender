"""Interactive figures; exact decimal strings remain available in data tables."""
import math
from html import escape

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from app.state import pounds, SEGMENTS

BLUE = '#3159D6'
TEAL = '#16838B'
AMBER = '#B66A16'
INK = '#1D2B45'
COLORS = [BLUE, TEAL, AMBER, '#7765AE', '#7C899E']


def finish(fig, height=320, *, money=False):
    fig.update_layout(height=height, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Retail Numerals, Retail Chinese, sans-serif', size=12, color=INK),
        margin=dict(l=8,r=16,t=30,b=12), hoverlabel=dict(bgcolor='white',font_size=13),
        legend=dict(orientation='h',y=1.14,x=0,font_size=12),
        hovermode='x unified', dragmode=False, modebar_remove=['toImage','select2d','lasso2d'])
    fig.update_xaxes(showgrid=False,zeroline=False,fixedrange=True,color='#63718A',title_text=None,automargin=True)
    fig.update_yaxes(gridcolor='#EAF0F6',zeroline=False,fixedrange=True,color='#63718A',title_text=None,automargin=True)
    if money:
        fig.update_yaxes(tickprefix='£',tickformat='~s')
    return fig


def trend(points):
    fig = go.Figure()
    for key,label,color,fill in [('gross_amount','正向销售',BLUE,True),('return_amount','退货金额',AMBER,False),('net_amount','销售净额',TEAL,False)]:
        fig.add_trace(go.Scatter(x=[r['period_start'] for r in points],y=[float(r[key]) for r in points],
            name=label,mode='lines',line=dict(color=color,width=2.2 if fill else 1.7,dash='dot' if key=='net_amount' else 'solid'),
            fill='tozeroy' if fill else None,fillcolor='rgba(49,89,214,0.065)' if fill else None,
            customdata=[pounds(r[key]) for r in points],hovertemplate='%{customdata}<extra>'+label+'</extra>'))
    finish(fig,340,money=True)
    fig.update_xaxes(tickformat='%Y-%m',hoverformat='%Y-%m-%d')
    return fig


def ranking(rows, *, country=False, quantity=False):
    value = 'net_amount' if country else 'sale_quantity' if quantity else 'gross_amount'
    key = 'country' if country else 'stock_code'
    label = '销售净额' if country else '正向销量' if quantity else '正向销售额'
    fig = go.Figure(go.Bar(x=[float(r[value]) for r in rows[::-1]], y=[r[key] for r in rows[::-1]],
        orientation='h',marker_color=BLUE,marker_line_width=0,width=.6,
        customdata=[str(r[value]) if quantity else pounds(r[value]) for r in rows[::-1]],
        hovertemplate='%{y}<br>'+label+'：%{customdata}<extra></extra>'))
    finish(fig, max(220, min(650, len(rows)*25+45)))
    fig.update_layout(hovermode='closest',bargap=.3)
    fig.update_xaxes(showgrid=True,gridcolor='#EAF0F6',tickprefix='' if quantity else '£',tickformat='~s')
    fig.update_yaxes(showgrid=False,type='category')
    return fig


def segments(rows):
    fig = go.Figure(go.Bar(x=[r['customer_count'] for r in rows[::-1]],
        y=[SEGMENTS[r['segment']] for r in rows[::-1]],orientation='h',
        marker_color=COLORS[::-1],text=[format(r['customer_count'],',') for r in rows[::-1]],textposition='outside',
        hovertemplate='%{y}<br>%{x:,} 位客户<extra></extra>',cliponaxis=False,width=.5))
    finish(fig,260)
    fig.update_layout(hovermode='closest',margin=dict(l=8,r=50,t=10,b=15))
    fig.update_yaxes(showgrid=False)
    fig.update_xaxes(showgrid=True,gridcolor='#EAF0F6',rangemode='tozero')
    return fig


def associations(data):
    items = data['items']
    fig = go.Figure()
    xs,ys,texts,hover = [0],[0],[data['stock_code']],[escape(data['description'] or data['stock_code'])]
    for index,row in enumerate(items):
        angle = 2*math.pi*index/max(1,len(items)) - math.pi/2
        x,y = math.cos(angle),math.sin(angle)
        fig.add_trace(go.Scatter(x=[0,x],y=[0,y],mode='lines',line=dict(color='#C9D7F4',width=1+row['similarity']*5),hoverinfo='skip',showlegend=False))
        xs.append(x);ys.append(y);texts.append(row['related_stock_code'])
        hover.append(f"{escape(row['description'] or row['related_stock_code'])}<br>相似度 {row['similarity']:.4f}<br>同单共现 {row['co_count']:,} 次")
    fig.add_trace(go.Scatter(x=xs,y=ys,mode='markers+text',text=texts,textposition='bottom center',
        textfont=dict(size=12,color=INK),marker=dict(size=[36]+[20]*len(items),color=[BLUE]+[TEAL]*len(items),line=dict(color='white',width=3)),
        hovertext=hover,hovertemplate='%{hovertext}<extra></extra>',showlegend=False))
    finish(fig,400)
    fig.update_xaxes(visible=False,range=[-1.45,1.45])
    fig.update_yaxes(visible=False,range=[-1.45,1.45],scaleanchor='x',scaleratio=1)
    fig.update_layout(hovermode='closest',margin=dict(l=0,r=0,t=8,b=8))
    return fig


def metrics(rows):
    fig = go.Figure()
    for field,label,color in [('ndcg_at_k','NDCG@10',BLUE),('recall_at_k','Recall@10',TEAL),('precision_at_k','Precision@10',AMBER)]:
        fig.add_trace(go.Bar(x=[r['algorithm'] for r in rows],y=[r[field] for r in rows],name=label,marker_color=color,
            hovertemplate='%{x}<br>'+label+'：%{y:.4f}<extra></extra>'))
    finish(fig,300)
    fig.update_layout(barmode='group',bargap=.45)
    fig.update_yaxes(rangemode='tozero',tickformat='.2f')
    return fig


def epochs(rows, purpose):
    fig = make_subplots(specs=[[{'secondary_y':True}]])
    fig.add_trace(go.Scatter(x=[r['epoch'] for r in rows],y=[r['train_loss'] for r in rows],mode='lines+markers',
        name='训练损失',line=dict(color=BLUE,width=2.2),marker_size=4),secondary_y=False)
    if purpose == 'EVAL' and any(r['validation_ndcg'] is not None for r in rows):
        fig.add_trace(go.Scatter(x=[r['epoch'] for r in rows],y=[r['validation_ndcg'] for r in rows],mode='lines+markers',
            name='验证 NDCG@10',line=dict(color=TEAL,width=1.8,dash='dot'),marker_size=4),secondary_y=True)
    finish(fig,300)
    fig.update_xaxes(title_text='实际训练轮次',dtick=1 if len(rows)<25 else None)
    fig.update_yaxes(title_text='训练损失',secondary_y=False)
    fig.update_yaxes(title_text='验证 NDCG@10' if purpose=='EVAL' else None,showgrid=False,secondary_y=True)
    return fig


def benchmarks(rows):
    fig = go.Figure()
    for device,label,color in [('CPU','CPU',BLUE),('CUDA','GPU / CUDA',TEAL)]:
        group = [r for r in rows if r['device']==device]
        fig.add_trace(go.Bar(x=[f"第 {r['repeat_index']} 次" for r in group],y=[r['train_seconds'] for r in group],
            name=label,marker_color=color,hovertemplate=label+'<br>%{y:.3f} 秒<extra></extra>'))
    finish(fig,260)
    fig.update_layout(barmode='group',bargap=.5)
    fig.update_yaxes(title_text='训练用时 / 秒',rangemode='tozero')
    return fig
