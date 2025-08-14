
import logging
from datetime import timedelta
import pandas as pd
from flask import Blueprint, request
import extrator
import plotly.graph_objs as go
from markupsafe import Markup

bp_grafico = Blueprint('grafico', __name__)



@bp_grafico.route('/grafico')
def grafico():
    logging.debug('Request recebido na rota /grafico')
    ticker = request.args.get('ticker', 'TSLA')
    tipo = request.args.get('tipo', None)  # None, CALL, PUT
    # Buscar a data de expiração mais próxima que tenha opções do tipo solicitado
    expiry = None
    expiries = (extrator.Derivativo
        .select(extrator.Derivativo.expiry)
        .where(extrator.Derivativo.ticker == ticker)
        .distinct()
        .order_by(extrator.Derivativo.expiry.asc()))
    for exp in expiries:
        if tipo is None:
            # Qualquer expiração com pelo menos um derivativo
            has_call = extrator.Derivativo.select().where((extrator.Derivativo.ticker == ticker) & (extrator.Derivativo.expiry == exp.expiry) & (extrator.Derivativo.tipo == 'CALL')).exists()
            has_put = extrator.Derivativo.select().where((extrator.Derivativo.ticker == ticker) & (extrator.Derivativo.expiry == exp.expiry) & (extrator.Derivativo.tipo == 'PUT')).exists()
            if has_call or has_put:
                expiry = exp.expiry
                break
        else:
            if extrator.Derivativo.select().where((extrator.Derivativo.ticker == ticker) & (extrator.Derivativo.expiry == exp.expiry) & (extrator.Derivativo.tipo == tipo.upper())).exists():
                expiry = exp.expiry
                break
    if not expiry:
        return f'Nenhum derivativo do tipo {tipo or "CALL/PUT"} encontrado para {ticker}.'
    try:
        expiry_date = pd.to_datetime(expiry).date()
    except Exception as e:
        return f'Erro ao converter data de expiração: {expiry} ({e})'
    d0 = expiry_date
    d60 = d0 - timedelta(days=60)
    # Buscar histórico do ticker nesse período
    historico = (extrator.HistoricoAcao
        .select()
        .where((extrator.HistoricoAcao.ticker == ticker) &
               (extrator.HistoricoAcao.data >= d60) &
               (extrator.HistoricoAcao.data <= d0))
        .order_by(extrator.HistoricoAcao.data))
    datas = [h.data for h in historico]
    precos = [h.preco_fechamento for h in historico]
    # Buscar derivativos (CALLs e PUTs) para a data de expiração
    calls = list(extrator.Derivativo.select().where((extrator.Derivativo.ticker == ticker) & (extrator.Derivativo.expiry == expiry) & (extrator.Derivativo.tipo == 'CALL')))
    puts = list(extrator.Derivativo.select().where((extrator.Derivativo.ticker == ticker) & (extrator.Derivativo.expiry == expiry) & (extrator.Derivativo.tipo == 'PUT')))

    # Gráfico interativo com Plotly
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=datas, y=precos, mode='lines+markers', name=f'{ticker} (fechamento)', yaxis='y1', line=dict(color='blue')))
    if tipo is None or tipo.upper() == 'CALL':
        if calls:
            fig.add_trace(go.Scatter(x=[c.strike for c in calls], y=[c.last_price for c in calls], mode='markers+lines', name='CALLs (preço)', yaxis='y2', marker=dict(color='green')))
    if tipo is None or tipo.upper() == 'PUT':
        if puts:
            fig.add_trace(go.Scatter(x=[p.strike for p in puts], y=[p.last_price for p in puts], mode='markers+lines', name='PUTs (preço)', yaxis='y2', marker=dict(color='red')))
    fig.update_layout(
        title=f'{ticker}: Histórico (d-60 a d0) e Derivativos (expiração {expiry})',
        xaxis=dict(title='Data (ação) / Strike (derivativos)'),
        yaxis=dict(title='Preço da ação (USD)', titlefont=dict(color='blue'), tickfont=dict(color='blue')),
        yaxis2=dict(title='Preço do derivativo (USD)', titlefont=dict(color='gray'), tickfont=dict(color='gray'), overlaying='y', side='right'),
        legend=dict(x=0.01, y=0.99),
        margin=dict(l=40, r=40, t=60, b=40),
        height=500
    )
    # Renderizar como HTML
    graph_html = fig.to_html(full_html=False)
    return Markup(graph_html)
