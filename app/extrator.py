from b3_opcoes import baixar_arquivo_b3, extrair_csv_do_zip, ler_opcoes_b3_csv
from datetime import datetime


import yfinance as yf
import pandas as pd
from peewee import Model, SqliteDatabase, CharField, DateField, FloatField
from config import TICKERS

# Configuração do banco SQLite
db = SqliteDatabase('data/acoes.db')

# Modelos
class BaseModel(Model):
    class Meta:
        database = db

class HistoricoAcao(BaseModel):
    ticker = CharField()
    data = DateField()
    preco_fechamento = FloatField()

class Derivativo(BaseModel):
    ticker = CharField()
    expiry = CharField()
    tipo = CharField()  # CALL ou PUT
    strike = FloatField()
    last_price = FloatField()
    bid = FloatField()
    ask = FloatField()
    volume = FloatField()


def extrair_acoes(tickers, start='2025-01-01', end='2030-12-31'):
    dados = {}
    for ticker in tickers:
        acao = yf.Ticker(ticker)
        hist = acao.history(start=start, end=end)
        dados[ticker] = hist['Close']
        # Salva no banco em lote
        registros = [
            {'ticker': ticker, 'data': data.date(), 'preco_fechamento': preco}
            for data, preco in hist['Close'].items()
        ]
        if registros:
            with db.atomic():
                HistoricoAcao.insert_many(registros).on_conflict_replace().execute()
    df = pd.DataFrame(dados)
    return df



import threading
import time as time_mod

def extrair_e_salvar():
    # --- INTEGRAÇÃO B3 PARA AÇÕES BRASILEIRAS ---
    import os
    from datetime import date, timedelta
    import time as time_mod
    tickers_brasil = [t for t in TICKERS if t.endswith('.SA')]
    tickers_brasil_base = [t.replace('.SA','') for t in tickers_brasil]
    if tickers_brasil:
        pasta_zip = 'data/b3_zip'
        os.makedirs(pasta_zip, exist_ok=True)
        # Descobrir a data mínima de negociação já salva
        min_expiry = (
            Derivativo.select(Derivativo.expiry)
            .where(Derivativo.ticker.in_(tickers_brasil))
            .order_by(Derivativo.expiry.asc())
            .first()
        )
        min_date = date(2025, 1, 1)
        if min_expiry:
            try:
                min_date = datetime.strptime(str(min_expiry.expiry)[:10], '%Y-%m-%d').date()
            except Exception:
                pass
        hoje = date.today()
        data_atual = min_date
        while data_atual <= hoje:
            if data_atual.weekday() < 5:  # 0=segunda, 4=sexta
                nome_zip = f"COTAHIST_D{data_atual.strftime('%d%m%Y')}.ZIP"
                caminho_zip = os.path.join(pasta_zip, nome_zip)
                if not os.path.exists(caminho_zip):
                    try:
                        # Função baixar_arquivo_b3 deve aceitar caminho de destino
                        zip_path = baixar_arquivo_b3(data_atual.strftime('%Y%m%d'), destino=caminho_zip)
                        if zip_path:
                            # Descompacta em memória
                            csv_path = extrair_csv_do_zip(zip_path, em_memoria=True)
                            if csv_path:
                                df_b3 = ler_opcoes_b3_csv(csv_path, tickers=tickers_brasil_base)
                                for _, row in df_b3.iterrows():
                                    try:
                                        Derivativo.get_or_create(
                                            ticker=row['PAPEL']+'.SA',
                                            expiry=row['VENCIMENTO'],
                                            tipo=row['TIPO_OPCAO'],
                                            strike=row['PRECO_EXERC'],
                                            defaults={
                                                'last_price': row.get('PRECO_ULTIMO', 0) if pd.notnull(row.get('PRECO_ULTIMO', 0)) else 0,
                                                'bid': 0,
                                                'ask': 0,
                                                'volume': row.get('QUANT_NEGOCIADA', 0) if pd.notnull(row.get('QUANT_NEGOCIADA', 0)) else 0
                                            }
                                        )
                                    except Exception as e:
                                        print(f"[B3] Erro ao salvar derivativo: {e}")
                            # Aguarda 60 segundos antes de baixar o próximo arquivo
                            print(f"[B3] Aguardando 60s para próximo arquivo...")
                            time_mod.sleep(60)
                    except Exception as e:
                        print(f"[B3] Erro ao baixar/processar {nome_zip}: {e}")
            data_atual += timedelta(days=1)
    db.connect(reuse_if_open=True)
    db.create_tables([HistoricoAcao, Derivativo])
    tickers = TICKERS
    df = extrair_acoes(tickers)

    # Buscar derivativos (opções) para cada ação e todas as datas de expiração disponíveis
    for ticker in tickers:
        acao = yf.Ticker(ticker)
        expiries = acao.options
        print(f"\nDerivativos para {ticker} (datas de expiração disponíveis): {expiries}")
        if expiries:
            for expiry in expiries:
                try:
                    opt_chain = acao.option_chain(expiry)
                except Exception as e:
                    print(f"Erro ao buscar opções para {ticker} em {expiry}: {e}")
                    continue
                # Salva CALLS
                for _, row in opt_chain.calls.iterrows():
                    Derivativo.get_or_create(
                        ticker=ticker,
                        expiry=expiry,
                        tipo='CALL',
                        strike=row['strike'],
                        defaults={
                            'last_price': row.get('lastPrice', 0) if pd.notnull(row.get('lastPrice', 0)) else 0,
                            'bid': row.get('bid', 0) if pd.notnull(row.get('bid', 0)) else 0,
                            'ask': row.get('ask', 0) if pd.notnull(row.get('ask', 0)) else 0,
                            'volume': row.get('volume', 0) if pd.notnull(row.get('volume', 0)) else 0
                        }
                    )
                # Salva PUTS
                for _, row in opt_chain.puts.iterrows():
                    Derivativo.get_or_create(
                        ticker=ticker,
                        expiry=expiry,
                        tipo='PUT',
                        strike=row['strike'],
                        defaults={
                            'last_price': row.get('lastPrice', 0) if pd.notnull(row.get('lastPrice', 0)) else 0,
                            'bid': row.get('bid', 0) if pd.notnull(row.get('bid', 0)) else 0,
                            'ask': row.get('ask', 0) if pd.notnull(row.get('ask', 0)) else 0,
                            'volume': row.get('volume', 0) if pd.notnull(row.get('volume', 0)) else 0
                        }
                    )
    db.close()

def thread_extrator():
    while True:
        print("[EXTRATOR] Iniciando extração e salvamento de dados...")
        try:
            extrair_e_salvar()
        except Exception as e:
            print(f"[EXTRATOR] Erro: {e}")
        print("[EXTRATOR] Aguardando 5 minutos para próxima execução...")
        time_mod.sleep(300)
