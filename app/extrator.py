

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
