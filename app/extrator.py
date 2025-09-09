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
    print("[EXTRATOR] Conectando ao banco de dados...")
    try:
        db.connect(reuse_if_open=True)
        db.create_tables([HistoricoAcao, Derivativo])
        print("[EXTRATOR] Banco de dados conectado e tabelas criadas")
    except Exception as e:
        print(f"[EXTRATOR] Erro ao conectar banco: {e}")
        return
    
    # --- INTEGRAÇÃO B3 PARA AÇÕES BRASILEIRAS ---
    import os
    from datetime import date, timedelta
    import time as time_mod
    
    print(f"[EXTRATOR] Iniciando extração de dados para tickers: {TICKERS}")
    
    # Primeiro, extrai dados básicos das ações
    try:
        print("[EXTRATOR] Extraindo dados básicos das ações...")
        tickers = TICKERS
        df = extrair_acoes(tickers)
        print(f"[EXTRATOR] Dados básicos extraídos para {len(tickers)} tickers")
    except Exception as e:
        print(f"[EXTRATOR] Erro ao extrair dados básicos: {e}")

    # Buscar derivativos (opções) para cada ação e todas as datas de expiração disponíveis
    print("[EXTRATOR] Iniciando extração de derivativos...")
    for ticker in TICKERS:
        try:
            print(f"[EXTRATOR] Processando derivativos para {ticker}...")
            acao = yf.Ticker(ticker)
            expiries = acao.options
            print(f"[EXTRATOR] Derivativos para {ticker} (datas de expiração disponíveis): {len(expiries) if expiries else 0}")
            
            if expiries:
                for expiry in expiries[:5]:  # Limita a 5 expirations para não sobrecarregar
                    try:
                        opt_chain = acao.option_chain(expiry)
                        print(f"[EXTRATOR] Processando opções para {ticker} - {expiry}")
                    except Exception as e:
                        print(f"[EXTRATOR] Erro ao buscar opções para {ticker} em {expiry}: {e}")
                        continue
                        
                    # Salva CALLS
                    calls_count = 0
                    for _, row in opt_chain.calls.iterrows():
                        try:
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
                            calls_count += 1
                        except Exception as e:
                            print(f"[EXTRATOR] Erro ao salvar CALL: {e}")
                    
                    # Salva PUTS
                    puts_count = 0
                    for _, row in opt_chain.puts.iterrows():
                        try:
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
                            puts_count += 1
                        except Exception as e:
                            print(f"[EXTRATOR] Erro ao salvar PUT: {e}")
                    
                    print(f"[EXTRATOR] Salvos {calls_count} CALLs e {puts_count} PUTs para {ticker} - {expiry}")
        except Exception as e:
            print(f"[EXTRATOR] Erro geral ao processar {ticker}: {e}")
    
    try:
        db.close()
        print("[EXTRATOR] Banco de dados fechado")
    except Exception as e:
        print(f"[EXTRATOR] Erro ao fechar banco: {e}")
    
    print("[EXTRATOR] Extração completa!")

def thread_extrator():
    while True:
        print("[EXTRATOR] Iniciando extração e salvamento de dados...")
        try:
            extrair_e_salvar()
        except Exception as e:
            print(f"[EXTRATOR] Erro: {e}")
        print("[EXTRATOR] Aguardando 5 minutos para próxima execução...")
        time_mod.sleep(300)
