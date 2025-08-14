
import yfinance as yf
import pandas as pd
from peewee import Model, SqliteDatabase, CharField, DateField, FloatField

# Configuração do banco SQLite
db = SqliteDatabase('acoes.db')

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


def extrair_acoes(tickers, start='2010-01-01', end='2025-12-31'):
    dados = {}
    for ticker in tickers:
        acao = yf.Ticker(ticker)
        hist = acao.history(start=start, end=end)
        dados[ticker] = hist['Close']
        # Salva no banco
        for data, preco in hist['Close'].items():
            HistoricoAcao.get_or_create(ticker=ticker, data=data.date(), defaults={'preco_fechamento': preco})
    df = pd.DataFrame(dados)
    return df


if __name__ == "__main__":
    db.connect()
    db.create_tables([HistoricoAcao, Derivativo])
    tickers = ['TSLA', 'MSFT']
    df = extrair_acoes(tickers)
    print("Histórico de ações:")
    print(df.tail())

    # Buscar derivativos (opções) para cada ação
    for ticker in tickers:
        acao = yf.Ticker(ticker)
        expiries = acao.options
        print(f"\nDerivativos para {ticker} (datas de expiração disponíveis): {expiries}")
        if expiries:
            expiry = expiries[-1]
            opt_chain = acao.option_chain(expiry)
            # Salva CALLS
            for _, row in opt_chain.calls.iterrows():
                Derivativo.get_or_create(
                    ticker=ticker,
                    expiry=expiry,
                    tipo='CALL',
                    strike=row['strike'],
                    defaults={
                        'last_price': row['lastPrice'],
                        'bid': row['bid'],
                        'ask': row['ask'],
                        'volume': row['volume']
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
                        'last_price': row['lastPrice'],
                        'bid': row['bid'],
                        'ask': row['ask'],
                        'volume': row['volume']
                    }
                )
    db.close()
