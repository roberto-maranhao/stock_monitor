
from flask import Flask, render_template, request
import extrator
import time
import logging

# Garante que as tabelas existem ao iniciar o app
extrator.db.connect()
extrator.db.create_tables([extrator.HistoricoAcao, extrator.Derivativo], safe=True)


# Configura logging para mostrar no console
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
app = Flask(__name__)


@app.route('/')
def index():
    logging.debug('Request recebido na rota /')
    start = time.time()
    tickers = request.args.get('tickers', 'PETR4.SA,VALE3.SA,ITUB4.SA').split(',')
    logging.debug(f'Tickers recebidos: {tickers}')
    df = extrator.extrair_acoes(tickers)
    logging.debug(f'Consulta/extracao de acoes finalizada em {time.time() - start:.2f}s')
    html = df.to_html(classes='table table-striped')
    logging.debug(f'Total de tempo de processamento: {time.time() - start:.2f}s')
    return html

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8085)
