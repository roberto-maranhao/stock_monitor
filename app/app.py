

# Importa e registra blueprint do gráfico
from grafico import bp_grafico

from flask import Flask, render_template, request
import extrator
import time
import logging


# Inicia a thread de extração automática
import threading
extrator_thread = threading.Thread(target=extrator.thread_extrator, daemon=True)
extrator_thread.start()


# Configura logging para mostrar no console
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

app = Flask(__name__)
app.register_blueprint(bp_grafico)



@app.route('/')
def index():
    logging.debug('Request recebido na rota /')
    # Buscar todos os tickers extraídos (do extrator)
    from config import TICKERS
    tickers = TICKERS
    links = []
    for ticker in tickers:
        links.append(f'<li><b>{ticker}</b> '
                    f'<a href="/grafico?ticker={ticker}">Histórico</a>'
                    f' | <a href="/grafico?ticker={ticker}&tipo=CALL">CALLs</a>'
                    f' | <a href="/grafico?ticker={ticker}&tipo=PUT">PUTs</a>'
                    + '</li>')
    html = '<h2>Selecione o ativo e o tipo de gráfico:</h2><ul>' + '\n'.join(links) + '</ul>'
    return html

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8085)
