from flask import Flask, render_template, request
import extrator

app = Flask(__name__)

@app.route('/')
def index():
    tickers = request.args.get('tickers', 'PETR4.SA,VALE3.SA,ITUB4.SA').split(',')
    df = extrator.extrair_acoes(tickers)
    return df.to_html(classes='table table-striped')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8085)
