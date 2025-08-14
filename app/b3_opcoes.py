import os
import requests
import pandas as pd
from datetime import datetime

def baixar_arquivo_b3(data, pasta_destino='data'):
    """
    Baixa o arquivo de opções da B3 para a data informada (YYYYMMDD).
    """
    url = f"https://bvmf.bmfbovespa.com.br/InstDados/SerHist/Opcao_{data}.zip"
    local_zip = os.path.join(pasta_destino, f"Opcao_{data}.zip")
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(local_zip, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return local_zip
    else:
        return None

def extrair_csv_do_zip(zip_path, pasta_destino='data'):
    import zipfile
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(pasta_destino)
    for fname in os.listdir(pasta_destino):
        if fname.endswith('.csv'):
            return os.path.join(pasta_destino, fname)
    return None

def ler_opcoes_b3_csv(csv_path, tickers=None):
    df = pd.read_csv(csv_path, sep=';', encoding='latin1')
    if tickers:
        df = df[df['PAPEL'].isin(tickers)]
    return df

# Exemplo de uso:
# data = datetime.now().strftime('%Y%m%d')
# zip_path = baixar_arquivo_b3(data)
# if zip_path:
#     csv_path = extrair_csv_do_zip(zip_path)
#     df = ler_opcoes_b3_csv(csv_path, tickers=['PETR4', 'BBAS3'])
#     print(df.head())
