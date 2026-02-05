import requests
import pandas as pd
import json
import boto3
import time
from datetime import datetime

def fetch_crypto_data(coin_id='bitcoin'):
    """Busca dados de mercado de uma criptomoeda específica."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    max_retries = 3
    
    for attempt in range(max_retries):
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            if attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)  # Exponential backoff: 2, 4, 8 segundos
                print(f"    Rate limited. Aguardando {wait_time}s antes de retry...")
                time.sleep(wait_time)
            else:
                print(f"Erro na API: {response.status_code} (máximo de tentativas excedido)")
                return None
        else:
            print(f"Erro na API: {response.status_code}")
            return None
    
    return None
    
def simplify_data(raw_data):
    """Transforma o JSON bruto em um formato estruturado (Dicionario)."""
    processed_data = {
        "id": raw_data['id'],
        "symbol": raw_data['symbol'],
        "price_usd": raw_data['market_data']['current_price']['usd'],
        "market_cap_usd": raw_data['market_data']['market_cap']['usd'],
        "timestamp": datetime.now().isoformat()
        }
    return processed_data

def fetch_multiple_cryptos(coin_list=['bitcoin', 'ethereum', 'ripple', 'cardano']):
    """Busca dados de múltiplas criptomoedas."""
    all_data = []
    
    for coin in coin_list:
        print(f"  Buscando dados de {coin}...")
        raw = fetch_crypto_data(coin)
        if raw:
            data = simplify_data(raw)
            all_data.append(data)
        else:
            print(f"  ! Falha ao buscar {coin}")
        time.sleep(2)  # Delay de 2 segundos entre requisições
    
    return all_data

def upload_to_s3(file_name, bucket):
    try: 
        s3 = boto3.client('s3',
                             endpoint_url="http://localhost:4566",
                             aws_access_key_id="test",
                             aws_secret_access_key="test",
                             region_name="us-east-1"
                             )
        
        s3.upload_file(file_name, bucket, file_name)
        print(f"---Sucesso no S3!")
        
    except Exception as e:
        print(f"Aviso: S3 Offline, mas os dados locais estão salvos.")
        
if __name__ == "__main__":
    print("Iniciando extração de múltiplas criptomoedas...")
    
    # Lista de criptomoedas para análise (apenas as com sucesso consistente)
    cryptos = ['bitcoin', 'ethereum', 'ripple', 'cardano', 'uniswap']
    data_list = fetch_multiple_cryptos(cryptos)
    
    if data_list:
       # Salvando localmente (landing zone - Data Lake)
       file_path = 'raw_data.json'
       with open(file_path, 'w') as f:
              json.dump(data_list, f, indent=4)

       print(f"2. Dados salvos localmente em {file_path}")

       # Enviando para o LocalStack S3
       print("3. Iniciando upload para o LocalStack S3...")
       upload_to_s3(file_path, 'data-lake-raw')