import requests
import pandas as pd
import json
import boto3
from datetime import datetime

def fetch_crypto_data(coin_id='bitcoin'):
    """Busca dados de mercado de uma criptomoeda específica."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro na API:{response.status_code}")
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

def upload_to_s3(file_name, bucket):
    """Envia o arquivo para o LocalStack (simulando AWS S3)."""
    #Usamos 'test' como credencial para o LocalStack nao travar.
    s3 = boto3.client('s3',
                         endpoint_url="http://localhost:4566",
                         aws_access_key_id="test",
                         aws_secret_access_key="test",
                         region_name="us-east-1"
                         )
    
    try:
        s3.upload_file(file_name, bucket, file_name)
        print(f"---Sucesso! {file_name} enviado para o bucket {bucket}---")
        
    except Exception as e:
        print(f"Erro ao enviar para S3: {e}")
        
if __name__ == "__main__":
    print("Iniciando extração...")
    raw = fetch_crypto_data()
    
    if raw:
       data = simplify_data(raw)
       
       #Salvando localmente 'landing zone' (Data Lake)
       file_path = 'raw_data.json'
       with open(file_path, 'w') as f:
              json.dump(data, f, indent=4)

print(F"2. Dados salvos localmente em {file_path}")

# Enviando para o LocalStack S3
print("3. Iniciando upload para o LocalStack S3...")
upload_to_s3(file_path, 'data-lake-raw')