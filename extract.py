import requests
import pandas as pd
import json
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
        "price_usd": raw_data['mSarket_data']['current_price']['usd'],
        "market_cap_usd": raw_data['market_data']['market_cap']['usd'],
        "timestamp": datetime.now().isoformat()
        }
    return processed_data

if __name__ == "__main__":
    print("Iniciando extração...")
    raw = fetch_crypto_data()
    if raw:
       data = simplify_data(raw)
       #Salva como JSON para simularum 'landing zone' (Data Lake)
       with open('raw_data.json', 'w') as f:
              json.dump(raw, f, indent=4)
        
print("Dados salvos com sucesso em raw_data.json!")