import pandas as pd
from datetime import datetime

def create_gold_layer(input_parquet, output_gold):
    """Cria uma tabela analitica (Camada Gold) pronta para Dashboards."""
    
    try:
        # Lendo o dado refinado (Silver)
        df = pd.read_parquet(input_parquet)
        
        # Na Gold, criamos novas métricas (Business Logic)
        
        # 1. Status de Preço - Categorização de preço
        df['price_range'] = df['price_usd'].apply(
            lambda x: 'Ultra Alto' if x > 100000 
            else 'Alto' if x > 50000 
            else 'Médio' if x > 10000 
            else 'Baixo'
        )
        
        # 2. Status de Mercado - Categorização de market cap
        df['market_status'] = df['price_usd'].apply(
            lambda x: 'High' if x > 50000 else 'Low'
        )
        
        # 3. Classificação de Capitalização
        df['market_cap_status'] = df['market_cap_usd'].apply(
            lambda x: 'Mega' if x > 1000000000000 
            else 'Grande' if x > 100000000000 
            else 'Médio' if x > 10000000000 
            else 'Pequeno'
        )
        
        # 4. Fonte de dados
        df['data_source'] = 'CoinGecko API'
        
        # 5. Timestamp de extração (renomeando a coluna existente)
        df = df.rename(columns={'timestamp': 'extracted_at'})
        
        # 6. Timestamp de atualização na gold
        df['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Selecionando e ordenando as colunas essenciais para o relatório final 
        gold_df = df[['id', 'symbol', 'price_usd', 'price_range', 'market_status', 
                      'market_cap_usd', 'market_cap_status', 'data_source', 
                      'extracted_at', 'last_update']]
        
        # Salvando a tabela final
        gold_df.to_parquet(output_gold, index=False)
        print(f"---Sucesso! Camada Gold criada em {output_gold}---")
        print(f"\nDados processados:")
        print(gold_df.to_string())
        
    except Exception as e:
        print(f"Erro na criação da camada Gold: {e}")

        
if __name__ == "__main__":
    create_gold_layer('refined_data.parquet', 'gold_crypto_report.parquet')