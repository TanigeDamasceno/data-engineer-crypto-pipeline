import pandas as pd
from datetime import datetime

def create_gold_layer(input_parquet, output_gold):
    """Cria uma tabela analitica (Camada Gold) pronta para Dashboards."""
    
    try:
        # Lendo o dado refinado (Silver)
        df = pd.read_parquet(input_parquet)
        
        #Na Gold, criamos novas métricas (Business Logic)
        #Ex: Cálculo de variação ou status de mercado
        df['market_status'] = df['price_usd'].apply(lambda x: 'High' if x > 50000 else 'Low')
        df['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Selecionando apenas o que é essencial para o relatório final 
        gold_df = df[['symbol', 'price_usd', 'market_status', 'last_update']]
        
        # Salvando a tabela final
        
        gold_df.to_parquet(output_gold, index=False)
        print(f"---Sucesso! Camada Gold criada em {output_gold}---")
        
    except Exception as e:
        print(f"Erro na criação da camada Gold: {e}")
        
    if __name__ == "__main__":
        create_gold_layer('refined_data.parquet', 'gold_crypto_report.parquet')
        print(f"---Sucesso! Camada Gold criada em gold_crypto_report.parquet---")