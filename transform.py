import pandas as pd
import json

def transform_json_to_parquet(input_file, output_file):
    """Le um rquivo JSON e o converte em Parquet."""
    try: 
        
        # Carrega o Json
        with open(input_file, 'r') as f:
            
            data = json.load(f)
            
        # Converte para um DataFrame do Pandas (formato de tabela)
        df = pd.DataFrame([data])  # Envolvemos em uma lista para criar um DataFrame de uma linha
    
        # Salva como Parquet
        df.to_parquet(output_file, index=False)
        print(f"---Sucesso! {input_file} transformado em {output_file}---")
        
    except Exception as e:
        print(f"Erro na transformação: {e}")
        
if __name__ == "__main__":
        transform_json_to_parquet('raw_data.json', 'refined_data.parquet')