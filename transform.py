import pandas as pd
import json

def transform_json_to_parquet(input_file, output_file):
    """Le um arquivo JSON e o converte em Parquet."""
    try: 
        
        # Carrega o Json
        with open(input_file, 'r') as f:
            
            data = json.load(f)
            
        # Converte para um DataFrame do Pandas (formato de tabela)
        # Se for uma lista, converte diretamente; se for um dicionário, envolve em lista
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame([data])
    
        # Salva como Parquet
        df.to_parquet(output_file, index=False)
        print(f"---Sucesso! {input_file} transformado em {output_file}---")
        print(f"Total de registros: {len(df)}")
        
    except Exception as e:
        print(f"Erro na transformação: {e}")
        
if __name__ == "__main__":
    transform_json_to_parquet('raw_data.json', 'refined_data.parquet')