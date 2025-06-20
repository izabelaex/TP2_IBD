import pandas as pd
import glob
import re # Usaremos a biblioteca de expressões regulares para a limpeza

print("--- INICIANDO PROCESSAMENTO (LIMPEZA AGRESSIVA DE CNPJ) ---")

# --- 1. LEITURA COM LIMPEZA AGRESSIVA DO ARQUIVO DE SÓCIOS ---
arquivo_socios = 'socios.csv'
cnpjs_interesse = set()

print("✔ [Passo 1/3] Lendo o arquivo de sócios com limpeza agressiva...")
try:
    with open(arquivo_socios, mode='r', encoding='utf-8-sig') as file:
        for line in file:
            if not line.strip():
                continue
            
            # Pega os primeiros 15 caracteres da linha (para garantir)
            parte_inicial = line[:15]
            
            # Usa uma expressão regular para remover TUDO que não for dígito
            cnpj_limpo = re.sub(r'\D', '', parte_inicial)
            
            # Pega apenas os 8 primeiros dígitos do resultado limpo
            if len(cnpj_limpo) >= 8:
                cnpj_final = cnpj_limpo[:8]
                cnpjs_interesse.add(cnpj_final)
    
    if not cnpjs_interesse:
        print("🚨 ERRO: Nenhum CNPJ de 8 dígitos foi extraído. Verifique o formato do arquivo de sócios.")
        exit()
        
    print(f"✔ {len(cnpjs_interesse)} CNPJs de sócios extraídos e limpos com sucesso.")

except Exception as e:
    print(f"🚨 ERRO CRÍTICO ao ler o arquivo de sócios: {e}")
    exit()

# --- 2. BUSCA NOS ARQUIVOS DE EMPRESAS (sem alterações aqui) ---
print("\n✔ [Passo 2/3] Buscando correspondências nos arquivos de empresas...")
padrao_arquivos_empresas = 'empresas*.csv' 
lista_arquivos_empresas = sorted(glob.glob(padrao_arquivos_empresas))

if not lista_arquivos_empresas:
    print(f"🚨 ERRO: Nenhum arquivo de empresa encontrado com o padrão '{padrao_arquivos_empresas}'.")
    exit()

print(f"Encontrados {len(lista_arquivos_empresas)} arquivos para processar.")
colunas_empresas = ['CNPJ BÁSICO', 'RAZÃO SOCIAL', 'NATUREZA JURÍDICA', 'QUALIFICAÇÃO RESP.', 'CAPITAL SOCIAL', 'PORTE', 'EFR']
chunks_encontrados = []

for arquivo_empresa in lista_arquivos_empresas:
    print(f"  -> Processando o arquivo: {arquivo_empresa}...")
    try:
        with pd.read_csv(
            arquivo_empresa, header=None, sep=';', names=colunas_empresas,
            encoding='latin-1', dtype=str, chunksize=500000
        ) as reader:
            for i, chunk in enumerate(reader):
                chunk['CNPJ BÁSICO'] = chunk['CNPJ BÁSICO'].str.strip()
                resultado_chunk = chunk[chunk['CNPJ BÁSICO'].isin(cnpjs_interesse)]
                if not resultado_chunk.empty:
                    chunks_encontrados.append(resultado_chunk)
                    print(f"    >>> SUCESSO! Bloco {i+1}: {len(resultado_chunk)} empresa(s) encontrada(s)!")
    except Exception as e:
        print(f"    > Ocorreu um erro ao processar o arquivo {arquivo_empresa}: {e}")

# --- 3. FINALIZAÇÃO ---
print("\n✔ [Passo 3/3] Finalizando o processo...")
if chunks_encontrados:
    df_final = pd.concat(chunks_encontrados, ignore_index=True)
    arquivo_resultado = 'empresas_encontradas_FINAL.csv'
    
    print("\n🎉 Processo concluído com SUCESSO!")
    print(f"Total de {len(df_final)} empresas correspondentes encontradas.")
    df_final.to_csv(arquivo_resultado, index=False, sep=';', encoding='utf-8-sig')
    print(f"💾 Os dados foram salvos no arquivo: '{arquivo_resultado}'")
else:
    print("\n🏁 Processo concluído. Nenhuma empresa correspondente foi encontrada.")