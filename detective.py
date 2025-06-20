import pandas as pd
import glob

print("--- Comparação Direta de Amostras de CNPJ ---")

cnpj_amostra_socio = None
cnpj_amostra_empresa = None

# --- PASSO 1: Extrair uma amostra do arquivo de Sócios ---
try:
    print("\n[1/3] Lendo o arquivo de Sócios...")
    df_socios = pd.read_csv(
        'socios.csv',
        header=None, sep=';', engine='python', encoding='utf-8-sig',
        names=['CNPJ BÁSICO', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10', 'Col11'],
        dtype=str
    )
    # Pega o primeiro CNPJ da lista após a limpeza
    cnpj_amostra_socio = df_socios.loc[0, 'CNPJ BÁSICO'].strip()
    
    print("✔ Amostra do arquivo de Sócios extraída com sucesso.")
    print("--- Detalhes da Amostra SÓCIO (Linha 1) ---")
    print(f"  - Valor exato lido: {repr(cnpj_amostra_socio)}")
    print(f"  - Tamanho (deve ser 8): {len(cnpj_amostra_socio)}")

except Exception as e:
    print(f"🚨 ERRO ao ler o arquivo de sócios: {e}")

# --- PASSO 2: Extrair uma amostra do primeiro arquivo de Empresas ---
try:
    print("\n[2/3] Lendo o primeiro arquivo de Empresas...")
    # Pega o primeiro arquivo da lista (ex: empresas0.csv ou empresas1.csv)
    primeiro_arquivo_empresa = sorted(glob.glob('empresas*.csv'))[0]
    print(f"Usando o arquivo '{primeiro_arquivo_empresa}' para a amostra.")

    with pd.read_csv(
        primeiro_arquivo_empresa, header=None, sep=';', encoding='latin-1',
        names=['CNPJ BÁSICO', 'RAZÃO SOCIAL', 'etc...'], dtype=str, chunksize=10
    ) as reader:
        first_chunk = next(reader)
        # Pega o primeiro CNPJ do chunk após a limpeza
        cnpj_amostra_empresa = first_chunk.loc[0, 'CNPJ BÁSICO'].strip()

        print("✔ Amostra do arquivo de Empresas extraída com sucesso.")
        print("--- Detalhes da Amostra EMPRESA (Linha 1 de {primeiro_arquivo_empresa}) ---")
        print(f"  - Valor exato lido: {repr(cnpj_amostra_empresa)}")
        print(f"  - Tamanho (deve ser 8): {len(cnpj_amostra_empresa)}")

except Exception as e:
    print(f"🚨 ERRO ao ler o arquivo de empresas: {e}")

# --- PASSO 3: Comparação Direta ---
print("\n[3/3] Comparando as duas amostras...")
if cnpj_amostra_socio is not None and cnpj_amostra_empresa is not None:
    # Verificação final: forçar os dois a terem 8 dígitos, com zeros à esquerda.
    # Esta é a forma mais robusta de comparar.
    socio_final = cnpj_amostra_socio.zfill(8)
    empresa_final = cnpj_amostra_empresa.zfill(8)
    
    print(f"Comparando Socio ('{socio_final}') com Empresa ('{empresa_final}')")
    
    if socio_final == empresa_final:
        print("✔ RESULTADO: As amostras SÃO IGUAIS.")
    else:
        print("❌ RESULTADO: As amostras SÃO DIFERENTES.")
else:
    print("Não foi possível extrair ambas as amostras para comparação.")

print("\n--- Diagnóstico Concluído ---")