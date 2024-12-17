import polars as pl
from datetime import datetime
import numpy as np
from matplotlib import pyplot as plt

ENDERECO_DADOS = r'./dados/'

# LENDO OS DADOS DO ARQUIVO PARQUET

try:
    print('\n Lendo arquivos')

    # Marca o Tempo inicial
    inicio = datetime.now()

    # Scan_parquet: Cria um plano de execução preguiçoso para a leitura do parquet
    df_scan_bolsa_familia = pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

    # Executa as operações lazys e coleta os resultados
    df_bolsa_familia = df_scan_bolsa_familia.collect()

    print(df_bolsa_familia)

    # Pega o tempo final
    fim = datetime.now()

    print(f'Tempo de execução para leitura do parquet: {fim - inicio}')
    print('\nArquivo parquet lido com sucesso!')

except ImportError as e: 
    print(f'Erro ao ler os dados do parquet: {e}')


try:
    print('Analisando as medidas posicão')

    array_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])

    media_parcela = np.mean(array_parcela)
    mediana_parcela = np.median(array_parcela)
    media_mediana = abs(media_parcela - mediana_parcela)

    print('\nMEDIDAS DE TENDÊNCIA CENTRAL')
    print(f'Média: {media_parcela:.2f}')
    print(f'Mediana: {mediana_parcela:.2f}')
    print(f'Distância: {media_mediana:.2f}')
    print(30*'=')

    max = np.max(array_parcela)
    min = np.min(array_parcela)
    amplitude_total = max - min

    print('\nMEDIDAS DE DISPERSÃO')
    print('Máximo: ', max)
    print('Mínimo: ', min)
    print('Amplitude Total: ', amplitude_total)
    print(30*'=')

    q1 = np.quantile(array_parcela, 0.25, method='weibull')
    q2 = np.quantile(array_parcela, 0.50, method='weibull')
    q3 = np.quantile(array_parcela, 0.75, method='weibull')
    iqr = q3 - q1
    lmt_sup = q3 + (1.5 * iqr)
    lmt_inf = q1 + (1.5 * iqr)

    print('\nMEDIDAS DE POSIÇÃO')
    print('Mínimo: ', min)
    print(f'Limite Inferior: {lmt_inf:.2f}')
    print('Q1 (25%): ', q1)
    print('Q2 (50%): ', q2)
    print('Q3 (75%): ', q3)
    print(f'IQR: {iqr:.2f}')
    print(f'Limite Superior: {lmt_sup:.2f}')
    print('Máximo: ', max)
    print(30*'=')


except ImportError as e: 
    print(f'Erro ao ler os dados do parquet: {e}')