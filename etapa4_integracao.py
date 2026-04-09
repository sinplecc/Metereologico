import pandas as pd

# Lista de arquivos da secretaria
arquivos = [
    'dados_secretaria/dados2021.csv',
    'dados_secretaria/dados2022.csv',
    'dados_secretaria/dados2023.csv',
    'dados_secretaria/dados2024.csv',
    'dados_secretaria/dados2025.csv',
    'dados_secretaria/dados2026.csv',
]

dfs = []
# Leitura em chunks para evitar estouro de RAM
for arq in arquivos:
    for chunk in pd.read_csv(arq, sep=';', encoding='latin1', chunksize=50000):
        # Padronizar nomes das colunas
        chunk.columns = chunk.columns.str.lower().str.strip()

        # Remover colunas inúteis (ex: Unnamed)
        chunk = chunk.loc[:, ~chunk.columns.str.contains('^unnamed')]
        chunk = chunk.loc[:, chunk.columns != '...']

        # Converter coluna de data
        chunk = chunk.dropna(subset=['data fato'])
        chunk['data fato'] = pd.to_datetime(chunk['data fato'], errors='coerce', dayfirst=True)
        chunk = chunk.dropna(subset=['data fato'])

        # Padronizar colunas de texto
        for col in ['tipo fato', 'grupo fato', 'municipio fato', 'bairro', 'local fato', 'tipo enquadramento']:
            if col in chunk.columns:
                chunk[col] = chunk[col].astype(str).str.lower().str.strip()

        dfs.append(chunk)

# Unificar todos os chunks
df_crimes = pd.concat(dfs, ignore_index=True)

# Remover duplicatas
df_crimes = df_crimes.drop_duplicates()

# Filtrar apenas dados de Passo Fundo
df_pf = df_crimes[df_crimes['municipio fato'] == 'passo fundo'].copy()

# Criar coluna auxiliar para merge
df_pf['cidade'] = 'passo fundo'

# Agrupar os dados por dia (nível diário para cruzar com meteorologia)
df_diario = (
    df_pf.groupby(['data fato', 'cidade'], as_index=False)
    .agg(
        ocorrencias=('data fato', 'count'),
        vitimas_total=('quantidade vítimas', 'sum'),
        idade_media_vitima=('idade vítima', 'mean'),
        
        # Contagem de tipos específicos de crime
        furtos=('tipo enquadramento', lambda x: x.str.contains('furto', na=False).sum()),
        roubos=('tipo enquadramento', lambda x: x.str.contains('roubo', na=False).sum()),
        homicidios=('tipo enquadramento', lambda x: x.str.contains('homic', na=False).sum())
    )
)

# Leitura do arquivo meteorológico (pulando metadados iniciais)
df_meteo = pd.read_csv(
    'dados_tempo21-25/pfdados.csv',
    sep=';',
    encoding='latin1',
    skiprows=10
)

# Padronizar nomes das colunas (remover acentos e inconsistências)
df_meteo.columns = (
    df_meteo.columns
    .str.lower()
    .str.strip()
    .str.normalize('NFKD')
    .str.encode('ascii', errors='ignore')
    .str.decode('utf-8')
)

# Remover colunas vazias ou inúteis
df_meteo = df_meteo.loc[:, ~df_meteo.columns.str.contains('^unnamed')]
if '' in df_meteo.columns:
    df_meteo = df_meteo.drop(columns=[''])

# DEBUG (depois lembrar de tirar durante entrega final)
print(df_meteo.columns.tolist())

# Criar mapeamento automático de colunas
mapa = {}

for col in df_meteo.columns:
    if 'data' in col:
        mapa[col] = 'data fato'
    elif 'precipitacao' in col:
        mapa[col] = 'precipitacao'
    elif 'temperatura maxima' in col:
        mapa[col] = 'temperatura_maxima'
    elif 'temperatura minima' in col:
        mapa[col] = 'temperatura_minima'
    elif 'umidade relativa' in col:
        mapa[col] = 'umidade_relativa'
    elif 'vento' in col and 'velocidade' in col:
        mapa[col] = 'vento_velocidade'

# Aplicar renomeação
df_meteo = df_meteo.rename(columns=mapa)

# Converter data
df_meteo['data fato'] = pd.to_datetime(df_meteo['data fato'], errors='coerce')

# Converter colunas numéricas (corrigir vírgula decimal)
for col in ['precipitacao', 'temperatura_maxima', 'temperatura_minima', 'umidade_relativa', 'vento_velocidade']:
    if col in df_meteo.columns:
        df_meteo[col] = (
            df_meteo[col]
            .astype(str)
            .str.replace(',', '.', regex=False)
            .str.strip()
        )
        df_meteo[col] = pd.to_numeric(df_meteo[col], errors='coerce')

# Criar temperatura média
if 'temperatura_maxima' in df_meteo.columns and 'temperatura_minima' in df_meteo.columns:
    df_meteo['temperatura_media'] = (
        df_meteo['temperatura_maxima'] + df_meteo['temperatura_minima']
    ) / 2
    
# Criar coluna cidade
df_meteo['cidade'] = 'passo fundo'

# Remover duplicatas
df_meteo = df_meteo.drop_duplicates(subset=['data fato', 'cidade'])

# Merge entre criminalidade e meteorologia
df_integrado = pd.merge(
    df_diario,
    df_meteo[['data fato', 'cidade', 'precipitacao', 'temperatura_maxima',
              'temperatura_minima', 'temperatura_media',
              'umidade_relativa', 'vento_velocidade']],
    on=['data fato', 'cidade'],
    how='left'
)

# Como a meteorologia é diária, o mais consistente é interpolar por data.
df_integrado = df_integrado.sort_values('data fato')

colunas_meteo = [
    'precipitacao', 'temperatura_maxima', 'temperatura_minima',
    'temperatura_media', 'umidade_relativa', 'vento_velocidade'
]

# Interpolação temporal (ideal para dados contínuos como clima)
for col in colunas_meteo:
    if col in df_integrado.columns:
        df_integrado[col] = df_integrado[col].interpolate(limit_direction='both')

# Preencher valores restantes com mediana
for col in colunas_meteo:
    if col in df_integrado.columns:
        df_integrado[col] = df_integrado[col].fillna(df_integrado[col].median())

print(df_integrado.info())
print(df_integrado.head())

# Organizar antes de salvar
df_integrado = df_integrado.sort_values('data fato').reset_index(drop=True)

# Salvar arquivo final
df_integrado.to_csv('etapa4_dados_integrados.csv', index=False)

print("Etapa 4 concluída com sucesso!")
