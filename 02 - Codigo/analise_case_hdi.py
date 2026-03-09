from pathlib import Path
import pandas as pd
import numpy as np

BaseDir = Path(__file__).resolve().parent.parent
pd.options.display.float_format = '{:,.2f}'.format

PastaDadosBrutos = BaseDir / '01 - Dados Brutos'
PastaDadosTratados = BaseDir / '03 - Dados Tratados'
PastaDadosTratados.mkdir(exist_ok=True)

df = pd.read_csv(PastaDadosBrutos / 'BaseDados.tab', sep='\t')

def converterFloat(coluna):
    return (coluna.astype(str)
            .str.strip()
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
            .astype(float)
            )

colunasNumericas = ['IS', 'VlrPremio', 'QtdSinistros', 'VlrIndenizacao']
for col in colunasNumericas:
    df[col] = converterFloat(df[col])

df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y')
df['QtdItens'] = 1


premioTotal = df['VlrPremio'].sum()
indenizacaoTotal = df['VlrIndenizacao'].sum()
sinistrosTotal = df['QtdSinistros'].sum()
sinistralidadeGeral = indenizacaoTotal / premioTotal

print(f'''==== RESUMO GERAL ====
Prêmio Total: R$ {premioTotal:,.2f}
Indenização Total: R$ {indenizacaoTotal:,.2f}
Quantidade Total de Sinistros: {int(sinistrosTotal)}
Sinistralidade Geral: {sinistralidadeGeral*100:.2f}%''')


indicadoresRegiao = df.groupby('Regiao').agg(
    Premio_Total=('VlrPremio', 'sum'),
    Indenizacao_Total=('VlrIndenizacao', 'sum'),
    Qtd_Sinistros=('QtdSinistros', 'sum'),
    Qtd_Itens=('QtdItens', 'sum')
).reset_index()

indicadoresRegiao['Premio_Medio'] = indicadoresRegiao['Premio_Total'] / indicadoresRegiao['Qtd_Itens']
indicadoresRegiao['Custo_Medio'] = indicadoresRegiao.apply(
    lambda x: x['Indenizacao_Total'] / x['Qtd_Sinistros'] if x['Qtd_Sinistros'] > 0 else 0, axis=1
)
indicadoresRegiao['Frequencia'] = indicadoresRegiao['Qtd_Sinistros'] / indicadoresRegiao['Qtd_Itens']
indicadoresRegiao['Sinistralidade (%)'] = (indicadoresRegiao['Indenizacao_Total'] / indicadoresRegiao['Premio_Total'].replace(0, pd.NA)) * 100
indicadoresRegiao['Sinistralidade (%)'] = indicadoresRegiao['Sinistralidade (%)'].round(2)
indicadoresRegiao = indicadoresRegiao.sort_values('Sinistralidade (%)', ascending=False)

print(f'\n==== INDICADORES POR REGIÃO ====\n{indicadoresRegiao}')


df['MesReferencia'] = df['Data'].dt.to_period('M').dt.to_timestamp()
dfMensal = df.groupby(['MesReferencia', 'Regiao', 'Produto']).agg(
    SomaPremio=('VlrPremio', 'sum'),
    SomaIndenizacao=('VlrIndenizacao', 'sum'),
    SomaSinistros=('QtdSinistros', 'sum'),
    Exposicao=('Regiao', 'count')
).reset_index()

dfMensal['PremioMedio'] = dfMensal['SomaPremio'] / dfMensal['Exposicao']
dfMensal['Frequencia'] = dfMensal['SomaSinistros'] / dfMensal['Exposicao']
dfMensal['CustoMedio'] = np.where(dfMensal['SomaSinistros'] > 0, dfMensal['SomaIndenizacao'] / dfMensal['SomaSinistros'], 0)


def salvarCSV(dfAlvo, caminhoArquivo):
    if caminhoArquivo.exists():
        print(f"Arquivo já existia e foi sobrescrito: {caminhoArquivo.name}")
    else:
        print(f"Arquivo criado: {caminhoArquivo.name}")
    dfAlvo.to_csv(caminhoArquivo, index=False, sep=',', float_format='%.2f')

salvarCSV(df, PastaDadosTratados / 'Base_Tratada.csv')
salvarCSV(indicadoresRegiao, PastaDadosTratados / 'Indicadores_Por_Regiao.csv')
salvarCSV(dfMensal, PastaDadosTratados / 'IndicadoresMensais.csv')
