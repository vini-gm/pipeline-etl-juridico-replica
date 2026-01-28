import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger

ARQUIVO_ENTRADA = "dados_brutos_simulados.csv"  # O arquivo gerado pelo mock
PASTA_SAIDA = Path("output_relatorios")

MAPA_ORGAOS = {
    '1T': 'PRIMEIRA TURMA', 'T1': 'PRIMEIRA TURMA', 'PRIMEIRATURMA': 'PRIMEIRA TURMA', '1TURMA': 'PRIMEIRA TURMA',
    '2T': 'SEGUNDA TURMA', 'T2': 'SEGUNDA TURMA', 'SEGUNDATURMA': 'SEGUNDA TURMA', '2TURMA': 'SEGUNDA TURMA',
    '3T': 'TERCEIRA TURMA', 'T3': 'TERCEIRA TURMA', 'TERCEIRATURMA': 'TERCEIRA TURMA', '3TURMA': 'TERCEIRA TURMA',
    '4T': 'QUARTA TURMA', 'T4': 'QUARTA TURMA', 'QUARTATURMA': 'QUARTA TURMA', '4TURMA': 'QUARTA TURMA',
    '5T': 'QUINTA TURMA', 'T5': 'QUINTA TURMA', 'QUINTATURMA': 'QUINTA TURMA', '5TURMA': 'QUINTA TURMA',
    '6T': 'SEXTA TURMA', 'T6': 'SEXTA TURMA', 'SEXTATURMA': 'SEXTA TURMA', '6TURMA': 'SEXTA TURMA',
    'CE': 'CORTE ESPECIAL', 'CORTEESPECIAL': 'CORTE ESPECIAL',
    '1S': 'PRIMEIRA SEÇÃO', 'S1': 'PRIMEIRA SEÇÃO', 'PRIMEIRASEÇÃO': 'PRIMEIRA SEÇÃO', '1SEÇÃO': 'PRIMEIRA SEÇÃO',
    '2S': 'SEGUNDA SEÇÃO', 'S2': 'SEGUNDA SEÇÃO', 'SEGUNDASEÇÃO': 'SEGUNDA SEÇÃO', '2SEÇÃO': 'SEGUNDA SEÇÃO',
    '3S': 'TERCEIRA SEÇÃO', 'S3': 'TERCEIRA SEÇÃO', 'TERCEIRASEÇÃO': 'TERCEIRA SEÇÃO', '3SEÇÃO': 'TERCEIRA SEÇÃO',
    '4S': 'QUARTA SEÇÃO', 'S4': 'QUARTA SEÇÃO', 'QUARTASEÇÃO': 'QUARTA SEÇÃO', '4SEÇÃO': 'QUARTA SEÇÃO',
    '5S': 'QUINTA SEÇÃO', 'S5': 'QUINTA SEÇÃO', 'QUINTASEÇÃO': 'QUINTA SEÇÃO', '5SEÇÃO': 'QUINTA SEÇÃO'
}

class RelatorioJuridico:
    """
    Classe responsável pelo pipeline de ETL dos processos do Núcleo de Inteligência de Dados.
    Simula a arquitetura original utilizada na PGFN, adaptada para dados sintéticos.
    """

    def __init__(self, arquivo_entrada):
        self.arquivo_entrada = Path(arquivo_entrada)
        self.df_base = pd.DataFrame()
        self.resultados = {}  # Armazena os DFs finais para exportação
        self.calend_d_uteis = pd.DataFrame()

        if not PASTA_SAIDA.exists():
            PASTA_SAIDA.mkdir()

    def carregar_dados(self):
        """Etapa de Extração (Extraction)"""
        logger.info(f"Carregando dados de: {self.arquivo_entrada}")

        if not self.arquivo_entrada.exists():
            logger.error("Arquivo de entrada não encontrado. Rode o gerador_dados.py primeiro.")
            raise FileNotFoundError("Base de dados mock não encontrada.")
        self.df_base = pd.read_csv(self.arquivo_entrada, dtype=str)
        logger.info(f"Dados carregados: {len(self.df_base)} registros.")

    def _aplicar_vetorizacao(self):
        """
        Etapa de Transformação (Transformation) OTIMIZADA.
        Substitui o uso de .apply() por métodos vetorizados do Pandas/Numpy.
        """
        logger.info("Aplicando regras de negócio (Vetorização)...")
        df = self.df_base

        val_temp = df['Valor da causa'].str.split('\n').str[-1]
        val_temp = val_temp.str.replace('R$', '', regex=False) \
            .str.replace('.', '', regex=False) \
            .str.replace(',', '.', regex=False) \
            .str.strip()
        df['Valor da causa'] = pd.to_numeric(val_temp, errors='coerce').fillna(0.0)

        orgao_clean = df['Órgão Julgador'].str.replace(r'[\sªº]', '', regex=True).str.upper()
        df['Órgão Padronizado'] = orgao_clean.map(MAPA_ORGAOS)
        mask_nulo = df['Órgão Padronizado'].isna()
        mask_pres = df['Relator'].str.upper().str.contains('PRESIDENTE', na=False)
        df.loc[mask_nulo & mask_pres, 'Órgão Padronizado'] = df.loc[mask_nulo & mask_pres, 'Relator']
        df['Órgão Padronizado'] = df['Órgão Padronizado'].fillna(df['Órgão Julgador'])

        split_ufs = df['UF'].str.strip().str.split('\n', expand=True)
        df['UF_1'] = split_ufs[0].fillna('')
        df['UF_2'] = split_ufs[1].fillna('') if split_ufs.shape[1] > 1 else ''

        df['Status Normalizado'] = np.where(
            df['Situação do processo'].str.strip().str.upper().str.startswith('CONCLUÍDO'),
            'CONCLUÍDO',
            'PENDENTE'
        )
        df['Data da Extração'] = pd.to_datetime(df['Data da Extração'], dayfirst=True, errors='coerce')

    def _gerar_calendario(self):
        """Gera calendário de dias úteis baseado nas datas encontradas"""
        datas = self.df_base['Data da Extração'].dropna()
        if datas.empty: return

        dt_range = pd.date_range(start=datas.min(), end=datas.max(), freq='B')
        self.calend_d_uteis = pd.DataFrame({'Data': dt_range})

    def _gerar_tabela_saj_mae(self):
        """
        Gera tabela dimensional explodida por Matéria.
        Técnica: Explode (Transforma lista em linhas).
        """
        logger.debug("Gerando tabela dimensional SAJ/MAE...")
        df = self.df_base.copy()
        if 'Código Matéria SAJ' not in df.columns:
            df['Código Matéria SAJ'] = '1.2.3|4.5.6'
        # Transforma sTring "1.2.3|4.5.6" em lista ["1.2.3", "4.5.6"]
        df['Lista_Materias'] = df['Código Matéria SAJ'].str.split('|')
        # Explode: Cria uma linha para cada código de matéria
        df_explode = df.explode('Lista_Materias')

        cols = ['Data da Extração', 'Número', 'Órgão Padronizado', 'Lista_Materias']
        self.resultados['dim_materias_mae'] = df_explode[cols].dropna()

    def _gerar_tabela_uf(self):
        """
        Gera tabela unificada de UFs.
        Técnica: Melt (Unpivot - Transforma colunas UF1/UF2 em linhas).
        """
        logger.debug("Gerando tabela dimensional de UFs...")

        # Transforma colunas largas em linhas longas
        df_melted = self.df_base.melt(
            id_vars=['Data da Extração', 'Número', 'Valor da causa'],
            value_vars=['UF_1', 'UF_2'],
            value_name='UF_Unificada'
        )

        # Remove vazios
        df_final = df_melted[df_melted['UF_Unificada'] != ''].copy()
        self.resultados['dim_regionalizacao_uf'] = df_final

    def _gerar_tabela_polo(self):
        """Gera tabela filtrada apenas para Polos Relevantes"""
        logger.debug("Gerando tabela de Polo PGFN...")
        df = self.df_base.copy()

        filtro_polo = df['Polo da PFGN'].str.upper().isin(['AUTOR', 'RÉU'])
        df_final = df[filtro_polo][['Data da Extração', 'Número', 'Polo da PFGN']]

        self.resultados['dim_polo_pgfn'] = df_final

    # ----------------------------------------------
    def processar_relatorios(self):
        """Orquestrador Principal da Transformação"""
        # Deduplicação
        self.df_base.drop_duplicates(subset=['Número', 'Classe'], keep='last', inplace=True)

        # 1. Transformações Gerais (Base)
        self._aplicar_vetorizacao()
        self._gerar_calendario()

        # 2. Geração das Tabelas (Fato e Dimensões)
        self._gerar_performance_procurador()
        self._gerar_base_analitica()

        # Chamada dos novos métodos
        self._gerar_tabela_saj_mae()
        self._gerar_tabela_uf()
        self._gerar_tabela_polo()

    def _gerar_performance_procurador(self):
        logger.debug("Calculando performance (com dias zerados)...")

        # 1. Filtra dados válidos
        df = self.df_base.dropna(subset=['Data da Extração'])
        # 2. Agregação inicial (Só quem trabalhou)
        df_agg = df.groupby(['Data da Extração', 'Procurador Responsável']).size().reset_index(name='Qtd_Processos')
            # 3. Lógica "Cross Join" para mostrar dias com 0 processos
        if not self.calend_d_uteis.empty:
            procuradores = pd.DataFrame({'Procurador Responsável': df['Procurador Responsável'].unique()})
            datas = self.calend_d_uteis.copy().rename(columns={'Data': 'Data da Extração'})
            datas['key'] = 1
            procuradores['key'] = 1
            template_completo = pd.merge(datas, procuradores, on='key').drop('key', axis=1)
            df_final = pd.merge(template_completo, df_agg, on=['Data da Extração', 'Procurador Responsável'],
                                how='left')
            df_final['Qtd_Processos'] = df_final['Qtd_Processos'].fillna(0).astype(int)
            self.resultados['performance'] = df_final
        else:
            self.resultados['performance'] = df_agg


    def _gerar_base_analitica(self):
        logger.debug("Gerando base analítica final...")
        cols = ['Data da Extração', 'Número', 'Classe', 'Procurador Responsável',
                'Órgão Padronizado', 'Valor da causa', 'Status Normalizado', 'UF_1', 'UF_2']
        self.resultados['base_analitica'] = self.df_base[cols].copy()

    def exportar_dados(self):
        """Etapa de Carga (Load)"""
        logger.info(f"Salvando resultados em {PASTA_SAIDA}...")

        for nome, df in self.resultados.items():
            if df.empty: continue

            # Formata datas para string ISO antes de salvar
            for col in df.select_dtypes(include=['datetime64']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d')

            caminho = PASTA_SAIDA / f"{nome}.csv"
            df.to_csv(caminho, index=False, encoding='utf-8-sig')
            logger.success(f"Arquivo gerado: {caminho.name}")


def main():
    logger.info(">>> INICIANDO PIPELINE PGFN (MOCK) <<<")
    try:

        etl = RelatorioJuridicoNID(ARQUIVO_ENTRADA)
        # Executa o Pipeline
        etl.carregar_dados()
        etl.processar_relatorios()
        etl.exportar_dados()

        logger.success("Pipeline finalizado com sucesso.")

    except Exception as e:
        logger.exception(f"Erro crítico no pipeline: {e}")


if __name__ == "__main__":
    main()