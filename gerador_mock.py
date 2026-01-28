import pandas as pd
from faker import Faker
import random

# Configuração
fake = Faker('pt_BR')
QTD_REGISTROS = 500  # Gera uma base robusta para o teste de performance

print(f">>> GERANDO {QTD_REGISTROS} PROCESSOS SINTÉTICOS COM DADOS 'SUJOS' <<<")

# Simulando uma equipe de 12 Procuradores
LISTA_PROCURADORES = [fake.name() for _ in range(12)]
# Simulando uma corte com 15 Desembargadores/Relatores
LISTA_RELATORES = [f"Des. {fake.first_name()} {fake.last_name()}" for _ in range(15)]
print(f"-> Equipe Simulada: {len(LISTA_PROCURADORES)} Procuradores")
print(f"-> Corte Simulada: {len(LISTA_RELATORES)} Relatores")

def gerar_valor_sujo():
    """
    Simula o erro comum de sistemas jurídicos onde o histórico de valores
    vem concatenado com quebra de linha.
    Ex: 'R$ 10.000,00\nR$ 12.500,50'
    """
    val1 = round(random.uniform(1000, 50000), 2)
    if random.random() > 0.8:  # 20% de chance de ter histórico sujo
        val2 = round(val1 * 1.1, 2)
        return f"R$ {val1:,.2f}\nR$ {val2:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
    return f"R$ {val1:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')


def gerar_uf_suja():
    """
    Simula campo de UF com múltiplos estados (quebra de linha).
    Ex: 'DF\nSP'
    """
    uf1 = fake.state_abbr()
    if random.random() > 0.9:  # 10% de chance de ser multi-estado
        uf2 = fake.state_abbr()
        return f"{uf1}\n{uf2}"
    return uf1


def gerar_orgao_variado():
    """
    Gera variações para testar o mapa de normalização.
    Mistura siglas ('1T') com nomes extensos ('Primeira Turma').
    """
    opcoes = [
        '1T', 'T1', '1ª Turma', 'PRIMEIRA TURMA', 'PRIMEIRATURMA', '1  Turma',
        '2T', 'T2', '2ª Turma', 'SEGUNDA TURMA', 'SEGUNDATURMA',
        '3T', 'T3', '3ª Turma', 'TERCEIRA TURMA', 'TERCEIRATURMA',
        '4T', 'T4', '4ª Turma', 'QUARTA TURMA', 'QUARTATURMA',
        '5T', 'T5', '5ª Turma', 'QUINTA TURMA', 'QUINTATURMA',
        '6T', 'T6', '6ª Turma', 'SEXTA TURMA', 'SEXTATURMA',
        'CE', 'Corte Especial', 'CORTE ESPECIAL', 'CORTEESPECIAL',
        '1S', 'S1', '1ª Seção', 'PRIMEIRA SEÇÃO', 'PRIMEIRASEÇÃO',
        '2S', 'S2', '2ª Seção', 'SEGUNDA SEÇÃO', 'SEGUNDASEÇÃO',
        '3S', 'S3', '3ª Seção', 'TERCEIRA SEÇÃO', 'TERCEIRASEÇÃO',
        'Não Informado', '', ' '
    ]
    return random.choice(opcoes)

def gerar_processo():
    data_extracao = fake.date_between(start_date='-60d', end_date='today')

    # Sorteia um dos procuradores da lista fixa. random.choice garante distribuição uniforme
    procurador = random.choice(LISTA_PROCURADORES)
    relator = random.choice(LISTA_RELATORES)

    return {
        "Data da Extração": data_extracao.strftime('%d/%m/%Y'),
        "Número": f"{random.randint(1000000, 9999999)}-{random.randint(10, 99)}.{random.randint(2020, 2025)}.4.01.{random.randint(3000, 4000)}",
        "Classe": random.choice(['Apelação Cível', 'Agravo de Instrumento', 'Execução Fiscal', 'Mandado de Segurança']),
        "Procurador Responsável": procurador,
        "Relator": relator,
        "Órgão Julgador": gerar_orgao_variado(),
        "Valor da causa": gerar_valor_sujo(),
        "UF": gerar_uf_suja(),
        "Polo da PFGN": random.choice(['Autor', 'Réu', 'Terceiro', 'Assistente']),
        "Situação do processo": random.choice([
            'CONCLUÍDO - SENTENÇA', 'CONCLUÍDO - ACÓRDÃO',
            'PENDENTE DE ANÁLISE', 'AGUARDANDO PRAZO', 'TRIAGEM'
        ]),
        "Código Matéria SAJ": f"1.{random.randint(1, 5)}.{random.randint(1, 9)}|2.{random.randint(1, 5)}.{random.randint(1, 9)}" if random.random() > 0.7 else f"1.{random.randint(1, 5)}.{random.randint(1, 9)}"
    }

# Gera a lista de dicionários
dados = [gerar_processo() for _ in range(QTD_REGISTROS)]
df = pd.DataFrame(dados).astype(str) # Cria DataFrame e Salva tudo como str para simular leitura de CSV bruto/Excel

arquivo_saida = "dados_brutos_simulados.csv"
df.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')

print(f"\n[SUCESSO] Base gerada! Verifique a distribuição no arquivo {arquivo_saida}:")
print(df['Procurador Responsável'].value_counts().head()) # Mostra quantos processos cada um pegou
print(df[['Órgão Julgador', 'Valor da causa', 'UF']].head())