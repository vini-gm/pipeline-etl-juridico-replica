# ⚖️ Pipeline de ETL Jurídico - Réplica PGFN

Este projeto reproduz a arquitetura de Engenharia de Dados utilizada na **Procuradoria-Geral da Fazenda Nacional (PGFN)** para automação de relatórios gerenciais e business intelligence.

O pipeline processa dados brutos de processos judiciais (simulados), aplica regras de negócio complexas (limpeza de valores, padronização de órgãos, regex) e exporta tabelas otimizadas para dashboards.

## 🛠️ Tecnologias Utilizadas
- **Python 3.10+**
- **Pandas & Numpy:** Para manipulação de dados de alta performance (Vectorization).
- **Faker:** Para geração de dados sintéticos realistas.
- **Loguru:** Para observabilidade e logs do processo.

## 🚀 Como Executar

1. **Clone o repositório**
   ```bash
   git clone [https://github.com/vini-gm/replica-pipeline_pgfn]
   cd replica-pipeline_pgfn
   
2. **Gere os dados simulados (Mock)**
   ```bash
    python gerador_mock.py
3. **Rode o Pipeline de ETL**
   ```bash
   python pipeline_etl.py