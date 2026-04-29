# CLAUDE.md — Projeto Condomínio: Conciliação de Arquivos Excel

## Visão Geral
Site Flask para conciliação de planilhas Excel. O usuário carrega duas planilhas
(parâmetros e dados), um script de análise processa a conciliação e a tela exibe
um resumo gerencial com opção de download do Excel resultante.

## Stack Técnica
- **Backend:** Python 3.14 + Flask
- **Processamento:** pandas, openpyxl, xlsxwriter
- **Frontend:** HTML/CSS/JS (sem framework pesado — vanilla ou mínimo Bootstrap)
- **Ambiente:** `.venv/` — sempre ativar antes de executar (`source .venv/bin/activate`)

## Estrutura de Diretórios
```
02_Condomínio/
├── app/
│   ├── routes/          # Blueprints Flask (upload, análise, download)
│   ├── services/        # Lógica de negócio e conciliação
│   ├── templates/       # HTML Jinja2
│   └── static/          # CSS e JS
├── uploads/             # Planilhas recebidas (não versionar conteúdo)
├── outputs/             # Excel gerado para download (não versionar conteúdo)
├── tests/               # Testes unitários
├── docs/                # Documentação e exemplos de planilhas
├── .venv/               # Ambiente virtual (não versionado)
├── requirements.txt
├── run.py               # Ponto de entrada
└── .env                 # Variáveis de ambiente (não versionado)
```

## Convenções de Código
- Funções de serviço em `app/services/` — sem lógica de negócio nas rotas
- Rotas apenas recebem request, chamam service, retornam response
- Nomes de variáveis e funções em **português** (contexto do domínio) ou inglês técnico — ser consistente
- Sem comentários óbvios; comentar apenas regras de negócio não evidentes
- Testes em `tests/` com pytest

## Parâmetros de Análise
Os parâmetros de conciliação serão definidos pelo usuário em sessão futura.
Quando informados, registrar aqui e em `docs/parametros.md`.

## Git / GitHub
- Repositório: `https://github.com/geovasconcelos/condominio-conciliacao`
- Branch padrão: `main`
- Nunca versionar: `.venv/`, `uploads/`, `outputs/`, `.env`, `*.pyc`, `__pycache__/`
- Commits em português, descritivos e concisos

## Comandos Frequentes
```bash
# Ativar ambiente
source .venv/bin/activate

# Rodar servidor de desenvolvimento
python run.py

# Instalar dependências
pip install -r requirements.txt

# Rodar testes
pytest tests/
```
