"""
Leitura e validação da planilha de parâmetros de cobrança.
Estrutura esperada (linhas do Excel):
  1       Título
  2-7     Identificação (label col A, valor col B)
  8       Separador
  9       Título Parâmetros Globais
  10      Sub-cabeçalho
  11-17   Parâmetros globais (label col A, valor col B)
  18      Separador
  19      Título Taxas Extras
  20      Sub-cabeçalho
  21-30   Taxas extras (cols A-E)
  31      Separador
  32      Título Matriz
  33      Sub-cabeçalho
  34-83   Dados por unidade (cols A-D): Unidade | Taxa Ord. | Taxa Extra S/N | Obs
"""
import re
import openpyxl


class ValidacaoError(Exception):
    """Erro de validação nos arquivos de entrada — mensagem amigável ao usuário."""


CAMPOS_OBRIGATORIOS = [
    ("taxa_ord_padrao",  "Taxa Ordinária Padrão"),
    ("dia_vencimento",   "Dia de Vencimento"),
    ("carencia_dias",    "Carência para Multa (dias)"),
    ("pct_multa",        "% Multa"),
    ("taxa_medicao",     "Taxa Medição e Leitura de Água"),
]


def _to_float(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_periodo(texto):
    """Converte 'abr/2025' ou datetime → (4, 2025). Retorna None se inválido."""
    if not texto:
        return None
    import datetime as _dt
    if isinstance(texto, (_dt.datetime, _dt.date)):
        return (texto.month, texto.year)
    meses = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,
             "jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}
    m = re.match(r"(\w{3})/(\d{4})", str(texto).strip().lower())
    if m:
        return (meses.get(m.group(1)), int(m.group(2)))
    return None


_ANCORAS = [
    (8,  1, "PARÂMETROS GLOBAIS"),
    (10, 1, "Parâmetro *"),
    (10, 2, "Valor"),
    (18, 1, "TAXAS EXTRAS (opcional)"),
    (20, 1, "Nome da Taxa"),
    (20, 2, "Valor (R$)"),
    (20, 3, "Início (mmm/aaaa)"),
    (20, 4, "Fim (mmm/aaaa)"),
    (33, 1, "Unidade"),
    (33, 2, "Taxa Ordinária (R$)"),
]


def ler_parametros(path: str) -> dict:
    """
    Lê a planilha de parâmetros e retorna um dicionário estruturado.
    Inclui lista 'campos_faltantes' com os campos obrigatórios ausentes.
    """
    from app.services.conciliacao import ValidacaoError

    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb["Parâmetros"]

    def v(row, col):
        return ws.cell(row=row, column=col).value

    # Valida âncoras estruturais — detecta linhas ou colunas inseridas/removidas
    for row, col, esperado in _ANCORAS:
        encontrado = str(v(row, col) or "").strip()
        if encontrado != esperado:
            raise ValidacaoError(
                f"A planilha de parâmetros está com estrutura inesperada. "
                f"Esperado na linha {row}: '{esperado}' — encontrado: '{encontrado}'. "
                "Use o modelo original sem inserir ou remover linhas e colunas."
            )

    # Síndico(s): campo B5 (coluna 2)
    # Normaliza para o mesmo formato da 004A: str(int(x)) strip leading zeros
    def _norm_u(s):
        try: return str(int(float(s)))
        except (ValueError, TypeError): return s
    sindicos_raw = str(v(5, 2) or "")
    sindicos = [_norm_u(s.strip()) for s in sindicos_raw.split(";") if s.strip()]

    params = {
        "cliente":         v(2, 2),
        "cnpj":            v(3, 2),
        "periodo":         v(4, 2),
        "sindicos":        sindicos,
        "isencao_sindico": str(v(6, 2) or "N").strip().upper() == "S",

        # Parâmetros globais (col B, linhas 11-17)
        "taxa_ord_padrao": _to_float(v(11, 2)),
        "dia_vencimento":  v(12, 2),
        "carencia_dias":   int(_to_float(v(13, 2)) or 4),
        "pct_multa":       _to_float(v(14, 2)) or 2.0,
        "pct_juros":       _to_float(v(15, 2)) or 1.0,
        "juros_prorata":   str(v(16, 2) or "N").strip().upper() == "S",
        "taxa_medicao":    _to_float(v(17, 2)),

        "taxas_extras": [],
        "unidades":     {},
        "campos_faltantes": [],
    }

    # Valida campos obrigatórios
    for key, label in CAMPOS_OBRIGATORIOS:
        if not params[key]:
            params["campos_faltantes"].append(label)

    # Taxas extras (linhas 21-30, cols A-E)
    for row in range(21, 31):
        nome  = v(row, 1)
        valor = _to_float(v(row, 2))
        if not nome or not valor:
            continue
        inicio = _parse_periodo(v(row, 3))
        fim    = _parse_periodo(v(row, 4))
        params["taxas_extras"].append({
            "nome":   str(nome).strip(),
            "valor":  valor,
            "inicio": inicio,
            "fim":    fim,
            "obs":    v(row, 5),
        })

    # Matriz por unidade: A=Unidade, B=Taxa Ord., C/D/E=S/N por taxa (até 3), F=Obs (fixo)
    n_extras = len(params["taxas_extras"])
    col_obs  = 6  # Observações sempre na col F, independente de quantas taxas existem

    for row in range(34, ws.max_row + 1):
        unidade_raw = v(row, 1)
        if not unidade_raw:
            continue
        try:
            unidade = str(int(float(str(unidade_raw))))
        except (ValueError, TypeError):
            unidade = str(unidade_raw).strip()

        # Uma entrada por taxa extra: {nome_da_taxa: bool}
        taxas_extras_flag = {
            taxa["nome"]: str(v(row, 3 + i) or "N").strip().upper() == "S"
            for i, taxa in enumerate(params["taxas_extras"])
        }

        params["unidades"][unidade] = {
            "taxa_ordinaria":   _to_float(v(row, 2)),
            "taxas_extras_flag": taxas_extras_flag,
            "tem_taxa_extra":   any(taxas_extras_flag.values()),  # atalho para compatibilidade
            "observacoes":      v(row, col_obs),
        }

    return params
