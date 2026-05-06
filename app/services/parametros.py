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
    """Converte 'abr/2025' → (4, 2025). Retorna None se inválido."""
    if not texto:
        return None
    meses = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,
             "jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}
    m = re.match(r"(\w{3})/(\d{4})", str(texto).strip().lower())
    if m:
        return (meses.get(m.group(1)), int(m.group(2)))
    return None


def ler_parametros(path: str) -> dict:
    """
    Lê a planilha de parâmetros e retorna um dicionário estruturado.
    Inclui lista 'campos_faltantes' com os campos obrigatórios ausentes.
    """
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb["Parâmetros"]

    def v(row, col):
        return ws.cell(row=row, column=col).value

    # Síndico(s): campo B5 (coluna 2)
    sindicos_raw = str(v(5, 2) or "")
    sindicos = [s.strip() for s in sindicos_raw.split(";") if s.strip()]

    params = {
        "cliente":         v(2, 2),
        "cnpj":            v(3, 2),
        "periodo":         v(4, 2),
        "sindicos":        sindicos,
        "isencao_sindico": str(v(6, 2) or "N").strip().upper() == "S",

        # Parâmetros globais (col B, linhas 11-17)
        "taxa_ord_padrao": _to_float(v(11, 2)),
        "dia_vencimento":  v(12, 2),
        "carencia_dias":   int(v(13, 2)) if v(13, 2) else 4,
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

    # Matriz por unidade (linhas 34-83, cols A-D): Unidade | Taxa Ord. | Taxa Extra S/N | Obs
    for row in range(34, 84):
        unidade_raw = v(row, 1)
        if not unidade_raw:
            continue
        try:
            unidade = str(int(float(str(unidade_raw))))
        except (ValueError, TypeError):
            unidade = str(unidade_raw).strip()

        params["unidades"][unidade] = {
            "taxa_ordinaria": _to_float(v(row, 2)),   # None → usa padrão global
            "tem_taxa_extra": str(v(row, 3) or "N").strip().upper() == "S",
            "observacoes":    v(row, 4),
        }

    return params
