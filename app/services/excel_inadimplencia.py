"""
Parser para relatório de inadimplência exportado em Excel pelo Controlar.
Formato esperado: mesmo layout do PDF "Inadimplência com composição (detalhado)".

Estrutura do Excel:
  Linha 0      Identificação do condomínio
  Linha 1      Título "Inadimplência com composição (detalhado)"
  Linha 2      Parâmetros do relatório
  Linha 3+     Cabeçalho de unidade: "NNNN   - NOME..." (repetido por unidade)
               Cabeçalho de colunas: Venc. | Comp. | Cód. | ... | Descrição | Valor | ...
               Linhas de dados:      DD/MM/AA | MM/YYYY | ... | nome_taxa | valor | ...
"""
import re
import numpy as np
import pandas as pd

_RE_UNIT = re.compile(r'^(\d{3,4})\s*-\s*\S')
_RE_DATE = re.compile(r'^\d{2}/\d{2}/\d{2,4}$')
_RE_COMP = re.compile(r'^\d{2}/\d{4}$')

# Índices das colunas relevantes no layout do Controlar
_COL_COMP  = 1   # Comp.      ex: "04/2025"
_COL_DESC  = 5   # Descrição  ex: "Taxa Ordinária"
_COL_VALOR = 6   # Valor      ex: "1.836,88"


def _norm_unit(s):
    try:
        return str(int(str(s).strip()))
    except (ValueError, TypeError):
        return str(s).strip()


def _cell(row, idx):
    if idx >= len(row):
        return ""
    v = row.iloc[idx]
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    return str(v).strip()


def _br_to_float(s):
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _norm_nome(s: str) -> str:
    return " ".join(str(s).strip().split()).lower()


def parsear_excel_inadimplencia(path: str) -> tuple[list, list]:
    """
    Parseia Excel de inadimplência do Controlar.

    Retorna (registros, erros):
      registros : list[{unidade, competencia, taxa, valor}]
      erros     : list[{pagina, linha, mensagem}]
    """
    try:
        df = pd.read_excel(path, sheet_name=0, header=None, dtype=str)
    except Exception as exc:
        return [], [{"pagina": 0, "linha": 0,
                     "mensagem": f"Erro ao abrir o Excel: {type(exc).__name__}: {exc}"}]

    registros = []
    erros = []
    unidade_atual = None
    encontrou_header = False
    em_resumo = False

    for linha_num, row in df.iterrows():
        primeira = _cell(row, 0)

        if not primeira and all(_cell(row, c) == "" for c in range(len(row))):
            continue

        # Cabeçalho do relatório
        if "Inadimplência com composição" in primeira:
            encontrou_header = True
            continue

        # Seção de resumo — encerra processamento de unidades
        if "Resumo por categoria" in primeira:
            em_resumo = True
            continue
        if em_resumo:
            continue

        # Cabeçalho de coluna (linha "Venc.")
        if primeira == "Venc.":
            continue

        # Cabeçalho de unidade: "0201   - NOME..."
        m = _RE_UNIT.match(primeira)
        if m and not _RE_DATE.match(primeira):
            unidade_atual = _norm_unit(m.group(1))
            continue

        # Linha de total
        if primeira.startswith("Total"):
            continue

        # Linha de dado: primeira célula = vencimento DD/MM/AA ou DD/MM/YYYY
        if unidade_atual and _RE_DATE.match(primeira):
            comp  = _cell(row, _COL_COMP)
            taxa  = _cell(row, _COL_DESC)
            v_raw = _cell(row, _COL_VALOR)

            if not _RE_COMP.match(comp):
                erros.append({"pagina": 0, "linha": linha_num,
                               "mensagem": f'Competência inválida: "{comp}"'})
                continue

            if not taxa:
                continue

            valor = _br_to_float(v_raw) if v_raw else None

            registros.append({
                "unidade":    unidade_atual,
                "competencia": comp,
                "taxa":        taxa,
                "valor":       valor or 0.0,
            })

    if not encontrou_header:
        erros.insert(0, {"pagina": 0, "linha": 0,
                          "mensagem": (
                              'Formato não reconhecido: o arquivo não parece ser um relatório '
                              '"Inadimplência com composição (detalhado)" do Controlar. '
                              'Verifique se o arquivo correto foi enviado.'
                          )})

    if not registros and not erros:
        erros.append({"pagina": 0, "linha": 0,
                       "mensagem": "Nenhuma linha de dados encontrada no Excel."})

    # Deduplica (mesmo boleto pode ter sofrido atualização e aparecer mais de uma vez)
    seen: set = set()
    unicos = []
    for r in registros:
        key = (r["unidade"], r["competencia"], r["taxa"])
        if key not in seen:
            seen.add(key)
            unicos.append(r)

    return unicos, erros


def comparar_excel_com_sistema(excel_registros: list, df_004a: pd.DataFrame) -> list:
    """
    Compara cada item do relatório Excel (unidade + mês + taxa) com a 004A.

    Retorna lista de registros para a aba "Conferência — Excel":
      {Unidade, Competência, Taxa, Valor em aberto (R$), Valor 004A (R$), Situação}
    """
    # Índice normalizado das colunas da 004A para busca por nome
    cols_norm = {_norm_nome(c): c for c in df_004a.columns}

    divergencias = []

    for reg in excel_registros:
        unidade        = reg["unidade"]
        comp           = reg["competencia"]   # "MM/YYYY"
        taxa_nome      = reg["taxa"]
        valor_cliente  = reg["valor"]

        try:
            mes = int(comp[:2])
            ano = int(comp[3:])
        except (ValueError, IndexError):
            continue

        # Linhas da 004A para esta unidade+mês
        mask = (
            (df_004a["Unidade"] == unidade) &
            (df_004a["Vencimento_dt"].dt.month == mes) &
            (df_004a["Vencimento_dt"].dt.year  == ano)
        )
        rows_004a = df_004a[mask]

        # Coluna correspondente na 004A (match por nome normalizado)
        col_004a = cols_norm.get(_norm_nome(taxa_nome))

        base = {
            "Unidade":             unidade,
            "Competência":         comp,
            "Taxa":                taxa_nome,
            "Valor em aberto (R$)": valor_cliente,
        }

        if len(rows_004a) == 0:
            divergencias.append({**base,
                "Valor 004A (R$)": 0.0,
                "Situação": "Boleto ausente na 004A",
            })
            continue

        if col_004a is None:
            divergencias.append({**base,
                "Valor 004A (R$)": "-",
                "Situação": "Taxa não localizada na 004A",
            })
            continue

        valor_004a = float(rows_004a[col_004a].fillna(0).sum())

        if valor_004a < 0.05:
            divergencias.append({**base,
                "Valor 004A (R$)": 0.0,
                "Situação": "Confirmado — sem pagamento na 004A",
            })
        else:
            divergencias.append({**base,
                "Valor 004A (R$)": round(valor_004a, 2),
                "Situação": "Divergência — 004A mostra pagamento registrado",
            })

    return divergencias
