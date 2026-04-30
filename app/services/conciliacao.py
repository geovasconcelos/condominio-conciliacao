"""
Serviço de análise e conciliação de planilhas de cobrança condominial.
"""
import os
import numpy as np
import pandas as pd


# ── Helpers de conversão ───────────────────────────────────────────────────────

def _br_to_float(s):
    if pd.isna(s):
        return np.nan
    s = str(s).strip()
    neg = s.startswith("(") and s.endswith(")")
    s = s.replace("(", "").replace(")", "").replace(".", "").replace(",", ".")
    try:
        return float(s) * (-1 if neg else 1)
    except ValueError:
        return np.nan


def _parse_date(s):
    if pd.isna(s):
        return pd.NaT
    try:
        return pd.to_datetime(str(s).strip(), dayfirst=True)
    except Exception:
        return pd.NaT


NUMERIC_COLS = [
    "Tarifa Liquidação Boleto", "Taxa de Água", "Medição e Leitura de Água",
    "Taxa Ordinária", "Taxa Extra -  Modernização Elevadores",
    "Taxa Extra Manut. Piscina", "Taxa Extra - Melhorias no Condomínio",
    "Taxa Extra - Aquisição de Gerador",
    "Taxa Extra  - Reforma - Aquisição - Equipamentos Academia",
    "Receita com Multas", "Outros", "Total",
]


# ── Carregamento e limpeza ─────────────────────────────────────────────────────

def _carregar(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, header=3)
    df = df[df["Tipo Lançamento"].notna()].copy()

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_br_to_float)

    df["Vencimento_dt"] = df["Vencimento"].apply(_parse_date)
    df["Credito_dt"]    = df["Crédito"].apply(_parse_date)
    df["dias_atraso"]   = (df["Credito_dt"] - df["Vencimento_dt"]).dt.days
    df["Unidade"]       = df["Unidade/Bloco"].apply(
        lambda x: str(int(x)) if pd.notna(x) else ""
    )
    return df


# ── Análise principal ──────────────────────────────────────────────────────────

def processar_conciliacao(path_dados: str, session_id: str, output_dir: str) -> dict:
    df = _carregar(path_dados)

    df_normal = df[df["Tipo Cobrança"] == "NORMAL"].copy()
    df_extra  = df[df["Tipo Cobrança"] == "EXTRA"].copy()
    df_acordo = df[df["Tipo Cobrança"] == "ACORDO"].copy()

    # ── Visão geral ────────────────────────────────────────────────────────────
    total_registros  = len(df)
    total_unidades   = df["Unidade"].nunique()
    periodo_inicio   = df["Vencimento_dt"].min()
    periodo_fim      = df["Vencimento_dt"].max()
    total_emissao    = df["Total"].sum()

    # ── Dados ausentes ─────────────────────────────────────────────────────────
    campos_criticos  = ["Unidade", "Vencimento_dt", "Credito_dt", "Taxa Ordinária", "Total"]
    nulos            = {c: int(df[c].isna().sum()) for c in campos_criticos}
    tem_nulos        = any(v > 0 for v in nulos.values())

    # ── Consistência da Taxa Ordinária ─────────────────────────────────────────
    taxa_stats = (
        df_normal.groupby("Unidade")["Taxa Ordinária"]
        .agg(["mean", "std", "min", "max", "count"])
        .reset_index()
    )
    taxa_stats["inconsistente"] = taxa_stats["std"].fillna(0) > 0
    unidades_taxa_inconsistente = taxa_stats[taxa_stats["inconsistente"]]["Unidade"].tolist()
    taxas_unicas = sorted(df_normal["Taxa Ordinária"].dropna().unique().tolist())

    # ── Análise de atrasos ─────────────────────────────────────────────────────
    # Dias 1-4 = compensação bancária normal de boleto; ≥5 dias = atraso real
    GRACE = 4

    df_normal["atrasado_real"] = df_normal["dias_atraso"] > GRACE
    df_normal["tem_multa"]     = df_normal["Receita com Multas"] > 0

    total_normal          = len(df_normal)
    no_prazo              = int((df_normal["dias_atraso"] <= 0).sum())
    compensacao_bancaria  = int(((df_normal["dias_atraso"] > 0) & (df_normal["dias_atraso"] <= GRACE)).sum())
    atrasados_reais       = int(df_normal["atrasado_real"].sum())

    # Atrasados reais sem multa (problema genuíno)
    criticos = df_normal[df_normal["atrasado_real"] & ~df_normal["tem_multa"]].copy()
    total_criticos = len(criticos)

    # Atrasados reais com multa
    com_multa_df = df_normal[df_normal["atrasado_real"] & df_normal["tem_multa"]].copy()
    com_multa_df["pct_multa"] = (
        com_multa_df["Receita com Multas"] / com_multa_df["Taxa Ordinária"] * 100
    )
    # Ignora divisão por zero (taxa ordinária = 0)
    com_multa_df = com_multa_df[com_multa_df["Taxa Ordinária"] > 0]

    multa_inconsistente = com_multa_df[
        (com_multa_df["pct_multa"] < 1.5) | (com_multa_df["pct_multa"] > 3.0)
    ]

    # Multa em taxa ordinária zero (anomalia)
    multa_em_zero = df_normal[
        (df_normal["Taxa Ordinária"] == 0) & (df_normal["Receita com Multas"] > 0)
    ]

    # Estatísticas de multa
    if len(com_multa_df) > 0:
        multa_media  = round(com_multa_df["pct_multa"].mean(), 2)
        multa_min    = round(com_multa_df["pct_multa"].min(), 2)
        multa_max    = round(com_multa_df["pct_multa"].max(), 2)
    else:
        multa_media = multa_min = multa_max = 0.0

    # ── Outliers (total por registro) ──────────────────────────────────────────
    q1  = df["Total"].quantile(0.25)
    q3  = df["Total"].quantile(0.75)
    iqr = q3 - q1
    outliers_df = df[
        (df["Total"] < q1 - 1.5 * iqr) | (df["Total"] > q3 + 1.5 * iqr)
    ].copy()
    outliers_df = outliers_df[outliers_df["Tipo Cobrança"] == "NORMAL"]

    # Distribuição de atrasos (para tabela de detalhe)
    bins   = [0, 1, 2, 4, 7, 15, 30, 9999]
    labels = ["1 dia", "2 dias", "3-4 dias", "5-7 dias", "8-15 dias", "16-30 dias", ">30 dias"]
    dist_atraso = (
        pd.cut(
            df_normal[df_normal["dias_atraso"] > 0]["dias_atraso"],
            bins=bins, labels=labels, right=True
        )
        .value_counts()
        .sort_index()
        .to_dict()
    )

    # ── Acordos ────────────────────────────────────────────────────────────────
    total_acordos       = len(df_acordo)
    unidades_acordo     = df_acordo["Unidade"].unique().tolist()
    valor_total_acordos = df_acordo["Total"].sum()

    # ── Montagem do resultado ──────────────────────────────────────────────────
    resultado = {
        # Visão geral
        "total_registros":   total_registros,
        "total_unidades":    total_unidades,
        "periodo_inicio":    periodo_inicio.strftime("%d/%m/%Y") if pd.notna(periodo_inicio) else "-",
        "periodo_fim":       periodo_fim.strftime("%d/%m/%Y")    if pd.notna(periodo_fim)    else "-",
        "total_emissao":     f"R$ {total_emissao:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        # Tipos
        "qt_normal":         len(df_normal),
        "qt_extra":          len(df_extra),
        "qt_acordo":         total_acordos,
        # Dados ausentes
        "tem_nulos":         tem_nulos,
        "nulos":             nulos,
        # Consistência taxa
        "taxas_unicas":      [f"R$ {v:,.2f}".replace(",","X").replace(".",",").replace("X",".") for v in taxas_unicas],
        "unidades_taxa_inconsistente": unidades_taxa_inconsistente,
        # Atrasos
        "no_prazo":          no_prazo,
        "compensacao":       compensacao_bancaria,
        "atrasados_reais":   atrasados_reais,
        "total_criticos":    total_criticos,
        "dist_atraso":       {str(k): int(v) for k, v in dist_atraso.items()},
        # Multa
        "total_com_multa":   len(com_multa_df),
        "multa_media":       multa_media,
        "multa_min":         multa_min,
        "multa_max":         multa_max,
        "multa_inconsistente_qt": len(multa_inconsistente),
        "multa_em_zero_qt":  len(multa_em_zero),
        # Outliers
        "total_outliers":    len(outliers_df),
        # Acordos
        "total_acordos":     total_acordos,
        "unidades_acordo":   unidades_acordo,
        "valor_acordos":     f"R$ {valor_total_acordos:,.2f}".replace(",","X").replace(".",",").replace("X","."),
    }

    # ── Gera Excel ─────────────────────────────────────────────────────────────
    output_path = os.path.join(output_dir, f"{session_id}_analise.xlsx")
    _gerar_excel(df, df_normal, com_multa_df, criticos, multa_inconsistente,
                 multa_em_zero, outliers_df, df_acordo, resultado, output_path)

    return resultado


# ── Geração do Excel de resultado ─────────────────────────────────────────────

def _gerar_excel(df, df_normal, com_multa_df, criticos, multa_inconsistente,
                 multa_em_zero, outliers_df, df_acordo, resultado, output_path):
    import openpyxl
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    NAVY  = "1A3C6E"
    BLUE  = "1E88E5"
    CYAN  = "29B6F6"
    WHITE = "FFFFFF"
    LGRAY = "F0F4F8"
    GREEN = "E8F5E9"
    RED   = "FFEBEE"
    YELL  = "FFF9C4"

    def hfill(c): return PatternFill("solid", fgColor=c)
    def hfont(bold=False, color=WHITE, size=10):
        return Font(name="Calibri", bold=bold, color=color, size=size)
    def hborder():
        s = Side(style="thin", color="B0BEC5")
        return Border(left=s, right=s, top=s, bottom=s)
    def center(): return Alignment(horizontal="center", vertical="center")
    def left():   return Alignment(horizontal="left",   vertical="center")

    wb = openpyxl.Workbook()

    # ── Aba 1: Resumo ──────────────────────────────────────────────────────────
    ws = wb.active
    ws.title = "Resumo"

    def titulo(ws, texto, row, ncols=6):
        ws.merge_cells(f"A{row}:{get_column_letter(ncols)}{row}")
        c = ws[f"A{row}"]
        c.value = texto
        c.font  = Font(name="Calibri", bold=True, size=11, color=WHITE)
        c.fill  = hfill(NAVY)
        c.alignment = center()
        ws.row_dimensions[row].height = 22

    def linha_kv(ws, row, label, valor, cor_fundo=LGRAY, cor_valor=NAVY):
        ws.merge_cells(f"A{row}:C{row}")
        cl = ws[f"A{row}"]
        cl.value = label
        cl.font  = Font(name="Calibri", bold=True, size=10, color=NAVY)
        cl.fill  = hfill(cor_fundo)
        cl.alignment = left()
        cl.border = hborder()
        ws.merge_cells(f"D{row}:F{row}")
        cv = ws[f"D{row}"]
        cv.value = valor
        cv.font  = Font(name="Calibri", bold=True, size=10, color=cor_valor)
        cv.fill  = hfill(WHITE)
        cv.alignment = left()
        cv.border = hborder()
        ws.row_dimensions[row].height = 17

    # Cabeçalho
    ws.merge_cells("A1:F1")
    ws["A1"].value = "OGS Serviços — Análise de Consistência de Cobranças"
    ws["A1"].font  = Font(name="Calibri", bold=True, size=14, color=WHITE)
    ws["A1"].fill  = hfill(NAVY)
    ws["A1"].alignment = center()
    ws.row_dimensions[1].height = 30

    r = 3
    titulo(ws, "VISÃO GERAL", r); r+=1
    linha_kv(ws, r, "Condomínio",       df["Condomínio"].dropna().iloc[0] if len(df) else "-"); r+=1
    linha_kv(ws, r, "Total de registros", resultado["total_registros"]); r+=1
    linha_kv(ws, r, "Unidades",          resultado["total_unidades"]); r+=1
    linha_kv(ws, r, "Período",           f"{resultado['periodo_inicio']} → {resultado['periodo_fim']}"); r+=1
    linha_kv(ws, r, "Total emitido",     resultado["total_emissao"]); r+=1
    linha_kv(ws, r, "Cobranças NORMAL",  resultado["qt_normal"]); r+=1
    linha_kv(ws, r, "Cobranças EXTRA",   resultado["qt_extra"]); r+=1
    linha_kv(ws, r, "Acordos",           resultado["qt_acordo"]); r+=1

    r+=1
    titulo(ws, "QUALIDADE DOS DADOS", r); r+=1
    linha_kv(ws, r, "Dados ausentes (campos críticos)",
             "Nenhum" if not resultado["tem_nulos"] else str(resultado["nulos"]),
             cor_valor="2E7D32" if not resultado["tem_nulos"] else "C62828"); r+=1
    linha_kv(ws, r, "Valores únicos de Taxa Ordinária", "  |  ".join(resultado["taxas_unicas"])); r+=1
    linha_kv(ws, r, "Unidades com Taxa Ordinária variável",
             ", ".join(resultado["unidades_taxa_inconsistente"]) or "Nenhuma",
             cor_valor="E65100" if resultado["unidades_taxa_inconsistente"] else "2E7D32"); r+=1

    r+=1
    titulo(ws, "ANÁLISE DE ATRASOS (cobranças NORMAL)", r); r+=1
    linha_kv(ws, r, "No prazo (crédito ≤ vencimento)",      resultado["no_prazo"]); r+=1
    linha_kv(ws, r, "Compensação bancária 1-4 dias (normal)", resultado["compensacao"]); r+=1
    linha_kv(ws, r, "Atrasos reais (≥5 dias)",               resultado["atrasados_reais"],
             cor_valor="E65100" if resultado["atrasados_reais"] else "2E7D32"); r+=1
    linha_kv(ws, r, "Atrasados reais SEM multa (crítico)",   resultado["total_criticos"],
             cor_valor="C62828" if resultado["total_criticos"] else "2E7D32"); r+=1

    r+=1
    titulo(ws, "CONSISTÊNCIA DA MULTA", r); r+=1
    linha_kv(ws, r, "Cobranças com multa aplicada",  resultado["total_com_multa"]); r+=1
    linha_kv(ws, r, "% multa médio",                 f"{resultado['multa_media']:.2f}%"); r+=1
    linha_kv(ws, r, "% multa mín / máx",             f"{resultado['multa_min']:.2f}% / {resultado['multa_max']:.2f}%"); r+=1
    linha_kv(ws, r, "Multa fora da faixa 1,5%–3,0%", resultado["multa_inconsistente_qt"],
             cor_valor="C62828" if resultado["multa_inconsistente_qt"] else "2E7D32"); r+=1
    linha_kv(ws, r, "Multa sobre Taxa Ordinária = R$0", resultado["multa_em_zero_qt"],
             cor_valor="E65100" if resultado["multa_em_zero_qt"] else "2E7D32"); r+=1

    r+=1
    titulo(ws, "VALORES ATÍPICOS E ACORDOS", r); r+=1
    linha_kv(ws, r, "Registros NORMAL com total atípico (IQR)",  resultado["total_outliers"],
             cor_valor="E65100" if resultado["total_outliers"] else "2E7D32"); r+=1
    linha_kv(ws, r, "Acordos registrados",  resultado["total_acordos"]); r+=1
    linha_kv(ws, r, "Unidades com acordo",  ", ".join(resultado["unidades_acordo"]) or "-"); r+=1
    linha_kv(ws, r, "Valor total dos acordos", resultado["valor_acordos"]); r+=1

    for col in ["A","B","C","D","E","F"]:
        ws.column_dimensions[col].width = 18

    # ── Aba 2: Atrasados Reais sem Multa ──────────────────────────────────────
    def aba_dataframe(wb, nome, df_aba, cols_map, fill_header=BLUE):
        ws2 = wb.create_sheet(nome)
        colunas = list(cols_map.keys())
        for ci, col in enumerate(colunas, 1):
            c = ws2.cell(row=1, column=ci)
            c.value = cols_map[col]
            c.font  = Font(name="Calibri", bold=True, size=10, color=WHITE)
            c.fill  = hfill(fill_header)
            c.alignment = center()
            c.border = hborder()
            ws2.column_dimensions[get_column_letter(ci)].width = 18
        ws2.row_dimensions[1].height = 20

        for ri, (_, row) in enumerate(df_aba.iterrows(), 2):
            row_fill = hfill(LGRAY) if ri % 2 == 0 else hfill(WHITE)
            for ci, col in enumerate(colunas, 1):
                c = ws2.cell(row=ri, column=ci)
                val = row.get(col, "")
                if isinstance(val, pd.Timestamp):
                    val = val.strftime("%d/%m/%Y") if pd.notna(val) else ""
                elif isinstance(val, float) and np.isnan(val):
                    val = ""
                c.value = val
                c.fill  = row_fill
                c.border = hborder()
                c.font  = Font(name="Calibri", size=10, color="1A2B3C")
                c.alignment = center()
            ws2.row_dimensions[ri].height = 16
        return ws2

    # Aba: Atrasos reais sem multa
    if len(criticos) > 0:
        aba_dataframe(wb, "Atrasados sem Multa", criticos, {
            "Unidade":            "Unidade",
            "Vencimento_dt":      "Vencimento",
            "Credito_dt":         "Crédito",
            "dias_atraso":        "Dias Atraso",
            "Taxa Ordinária":     "Taxa Ordinária (R$)",
            "Receita com Multas": "Multa Cobrada (R$)",
        }, fill_header="C62828")
    else:
        ws_ok = wb.create_sheet("Atrasados sem Multa")
        ws_ok["A1"].value = "Nenhum pagamento atrasado real sem multa encontrado."
        ws_ok["A1"].font  = Font(name="Calibri", bold=True, color="2E7D32", size=11)

    # Aba: Multa inconsistente
    if len(multa_inconsistente) > 0:
        aba_dataframe(wb, "Multa Inconsistente", multa_inconsistente, {
            "Unidade":            "Unidade",
            "Vencimento_dt":      "Vencimento",
            "Credito_dt":         "Crédito",
            "dias_atraso":        "Dias Atraso",
            "Taxa Ordinária":     "Taxa Ordinária (R$)",
            "Receita com Multas": "Multa (R$)",
            "pct_multa":          "% Multa Calculado",
        }, fill_header="E65100")

    # Aba: Multa sobre taxa zero
    if len(multa_em_zero) > 0:
        aba_dataframe(wb, "Multa sobre Taxa Zero", multa_em_zero, {
            "Unidade":            "Unidade",
            "Vencimento_dt":      "Vencimento",
            "Credito_dt":         "Crédito",
            "dias_atraso":        "Dias Atraso",
            "Taxa Ordinária":     "Taxa Ordinária (R$)",
            "Receita com Multas": "Multa (R$)",
        }, fill_header="E65100")

    # Aba: Acordos
    if len(df_acordo) > 0:
        aba_dataframe(wb, "Acordos", df_acordo, {
            "Unidade":            "Unidade",
            "Vencimento_dt":      "Vencimento",
            "Credito_dt":         "Crédito",
            "dias_atraso":        "Dias Atraso",
            "Receita com Multas": "Multa (R$)",
            "Total":              "Total (R$)",
        }, fill_header=NAVY)

    # Aba: Dados completos
    aba_dataframe(wb, "Dados Completos", df, {
        "Unidade":            "Unidade",
        "Vencimento_dt":      "Vencimento",
        "Credito_dt":         "Crédito",
        "dias_atraso":        "Dias Atraso",
        "Tipo Cobrança":      "Tipo",
        "Taxa Ordinária":     "Taxa Ordinária (R$)",
        "Taxa de Água":       "Água (R$)",
        "Receita com Multas": "Multa (R$)",
        "Total":              "Total (R$)",
    }, fill_header=NAVY)

    wb.save(output_path)
