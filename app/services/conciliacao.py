"""
Serviço de análise e conciliação de cobranças condominiais.
Compara a planilha de dados (004A) com a planilha de parâmetros.
"""
import os
import numpy as np
import pandas as pd

from app.services.parametros import ler_parametros

# ── Helpers ────────────────────────────────────────────────────────────────────

def _br_to_float(s):
    if pd.isna(s):
        return np.nan
    s = str(s).strip()
    neg = s.startswith("(") and s.endswith(")")
    s = s.replace("(","").replace(")","").replace(".","").replace(",",".")
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


def _fmt_brl(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "-"
    return f"R$ {v:,.2f}".replace(",","X").replace(".",",").replace("X",".")


NUMERIC_COLS = [
    "Tarifa Liquidação Boleto","Taxa de Água","Medição e Leitura de Água",
    "Taxa Ordinária","Taxa Extra -  Modernização Elevadores",
    "Taxa Extra Manut. Piscina","Taxa Extra - Melhorias no Condomínio",
    "Taxa Extra - Aquisição de Gerador",
    "Taxa Extra  - Reforma - Aquisição - Equipamentos Academia",
    "Receita com Multas","Outros","Total",
]

# Mapeamento nome do parâmetro → coluna no 004A
EXTRA_COL_MAP = {
    "Taxa Extra -  Modernização Elevadores":              "Taxa Extra -  Modernização Elevadores",
    "Taxa Extra  - Reforma - Aquisição - Equipamentos Academia": "Taxa Extra  - Reforma - Aquisição - Equipamentos Academia",
    "Taxa Extra - Aquisição de Gerador":                  "Taxa Extra - Aquisição de Gerador",
    "Taxa Extra - Melhorias no Condomínio":               "Taxa Extra - Melhorias no Condomínio",
    "Taxa Extra Manut. Piscina":                          "Taxa Extra Manut. Piscina",
}


# ── Carregamento dos dados ──────────────────────────────────────────────────────

def _carregar_dados(path: str) -> pd.DataFrame:
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


# ── Análise principal ───────────────────────────────────────────────────────────

def processar_conciliacao(path_params: str, path_dados: str,
                          session_id: str, output_dir: str) -> dict:
    params = ler_parametros(path_params)
    df     = _carregar_dados(path_dados)

    df_normal = df[df["Tipo Cobrança"] == "NORMAL"].copy()
    df_extra  = df[df["Tipo Cobrança"] == "EXTRA"].copy()
    df_acordo = df[df["Tipo Cobrança"] == "ACORDO"].copy()

    unidades_dados   = set(df["Unidade"].unique()) - {""}
    unidades_params  = set(params["unidades"].keys())
    sindicos         = set(params["sindicos"])

    # ── 1. Campos faltantes no parâmetro ──────────────────────
    campos_faltantes = params["campos_faltantes"]

    # ── 2. Unidades sem parâmetro / parâmetro sem dados ───────
    sem_parametro = sorted(unidades_dados - unidades_params)
    sem_dados     = sorted(unidades_params - unidades_dados)

    # ── 3. Taxa Ordinária ──────────────────────────────────────
    inconsistencias_taxa = []
    taxa_padrao = params["taxa_ord_padrao"] or 0.0

    for unidade, grp in df_normal.groupby("Unidade"):
        p = params["unidades"].get(unidade)
        if not p:
            continue

        isento = unidade in sindicos and params["isencao_sindico"]
        taxa_esperada = 0.0 if isento else (p["taxa_ordinaria"] or taxa_padrao)

        # Agrupa por mês e verifica apenas o maior valor do mês.
        # Cada mês pode ter múltiplas linhas NORMAL (taxa ordinária + outros encargos);
        # a linha de outros encargos tem taxa=0 e não deve ser verificada individualmente.
        grp = grp.copy()
        grp["_mes"] = grp["Vencimento_dt"].dt.to_period("M")
        for mes, grp_mes in grp.groupby("_mes"):
            taxa_real = grp_mes["Taxa Ordinária"].max()
            if pd.isna(taxa_real):
                taxa_real = 0.0
            diff = abs(taxa_real - taxa_esperada)
            if diff > 0.05:
                row = grp_mes.iloc[0]
                inconsistencias_taxa.append({
                    "Unidade":         unidade,
                    "Vencimento":      row["Vencimento_dt"].strftime("%m/%Y") if pd.notna(row["Vencimento_dt"]) else "-",
                    "Esperado (R$)":   taxa_esperada,
                    "Encontrado (R$)": taxa_real,
                    "Diferença (R$)":  taxa_real - taxa_esperada,
                    "Motivo":          "Síndico isento" if isento else "Divergência de taxa",
                })

    # ── 3b. Inadimplência — boletos ausentes na 004A ─────────────
    # A 004A só contém registros PAGOS. Meses sem registro = inadimplência.
    boletos_ausentes = []
    if taxa_padrao > 0:
        all_months = pd.period_range(
            df["Vencimento_dt"].min().to_period("M"),
            df["Vencimento_dt"].max().to_period("M"),
            freq="M"
        )
        df_n_mes = df_normal.copy()
        df_n_mes["_mes"] = df_n_mes["Vencimento_dt"].dt.to_period("M")

        for unidade, p in params["unidades"].items():
            isento = unidade in sindicos and params["isencao_sindico"]
            if isento:
                continue
            taxa_esp = p["taxa_ordinaria"] or taxa_padrao
            if not taxa_esp or taxa_esp < 0.05:
                continue
            regs_u = df_n_mes[df_n_mes["Unidade"] == unidade]
            for mes in all_months:
                regs_mes = regs_u[regs_u["_mes"] == mes]
                max_taxa = float(regs_mes["Taxa Ordinária"].max()) if len(regs_mes) > 0 else 0.0
                if pd.isna(max_taxa):
                    max_taxa = 0.0
                if max_taxa < 0.05:
                    boletos_ausentes.append({
                        "Unidade":            unidade,
                        "Competência":        f"{mes.month:02d}/{mes.year}",
                        "Taxa Esperada (R$)": taxa_esp,
                        "Motivo":             "Boleto não localizado" if len(regs_mes) == 0 else "Taxa ordinária zero no boleto",
                    })

    # ── 4. Taxa de Água ────────────────────────────────────────
    problemas_agua = []
    if "Taxa de Água" in df.columns:
        for unidade, grp in df_normal.groupby("Unidade"):
            meses_sem_agua = grp[
                grp["Taxa de Água"].isna() | (grp["Taxa de Água"] == 0)
            ]
            for _, row in meses_sem_agua.iterrows():
                problemas_agua.append({
                    "Unidade":    unidade,
                    "Vencimento": row["Vencimento_dt"].strftime("%d/%m/%Y") if pd.notna(row["Vencimento_dt"]) else "-",
                    "Problema":   "Taxa de Água ausente ou zero",
                    "Valor":      row["Taxa de Água"],
                })

    # ── 5. Medição e Leitura de Água ───────────────────────────
    problemas_medicao = []
    ref_medicao = params["taxa_medicao"] or 0.0
    if "Medição e Leitura de Água" in df.columns and ref_medicao > 0:
        for unidade, grp in df_normal.groupby("Unidade"):
            for _, row in grp.iterrows():
                med = row["Medição e Leitura de Água"]
                if pd.isna(med) or med == 0:
                    problemas_medicao.append({
                        "Unidade":    unidade,
                        "Vencimento": row["Vencimento_dt"].strftime("%d/%m/%Y") if pd.notna(row["Vencimento_dt"]) else "-",
                        "Problema":   "Medição ausente ou zero",
                        "Esperado":   ref_medicao,
                        "Encontrado": med,
                    })
                elif abs(med - ref_medicao) > 0.05:
                    problemas_medicao.append({
                        "Unidade":    unidade,
                        "Vencimento": row["Vencimento_dt"].strftime("%d/%m/%Y") if pd.notna(row["Vencimento_dt"]) else "-",
                        "Problema":   "Valor divergente",
                        "Esperado":   ref_medicao,
                        "Encontrado": med,
                    })

    # ── 6. Taxas Extras ────────────────────────────────────────
    inconsistencias_extra = []
    for taxa_param in params["taxas_extras"]:
        nome    = taxa_param["nome"]
        col     = EXTRA_COL_MAP.get(nome)
        valor_p = taxa_param["valor"]
        inicio  = taxa_param["inicio"]
        fim     = taxa_param["fim"]

        if not col or col not in df.columns:
            continue

        df_e = df_extra[df_extra["Unidade"].isin(
            [u for u, p in params["unidades"].items() if p["tem_taxa_extra"]]
        )].copy()

        for unidade, grp in df_e.groupby("Unidade"):
            for _, row in grp.iterrows():
                venc = row["Vencimento_dt"]
                if pd.isna(venc):
                    continue

                # Verifica se está no período esperado (comparação por data, não tupla)
                no_periodo = True
                if inicio and fim:
                    inicio_date = pd.Timestamp(year=inicio[1], month=inicio[0], day=1)
                    fim_date    = pd.Timestamp(year=fim[1],    month=fim[0],    day=28)
                    no_periodo  = inicio_date <= venc <= fim_date

                valor_real = row[col]
                if pd.isna(valor_real):
                    valor_real = 0.0

                if no_periodo and abs(valor_real - valor_p) > 0.05:
                    inconsistencias_extra.append({
                        "Taxa":          nome,
                        "Unidade":       unidade,
                        "Vencimento":    venc.strftime("%d/%m/%Y"),
                        "Esperado (R$)": valor_p,
                        "Encontrado (R$)": valor_real,
                        "Diferença (R$)": valor_real - valor_p,
                    })
                elif not no_periodo and valor_real > 0:
                    inconsistencias_extra.append({
                        "Taxa":          nome,
                        "Unidade":       unidade,
                        "Vencimento":    venc.strftime("%d/%m/%Y"),
                        "Esperado (R$)": 0.0,
                        "Encontrado (R$)": valor_real,
                        "Diferença (R$)": valor_real,
                        "Motivo":        "Cobrança fora do período vigente",
                    })

    # ── 7. Multa ────────────────────────────────────────────────
    carencia    = params["carencia_dias"]
    pct_multa_p = params["pct_multa"]

    # Síndico isento não entra nas verificações de atraso e multa
    if params["isencao_sindico"] and sindicos:
        df_multa = df_normal[~df_normal["Unidade"].isin(sindicos)].copy()
    else:
        df_multa = df_normal.copy()

    # Inadimplente = pago com atraso OU boleto sem crédito com vencimento já passado
    hoje = pd.Timestamp.today().normalize()
    sem_credito = (
        df_multa["Credito_dt"].isna() &
        df_multa["Vencimento_dt"].notna() &
        (df_multa["Vencimento_dt"] < hoje)
    )
    df_multa["atrasado_real"] = (df_multa["dias_atraso"] > carencia) | sem_credito
    df_multa["tem_multa"]     = df_multa["Receita com Multas"].fillna(0) > 0

    atrasados_reais     = int(df_multa["atrasado_real"].sum())
    atrasados_sem_multa = df_multa[df_multa["atrasado_real"] & ~df_multa["tem_multa"]]
    total_criticos      = len(atrasados_sem_multa)

    com_multa_df = df_multa[df_multa["atrasado_real"] & df_multa["tem_multa"]].copy()
    com_multa_df = com_multa_df[com_multa_df["Taxa Ordinária"].fillna(0) > 0].copy()
    com_multa_df["pct_multa_calc"] = (
        com_multa_df["Receita com Multas"] / com_multa_df["Taxa Ordinária"] * 100
    )
    multa_inconsistente = com_multa_df[
        (com_multa_df["pct_multa_calc"] < pct_multa_p * 0.75) |
        (com_multa_df["pct_multa_calc"] > pct_multa_p * 1.5)
    ]
    multa_em_zero = df_multa[
        (df_multa["Taxa Ordinária"].fillna(0) == 0) & (df_multa["Receita com Multas"].fillna(0) > 0)
    ]

    # ── 8. Estatísticas gerais ────────────────────────────────
    total_registros = len(df)
    total_unidades  = df["Unidade"].nunique()
    periodo_inicio  = df["Vencimento_dt"].min()
    periodo_fim     = df["Vencimento_dt"].max()
    total_emissao   = df["Total"].sum()

    no_prazo            = int((df_normal["dias_atraso"] <= 0).sum())
    compensacao_banc    = int(((df_normal["dias_atraso"] > 0) & (df_normal["dias_atraso"] <= carencia)).sum())

    bins   = [0, 1, 2, 4, 7, 15, 30, 9999]
    labels = ["1 dia","2 dias","3-4 dias","5-7 dias","8-15 dias","16-30 dias",">30 dias"]
    dist_atraso = (
        pd.cut(df_normal[df_normal["dias_atraso"] > 0]["dias_atraso"],
               bins=bins, labels=labels, right=True)
        .value_counts().sort_index().to_dict()
    )

    multa_media = round(com_multa_df["pct_multa_calc"].mean(), 2) if len(com_multa_df) else 0.0
    multa_min   = round(com_multa_df["pct_multa_calc"].min(), 2)  if len(com_multa_df) else 0.0
    multa_max   = round(com_multa_df["pct_multa_calc"].max(), 2)  if len(com_multa_df) else 0.0

    # Outliers
    q1  = df["Total"].quantile(0.25)
    q3  = df["Total"].quantile(0.75)
    iqr = q3 - q1
    outliers_n = len(df[
        ((df["Total"] < q1 - 1.5 * iqr) | (df["Total"] > q3 + 1.5 * iqr)) &
        (df["Tipo Cobrança"] == "NORMAL")
    ])

    resultado = {
        # Geral
        "total_registros":    total_registros,
        "total_unidades":     total_unidades,
        "periodo_inicio":     periodo_inicio.strftime("%d/%m/%Y") if pd.notna(periodo_inicio) else "-",
        "periodo_fim":        periodo_fim.strftime("%d/%m/%Y")    if pd.notna(periodo_fim)    else "-",
        "total_emissao":      _fmt_brl(total_emissao),
        "qt_normal":          len(df_normal),
        "qt_extra":           len(df_extra),
        "qt_acordo":          len(df_acordo),
        # Parâmetros
        "taxa_padrao_param":  _fmt_brl(params["taxa_ord_padrao"]),
        "carencia_param":     carencia,
        "pct_multa_param":    pct_multa_p,
        "sindicos":           params["sindicos"],
        "isencao_sindico":    params["isencao_sindico"],
        "ref_medicao":        _fmt_brl(params["taxa_medicao"]),
        "qt_taxas_extras_param": len(params["taxas_extras"]),
        # Validação de parâmetros
        "campos_faltantes":   campos_faltantes,
        "sem_parametro":      sem_parametro,
        "sem_dados":          sem_dados,
        # Taxa ordinária
        "qt_inconsist_taxa":  len(inconsistencias_taxa),
        # Inadimplência (boletos ausentes)
        "qt_boletos_ausentes": len(boletos_ausentes),
        "unidades_inadimplentes": sorted(set(b["Unidade"] for b in boletos_ausentes)),
        # Água
        "qt_prob_agua":       len(problemas_agua),
        "qt_prob_medicao":    len(problemas_medicao),
        # Taxa extra
        "qt_inconsist_extra": len(inconsistencias_extra),
        # Atrasos
        "no_prazo":           no_prazo,
        "compensacao":        compensacao_banc,
        "atrasados_reais":    atrasados_reais,
        "total_criticos":     total_criticos,
        "dist_atraso":        {str(k): int(v) for k, v in dist_atraso.items()},
        # Multa
        "total_com_multa":    len(com_multa_df),
        "multa_media":        multa_media,
        "multa_min":          multa_min,
        "multa_max":          multa_max,
        "multa_inconsistente_qt": len(multa_inconsistente),
        "multa_em_zero_qt":   len(multa_em_zero),
        # Atípicos e acordos
        "total_outliers":     outliers_n,
        "total_acordos":      len(df_acordo),
        "unidades_acordo":    df_acordo["Unidade"].unique().tolist(),
        "valor_acordos":      _fmt_brl(df_acordo["Total"].sum()),
    }

    # Gera Excel
    _gerar_excel(df, atrasados_sem_multa,
                 multa_inconsistente, multa_em_zero,
                 pd.DataFrame(inconsistencias_taxa),
                 pd.DataFrame(problemas_agua),
                 pd.DataFrame(problemas_medicao),
                 pd.DataFrame(inconsistencias_extra),
                 pd.DataFrame(boletos_ausentes),
                 sem_parametro, sem_dados, campos_faltantes,
                 resultado, output_dir, session_id)

    return resultado


# ── Geração do Excel ────────────────────────────────────────────────────────────

def _gerar_excel(df, atrasados_sem_multa,
                 multa_inconsistente, multa_em_zero,
                 df_taxa, df_agua, df_medicao, df_extra, df_inadimplentes,
                 sem_param, sem_dados, campos_faltantes,
                 resultado, output_dir, session_id):
    import openpyxl
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    NAVY="1A3C6E"; BLUE="1E88E5"; WHITE="FFFFFF"; LGRAY="F0F4F8"

    def hfill(c): return PatternFill("solid", fgColor=c)
    def hfont(bold=False, color=WHITE, size=10):
        return Font(name="Calibri", bold=bold, color=color, size=size)
    def hbdr():
        s=Side(style="thin",color="B0BEC5"); return Border(left=s,right=s,top=s,bottom=s)
    def center(): return Alignment(horizontal="center",vertical="center")
    def left():   return Alignment(horizontal="left",  vertical="center")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Resumo"

    def titulo(ws, texto, row, ncols=6):
        lc = get_column_letter(ncols)
        ws.merge_cells(f"A{row}:{lc}{row}")
        c=ws["A"+str(row)]; c.value=texto
        c.font=hfont(bold=True,size=11); c.fill=hfill(NAVY); c.alignment=center()
        ws.row_dimensions[row].height=22

    def kv(ws, row, label, valor, cor_val=NAVY):
        ws.merge_cells(f"A{row}:C{row}")
        cl=ws["A"+str(row)]; cl.value=label
        cl.font=hfont(bold=True,color=NAVY,size=10); cl.fill=hfill(LGRAY)
        cl.alignment=left(); cl.border=hbdr()
        ws.merge_cells(f"D{row}:F{row}")
        cv=ws["D"+str(row)]; cv.value=valor
        cv.font=hfont(bold=True,color=cor_val,size=10)
        cv.fill=hfill(WHITE); cv.alignment=left(); cv.border=hbdr()
        ws.row_dimensions[row].height=17

    def aba_df(wb, nome, df_aba, col_map, fill_hdr=BLUE):
        if df_aba is None or len(df_aba) == 0:
            ws2=wb.create_sheet(nome)
            ws2["A1"].value="Nenhuma ocorrência encontrada."
            ws2["A1"].font=hfont(bold=True,color="2E7D32",size=11)
            return
        ws2=wb.create_sheet(nome)
        colunas=list(col_map.keys())
        for ci,col in enumerate(colunas,1):
            c=ws2.cell(row=1,column=ci)
            c.value=col_map[col]; c.font=hfont(bold=True,size=10)
            c.fill=hfill(fill_hdr); c.alignment=center(); c.border=hbdr()
            ws2.column_dimensions[get_column_letter(ci)].width=20
        ws2.row_dimensions[1].height=20
        for ri,(_, row) in enumerate(df_aba.iterrows(),2):
            rf=hfill(LGRAY) if ri%2==0 else hfill(WHITE)
            for ci,col in enumerate(colunas,1):
                c=ws2.cell(row=ri,column=ci)
                val=row.get(col,"") if col in row.index else ""
                if isinstance(val,float) and np.isnan(val): val=""
                c.value=val; c.fill=rf; c.border=hbdr()
                c.font=hfont(color="1A2B3C",size=10); c.alignment=center()
            ws2.row_dimensions[ri].height=16

    # Resumo
    ws["A1"].value="OGS Serviços — Análise e Conciliação de Cobranças"
    ws["A1"].font=hfont(bold=True,size=14); ws["A1"].fill=hfill(NAVY)
    ws.merge_cells("A1:F1"); ws["A1"].alignment=center(); ws.row_dimensions[1].height=30
    for col in ["A","B","C","D","E","F"]: ws.column_dimensions[col].width=18

    r=3
    titulo(ws,"VISÃO GERAL",r); r+=1
    kv(ws,r,"Período",f"{resultado['periodo_inicio']} → {resultado['periodo_fim']}"); r+=1
    kv(ws,r,"Total emitido",resultado["total_emissao"]); r+=1
    kv(ws,r,"Registros / Unidades",f"{resultado['total_registros']} / {resultado['total_unidades']}"); r+=1

    r+=1; titulo(ws,"VALIDAÇÃO DOS PARÂMETROS",r); r+=1
    kv(ws,r,"Campos obrigatórios faltantes",
       ", ".join(campos_faltantes) if campos_faltantes else "Nenhum",
       "C62828" if campos_faltantes else "2E7D32"); r+=1
    kv(ws,r,"Unidades nos dados sem parâmetro",
       ", ".join(sem_param) if sem_param else "Nenhuma",
       "E65100" if sem_param else "2E7D32"); r+=1
    kv(ws,r,"Unidades no parâmetro sem dados",
       ", ".join(sem_dados) if sem_dados else "Nenhuma",
       "E65100" if sem_dados else "2E7D32"); r+=1

    r+=1; titulo(ws,"INADIMPLÊNCIA",r); r+=1
    kv(ws,r,"Boletos ausentes na 004A",resultado["qt_boletos_ausentes"],
       "C62828" if resultado["qt_boletos_ausentes"] else "2E7D32"); r+=1
    kv(ws,r,"Unidades inadimplentes",
       ", ".join(resultado["unidades_inadimplentes"]) if resultado["unidades_inadimplentes"] else "Nenhuma",
       "C62828" if resultado["unidades_inadimplentes"] else "2E7D32"); r+=1

    r+=1; titulo(ws,"INCONSISTÊNCIAS DE VALORES",r); r+=1
    kv(ws,r,"Taxa Ordinária divergente",resultado["qt_inconsist_taxa"],
       "C62828" if resultado["qt_inconsist_taxa"] else "2E7D32"); r+=1
    kv(ws,r,"Taxa de Água ausente/zero",resultado["qt_prob_agua"],
       "E65100" if resultado["qt_prob_agua"] else "2E7D32"); r+=1
    kv(ws,r,"Medição e Leitura divergente",resultado["qt_prob_medicao"],
       "E65100" if resultado["qt_prob_medicao"] else "2E7D32"); r+=1
    kv(ws,r,"Taxas Extras divergentes",resultado["qt_inconsist_extra"],
       "E65100" if resultado["qt_inconsist_extra"] else "2E7D32"); r+=1

    r+=1; titulo(ws,"MULTA E ATRASOS",r); r+=1
    kv(ws,r,"Atrasos reais (> carência)",resultado["atrasados_reais"]); r+=1
    kv(ws,r,"Atrasados reais sem multa",resultado["total_criticos"],
       "C62828" if resultado["total_criticos"] else "2E7D32"); r+=1
    kv(ws,r,"% multa médio / esperado",
       f"{resultado['multa_media']:.2f}% / {resultado['pct_multa_param']:.2f}%"); r+=1
    kv(ws,r,"Multa fora da faixa esperada",resultado["multa_inconsistente_qt"],
       "E65100" if resultado["multa_inconsistente_qt"] else "2E7D32"); r+=1
    kv(ws,r,"Multa sobre taxa ordinária zero",resultado["multa_em_zero_qt"],
       "E65100" if resultado["multa_em_zero_qt"] else "2E7D32"); r+=1

    # Abas de detalhe
    aba_df(wb,"Taxa Ordinária — Divergências", df_taxa,
           {"Unidade":"Unidade","Vencimento":"Vencimento",
            "Esperado (R$)":"Esperado (R$)","Encontrado (R$)":"Encontrado (R$)",
            "Diferença (R$)":"Diferença (R$)","Motivo":"Motivo"}, "C62828")

    aba_df(wb,"Taxa de Água — Problemas", df_agua,
           {"Unidade":"Unidade","Vencimento":"Vencimento","Problema":"Problema","Valor":"Valor"}, "E65100")

    aba_df(wb,"Medição — Divergências", df_medicao,
           {"Unidade":"Unidade","Vencimento":"Vencimento",
            "Problema":"Problema","Esperado":"Esperado (R$)","Encontrado":"Encontrado (R$)"}, "E65100")

    aba_df(wb,"Taxas Extras — Divergências", df_extra,
           {"Taxa":"Taxa","Unidade":"Unidade","Vencimento":"Vencimento",
            "Esperado (R$)":"Esperado (R$)","Encontrado (R$)":"Encontrado (R$)",
            "Diferença (R$)":"Diferença (R$)"}, "E65100")

    aba_df(wb,"Inadimplência — Boletos Ausentes", df_inadimplentes,
           {"Unidade":"Unidade","Competência":"Competência",
            "Taxa Esperada (R$)":"Taxa Esperada (R$)","Motivo":"Motivo"}, "C62828")

    aba_df(wb,"Atrasados sem Multa", atrasados_sem_multa,
           {"Unidade":"Unidade","Vencimento_dt":"Vencimento","Credito_dt":"Crédito",
            "dias_atraso":"Dias Atraso","Taxa Ordinária":"Taxa Ordinária (R$)",
            "Receita com Multas":"Multa (R$)"}, "C62828")

    aba_df(wb,"Multa Inconsistente", multa_inconsistente,
           {"Unidade":"Unidade","Vencimento_dt":"Vencimento","dias_atraso":"Dias Atraso",
            "Taxa Ordinária":"Taxa Ord. (R$)","Receita com Multas":"Multa (R$)",
            "pct_multa_calc":"% Multa Calculado"}, "E65100")

    aba_df(wb,"Multa sobre Taxa Zero", multa_em_zero,
           {"Unidade":"Unidade","Vencimento_dt":"Vencimento",
            "Taxa Ordinária":"Taxa Ord. (R$)","Receita com Multas":"Multa (R$)"}, "E65100")

    # Dados completos
    aba_df(wb,"Dados Completos", df,
           {"Unidade":"Unidade","Vencimento_dt":"Vencimento","Credito_dt":"Crédito",
            "dias_atraso":"Dias Atraso","Tipo Cobrança":"Tipo",
            "Taxa Ordinária":"Taxa Ord. (R$)","Taxa de Água":"Água (R$)",
            "Medição e Leitura de Água":"Medição (R$)",
            "Receita com Multas":"Multa (R$)","Total":"Total (R$)"}, NAVY)

    out = os.path.join(output_dir, f"{session_id}_analise.xlsx")
    wb.save(out)
