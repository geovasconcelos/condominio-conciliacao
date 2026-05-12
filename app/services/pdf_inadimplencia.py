"""
Parser para relatório de inadimplência gerado pelo Controlar.
Formato esperado: "Inadimplência com composição (detalhado)"
"""
import re
import pdfplumber

_RE_UNIT     = re.compile(r'^(\d{3,4})\s*-\s*\S')
_RE_COMP     = re.compile(r'^\d{2}/\d{4}$')
_RE_DATE_ROW = re.compile(r'^\d{2}/\d{2}/\d{2,4}\s')
_RE_TOTAL    = re.compile(r'^Total\s+[\d\.,]+')
_SKIP_LINES  = {"Venc.", "CONTROLAR", "Avenida", "atendimento@", "Emitido em",
                "Resumo por categoria", "Descrição"}


def _norm_unit(s):
    try:
        return str(int(s.strip()))
    except (ValueError, TypeError):
        return str(s).strip()


def parsear_pdf_inadimplencia(path: str) -> tuple[list, list]:
    """
    Parseia PDF de inadimplência do Controlar.

    Retorna (registros, erros) onde:
      registros : list[{unidade, competencia}] — pares únicos unit+mês
      erros     : list[{pagina, linha, mensagem}]
    """
    registros = []
    erros = []
    unidade_atual = None
    encontrou_header = False
    em_resumo = False

    try:
        with pdfplumber.open(path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                texto = page.extract_text()
                if not texto:
                    continue

                for linha_num, linha in enumerate(texto.split("\n"), 1):
                    linha = linha.strip()
                    if not linha:
                        continue

                    # Detecta cabeçalho do relatório
                    if "Inadimplência com composição" in linha:
                        encontrou_header = True
                        continue

                    # Seção de resumo final — para de processar unidades
                    if "Resumo por categoria" in linha:
                        em_resumo = True
                        continue
                    if em_resumo:
                        continue

                    # Linhas de rodapé e cabeçalho de página
                    if any(linha.startswith(s) for s in _SKIP_LINES):
                        continue
                    if re.match(r"^\d+ de \d+$", linha):
                        continue

                    # Linha de total de unidade
                    if _RE_TOTAL.match(linha):
                        continue

                    # Cabeçalho de unidade: "0201 - NOME..." — não começa com data
                    m = _RE_UNIT.match(linha)
                    if m and not _RE_DATE_ROW.match(linha):
                        unidade_atual = _norm_unit(m.group(1))
                        continue

                    # Linha de dado: começa com vencimento DD/MM/AA
                    if unidade_atual and _RE_DATE_ROW.match(linha):
                        partes = linha.split()
                        if len(partes) >= 2 and _RE_COMP.match(partes[1]):
                            registros.append({
                                "unidade":    unidade_atual,
                                "competencia": partes[1],
                            })
                        else:
                            erros.append({
                                "pagina":   page_num,
                                "linha":    linha_num,
                                "mensagem": f'Linha com data mas competência inválida: "{linha[:70]}"',
                            })

    except Exception as exc:
        erros.append({
            "pagina":   0,
            "linha":    0,
            "mensagem": f"Erro ao abrir o PDF: {type(exc).__name__}: {exc}",
        })
        return [], erros

    # Validação de formato
    if not encontrou_header:
        erros.insert(0, {
            "pagina":   1,
            "linha":    0,
            "mensagem": (
                'Formato não reconhecido: o PDF não parece ser um relatório '
                '"Inadimplência com composição (detalhado)" do Controlar. '
                'Verifique se o arquivo correto foi enviado.'
            ),
        })

    if not registros and not erros:
        erros.append({
            "pagina":   0,
            "linha":    0,
            "mensagem": "Nenhuma unidade ou linha de dados encontrada no PDF.",
        })

    # Remove pares duplicados (mesmo boleto tem múltiplas linhas de cobrança)
    seen: set = set()
    unicos = []
    for r in registros:
        key = (r["unidade"], r["competencia"])
        if key not in seen:
            seen.add(key)
            unicos.append(r)

    return unicos, erros


def comparar_com_sistema(pdf_registros: list, boletos_ausentes: list) -> list:
    """
    Compara dados do PDF com boletos_ausentes do sistema.

    Retorna lista de divergências com campos:
      Tipo, Unidade, Competência, Detalhe
    """
    # Normaliza sistema: {(unidade, comp): tipo_inadin}
    sistema: dict = {}
    for b in boletos_ausentes:
        key = (str(b["Unidade"]), str(b["Competência"]))
        sistema[key] = b.get("tipo_inadin", "NORMAL")

    sistema_units  = set(u for u, _ in sistema)
    sistema_meses  = set(sistema.keys())

    # PDF: set de pares e set de unidades
    pdf_set   = {(r["unidade"], r["competencia"]) for r in pdf_registros}
    pdf_units = {r["unidade"] for r in pdf_registros}

    divergencias = []

    # Unidades no PDF não detectadas pelo sistema
    for u in sorted(pdf_units - sistema_units, key=lambda x: x.zfill(10)):
        divergencias.append({
            "Tipo":        "No PDF, não no sistema",
            "Unidade":     u,
            "Competência": "—",
            "Detalhe":     "Unidade consta como inadimplente no relatório do cliente mas o sistema não detectou inadimplência.",
        })

    # Unidades no sistema não presentes no PDF
    for u in sorted(sistema_units - pdf_units, key=lambda x: x.zfill(10)):
        divergencias.append({
            "Tipo":        "No sistema, não no PDF",
            "Unidade":     u,
            "Competência": "—",
            "Detalhe":     "Sistema detectou inadimplência mas a unidade não aparece no relatório do cliente.",
        })

    # Meses divergentes para unidades em comum
    for u in sorted(pdf_units & sistema_units, key=lambda x: x.zfill(10)):
        meses_pdf = {comp for (un, comp) in pdf_set if un == u}
        meses_sis = {comp for (un, comp) in sistema_meses if un == u}

        for mes in sorted(meses_pdf - meses_sis):
            divergencias.append({
                "Tipo":        "Mês no PDF, não no sistema",
                "Unidade":     u,
                "Competência": mes,
                "Detalhe":     "Mês consta no relatório do cliente mas o sistema não detectou inadimplência neste mês.",
            })

        for mes in sorted(meses_sis - meses_pdf):
            tipo = sistema.get((u, mes), "")
            divergencias.append({
                "Tipo":        "Mês no sistema, não no PDF",
                "Unidade":     u,
                "Competência": mes,
                "Detalhe":     f"Sistema detectou inadimplência ({tipo}) mas o mês não aparece no relatório do cliente.",
            })

    return divergencias
