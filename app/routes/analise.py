import os
import uuid
from flask import Blueprint, request, render_template, send_from_directory, current_app, flash, redirect, url_for

from app.services.conciliacao import processar_conciliacao

analise_bp = Blueprint("analise", __name__)

EXTENSOES_PERMITIDAS = {"xlsx", "xls"}


def _extensao_valida(nome):
    return "." in nome and nome.rsplit(".", 1)[1].lower() in EXTENSOES_PERMITIDAS


@analise_bp.route("/analisar", methods=["POST"])
def analisar():
    arquivo_parametros = request.files.get("parametros")
    arquivo_dados = request.files.get("dados")

    if not arquivo_parametros or not arquivo_dados:
        flash("Envie os dois arquivos Excel.")
        return redirect(url_for("main.index"))

    if not (_extensao_valida(arquivo_parametros.filename) and _extensao_valida(arquivo_dados.filename)):
        flash("Apenas arquivos .xlsx ou .xls são aceitos.")
        return redirect(url_for("main.index"))

    session_id = str(uuid.uuid4())
    upload_dir = current_app.config["UPLOAD_FOLDER"]

    path_parametros = os.path.join(upload_dir, f"{session_id}_parametros.xlsx")
    path_dados = os.path.join(upload_dir, f"{session_id}_dados.xlsx")
    arquivo_parametros.save(path_parametros)
    arquivo_dados.save(path_dados)

    resultado = processar_conciliacao(path_parametros, path_dados, session_id, current_app.config["OUTPUT_FOLDER"])

    return render_template("resultado.html", resultado=resultado, session_id=session_id)


@analise_bp.route("/download/<session_id>")
def download(session_id):
    output_dir = current_app.config["OUTPUT_FOLDER"]
    nome_arquivo = f"{session_id}_conciliacao.xlsx"
    return send_from_directory(output_dir, nome_arquivo, as_attachment=True, download_name="conciliacao.xlsx")
