import os
import uuid
from flask import (Blueprint, request, render_template, send_from_directory,
                   current_app, flash, redirect, url_for)

from app.services.conciliacao import processar_conciliacao

analise_bp = Blueprint("analise", __name__)

EXTENSOES = {"xlsx", "xls"}


def _valida(nome):
    return "." in nome and nome.rsplit(".", 1)[1].lower() in EXTENSOES


@analise_bp.route("/analisar", methods=["POST"])
def analisar():
    arquivo_dados = request.files.get("dados")

    if not arquivo_dados or not arquivo_dados.filename:
        flash("Envie o arquivo de base de dados.")
        return redirect(url_for("main.index"))

    if not _valida(arquivo_dados.filename):
        flash("Apenas arquivos .xlsx ou .xls são aceitos.")
        return redirect(url_for("main.index"))

    session_id = str(uuid.uuid4())
    upload_dir = current_app.config["UPLOAD_FOLDER"]
    output_dir = current_app.config["OUTPUT_FOLDER"]

    path_dados = os.path.join(upload_dir, f"{session_id}_dados.xlsx")
    arquivo_dados.save(path_dados)

    resultado = processar_conciliacao(path_dados, session_id, output_dir)

    return render_template("index.html", resultado=resultado, session_id=session_id)


@analise_bp.route("/download/<session_id>")
def download(session_id):
    output_dir = current_app.config["OUTPUT_FOLDER"]
    nome = f"{session_id}_analise.xlsx"
    return send_from_directory(output_dir, nome, as_attachment=True,
                               download_name="analise_cobrancas.xlsx")
