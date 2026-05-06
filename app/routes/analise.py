import os
import uuid
from flask import (Blueprint, request, render_template, send_from_directory,
                   current_app, flash, redirect, url_for, session, abort,
                   after_this_request)

from app.services.conciliacao import processar_conciliacao

analise_bp = Blueprint("analise", __name__)

EXTENSOES = {"xlsx", "xls"}


def _valida(nome):
    return "." in nome and nome.rsplit(".", 1)[1].lower() in EXTENSOES


def _apagar(path):
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


@analise_bp.route("/analisar", methods=["POST"])
def analisar():
    arquivo_params = request.files.get("parametros")
    arquivo_dados  = request.files.get("dados")

    if not arquivo_params or not arquivo_params.filename:
        flash("Envie a planilha de parâmetros.")
        return redirect(url_for("main.index"))

    if not arquivo_dados or not arquivo_dados.filename:
        flash("Envie a planilha de dados.")
        return redirect(url_for("main.index"))

    if not (_valida(arquivo_params.filename) and _valida(arquivo_dados.filename)):
        flash("Apenas arquivos .xlsx ou .xls são aceitos.")
        return redirect(url_for("main.index"))

    session_id = str(uuid.uuid4())
    upload_dir = current_app.config["UPLOAD_FOLDER"]
    output_dir = current_app.config["OUTPUT_FOLDER"]

    path_params = os.path.join(upload_dir, f"{session_id}_parametros.xlsx")
    path_dados  = os.path.join(upload_dir, f"{session_id}_dados.xlsx")
    arquivo_params.save(path_params)
    arquivo_dados.save(path_dados)

    resultado = processar_conciliacao(path_params, path_dados, session_id, output_dir)

    _apagar(path_params)
    _apagar(path_dados)

    session["session_id"] = session_id

    return render_template("index.html", resultado=resultado, session_id=session_id)


@analise_bp.route("/download/<session_id>")
def download(session_id):
    # Bloqueia se o session_id não pertencer a esta sessão de browser
    if session.get("session_id") != session_id:
        abort(403)

    output_dir = current_app.config["OUTPUT_FOLDER"]
    path_output = os.path.join(output_dir, f"{session_id}_analise.xlsx")

    if not os.path.exists(path_output):
        abort(404)

    # Apaga o arquivo e limpa a sessão após o envio
    @after_this_request
    def limpar(response):
        _apagar(path_output)
        session.pop("session_id", None)
        return response

    return send_from_directory(output_dir, f"{session_id}_analise.xlsx",
                               as_attachment=True,
                               download_name="analise_cobrancas.xlsx")
