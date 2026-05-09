import os
from datetime import datetime
from flask import Flask
from dotenv import load_dotenv

load_dotenv()

# Capturado uma vez no startup: representa o momento em que o Railway subiu o container.
_DEPLOY_TIME = datetime.now().strftime("%d/%m/%Y %H:%M")
_CURRENT_YEAR = datetime.now().year
_COMMIT_SHA  = os.environ.get("RAILWAY_GIT_COMMIT_SHA", "")
_VERSION     = _COMMIT_SHA[:7] if _COMMIT_SHA else "local"


def create_app():
    app = Flask(__name__)
    secret = os.getenv("FLASK_SECRET_KEY", "")
    if not secret:
        import secrets
        secret = secrets.token_hex(32)
    app.secret_key = secret
    app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_UPLOAD_MB", 10)) * 1024 * 1024
    upload_dir = os.path.join(os.path.dirname(__file__), "..", "uploads")
    output_dir = os.path.join(os.path.dirname(__file__), "..", "outputs")
    os.makedirs(upload_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    app.config["UPLOAD_FOLDER"] = upload_dir
    app.config["OUTPUT_FOLDER"] = output_dir

    from app.routes.main import main_bp
    from app.routes.analise import analise_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(analise_bp)

    @app.context_processor
    def inject_version():
        return {"app_version": _VERSION, "deploy_time": _DEPLOY_TIME, "current_year": _CURRENT_YEAR}

    return app
