import os
from flask import Flask
from dotenv import load_dotenv

load_dotenv()


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

    return app
