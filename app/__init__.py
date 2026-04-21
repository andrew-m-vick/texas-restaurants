import os
from flask import Flask


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev")

    from .routes import bp
    app.register_blueprint(bp)
    return app
