import os
from decimal import Decimal
from datetime import date, datetime
from flask import Flask
from flask.json.provider import DefaultJSONProvider


class DecimalJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


def create_app() -> Flask:
    app = Flask(__name__)
    app.json = DecimalJSONProvider(app)
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev")

    from .routes import bp
    app.register_blueprint(bp)
    return app
