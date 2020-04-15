import os

from flask import Flask
from flask_cache import Cache
from flask_marshmallow import Marshmallow
from flask_sqlalchemy import SQLAlchemy
from noaadb import DATABASE_URI
from noaadb.schema.models import Base
from flask_swagger_ui import get_swaggerui_blueprint
basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)

# Configure the SQLAlchemy part of the app instance
app.config['SQLALCHEMY_ECHO'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # TODO signalling support
app.config["DEBUG"] = False if not "DEBUG" in os.environ else bool(int(os.environ["DEBUG"]))
cache = Cache(app,config={'CACHE_TYPE': 'filesystem' if not app.config["DEBUG"] else 'null', 'CACHE_DIR': '/tmp'})

# Create the SQLAlchemy db instance
db = SQLAlchemy(app, model_class=Base)
# Initialize Marshmallow
ma = Marshmallow(app)

swagger_blueprint = get_swaggerui_blueprint(
    '/api',
    '/static/swagger.json',
    config={
        'app_name': "NOAADB API"
    }
)
app.register_blueprint(swagger_blueprint, url_prefix='/api')