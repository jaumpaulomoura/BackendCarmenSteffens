import cx_Oracle
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

oracle_user = "PRODUCAO"
oracle_password = "a"
oracle_host = "192.168.1.62:1521/chbprd"

oracle_user_loja = "PRODUCAO"
oracle_password_loja = "a"
oracle_host_loja = "192.168.1.62:1521/ORCL"

def create_app():
    app = Flask(__name__)
    
    # Configuração do primeiro banco de dados
    app.config['SQLALCHEMY_DATABASE_URI'] = f'oracle://{oracle_user}:{oracle_password}@{oracle_host}'
    db = SQLAlchemy(app)

    # Configuração do segundo banco de dados
    app.config['SQLALCHEMY_BINDS'] = {
        'loja': f'oracle://{oracle_user_loja}:{oracle_password_loja}@{oracle_host_loja}'
    }

    return app, db

def connect_to_oracle(app: Flask):
    with app.app_context():
        return cx_Oracle.connect(oracle_user, oracle_password, oracle_host)

def connect_to_oracle_loja():
    return cx_Oracle.connect(oracle_user_loja, oracle_password_loja, oracle_host_loja)

app, db = create_app()
