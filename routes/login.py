from flask import Blueprint, current_app, jsonify, request
from models import Usuario, UsuarioDao

login_bp = Blueprint('login', __name__)

@login_bp.route('/api/login', methods=['POST'])
def login():
    app = current_app
    from app import app

    data = request.get_json()

    if 'login' not in data or 'senha' not in data:
        return jsonify({"error": "Credenciais incompletas"}), 400

    login = data['login']
    senha = data['senha']

    usuario = Usuario(login, senha)
    usuarioDao = UsuarioDao(usuario, app)

    if usuarioDao.verificarUsuario():
        return jsonify({"message": "Login bem-sucedido"}), 200
    else:
        return jsonify({"error": "Login falhou. Verifique suas credenciais"}), 401
