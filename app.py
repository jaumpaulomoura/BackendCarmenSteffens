import logging
from flask import Flask
from flask_cors import CORS
from routes import *
from routes.login import login_bp
from routes.insereNfBolsa import InseriNfBolsa
from routes.insereValorBolsa import InsereValorBolsa
from routes.pesFichaFab import PesFichaFab
from routes.fichaExpedicao import FichaExpedicao
from routes.pesModelEnt import pesModelEnt
from routes.pesModel import pesModel
from routes.cancEnvNf import CancEnvNf
from routes.importaTitulo import ImportaTitulo
from routes.limpaBordero import LimpaBordero
from routes.importVenc import ImportVenc
from routes.grupoPecista import GrupoPecista
from routes.bancaSapato import BancaSapato
from routes.autBolsa import AutBolsa
from database import create_app

app, db = create_app()
CORS(app)

app.register_blueprint(login_bp, name='login')
app.register_blueprint(InseriNfBolsa, name='insereNfBolsa')
app.register_blueprint(InsereValorBolsa, name='insereValorBolsa')
app.register_blueprint(PesFichaFab, name='pesFichaFab')
app.register_blueprint(FichaExpedicao, name='fichaExpedicao')
app.register_blueprint(pesModelEnt, name='pesModelEnt')
app.register_blueprint(pesModel, name='pesModel')
app.register_blueprint(CancEnvNf, name='cancEnvNf')
app.register_blueprint(ImportaTitulo, name='importaTitulo')
app.register_blueprint(LimpaBordero, name='limpaBordero')
app.register_blueprint(ImportVenc, name='importVenc')
app.register_blueprint(GrupoPecista, name='grupoPecista')
app.register_blueprint(BancaSapato, name='bancaSapato')
app.register_blueprint(AutBolsa, name='autBolsa')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # Substitua app.run(...) pelo servidor WSGI apropriado, como Gunicorn.
    app.run(host='0.0.0.0', debug=True, port=5000)
