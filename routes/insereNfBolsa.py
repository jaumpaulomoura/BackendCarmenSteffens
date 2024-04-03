
import cx_Oracle
from flask import Blueprint, current_app, jsonify, request

from database import connect_to_oracle

InseriNfBolsa = Blueprint('inseriNfBolsa', __name__)
       
@InseriNfBolsa.route('/api/inseriNfBolsa', methods=['POST'])
def inseriNfBolsa():
    dataNfBolsa = request.get_json()
    print(dataNfBolsa)
    inserir_dados(dataNfBolsa)
    return 'JSON recebido com sucesso'




def inserir_dados(lista_de_dados):
    app = current_app
    conexao = connect_to_oracle(app)
    cursor = conexao.cursor()

    for data in lista_de_dados:
        try:
            cursor.execute(''' INSERT INTO TB_NF_BOLSA (CODFOR, DTEMI, NF, ITEM, MODREF ,STATUSNF,DTREF) VALUES (:codFor,to_date(:dataEmissao,'dd/mm/yyyy'), :nf,:item, :modRef :status,to_date(:dtRef,'dd/mm/yyyy'))
            ''', {
                 'codFor': int(data['codFor']),
                'dataEmissao': data['dataEmissao'],  # Se o banco aceita data como string, nÃ£o precisa converter
                'nf': int(data['nf']),
		'item': int(data['item']),
		'modRef': data.get('modRef', None),
                'status': data.get('status', None),  # Verifique se 'status' existe no dicionÃ¡rio
		'dtRef': data['dtRef']
            })

        except cx_Oracle.Error as error:
            print("Erro durante a inserÃ§Ã£o:", error)

    conexao.commit()
    conexao.close()

    