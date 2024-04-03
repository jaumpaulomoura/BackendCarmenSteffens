
import cx_Oracle
from flask import Blueprint, current_app, jsonify, request

from database import connect_to_oracle

ImportVenc = Blueprint('importVenc', __name__)
       
@ImportVenc.route('/api/importVenc', methods=['POST'])
def importVenc():
    
    dataImportVenc = request.get_json()
    inserir_dados(dataImportVenc)
    return 'Alterado com sucesso'


def inserir_dados(lista_de_dados):
    app = current_app
    conexao = connect_to_oracle(app)
    cursor = conexao.cursor()

    for data in lista_de_dados:
        try:
            print(f"Data antes da inserção: {data}")
            
            # Verifique se as chaves necessárias estão presentes no dicionário antes de acessá-las
            ano_ped = int(data.get('anoPed', 0))
            pedido = int(data.get('pedido', 0))
            cond_pag = int(data.get('condPag', 0))

            print(f"Dados convertidos: anoPed={ano_ped}, pedido={pedido}, condPag={cond_pag}")

            cursor.execute('''
                update co23t set CO23CODPGT = :condPag, CO23SECPGT=:condPag, CO23FIXA='S' where CO23codemp = 61 and co23codped = :pedido and CO23ANO = :anoPed
            ''', {
                'anoPed': ano_ped,
                'pedido': pedido,
                'condPag': cond_pag
            })

            print("Inserção bem-sucedida.")

        except cx_Oracle.Error as error:
            print("Erro durante a alteração:", error)

    conexao.commit()
    conexao.close()
