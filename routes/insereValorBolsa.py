import cx_Oracle
from flask import Blueprint, current_app, jsonify, request
from database import connect_to_oracle

InsereValorBolsa = Blueprint('insereValorBolsa', __name__)


@InsereValorBolsa.route('/api/insereValorBolsa', methods=['POST'])
def inserirValorBolsa():
    dataNfBolsa = request.get_json()
    print("Dados recebidos:", dataNfBolsa)  # Adicione esta linha para imprimir os dados
    inserir_dados(dataNfBolsa)
    return 'Inserção de dados concluída com sucesso'


def inserir_dados(lista_de_dados):
    app = current_app
    conexao = connect_to_oracle(app)
    cursor = conexao.cursor()

    try:
        cursor.executemany('''
            INSERT INTO PRODUCAO.TB_PRECO_BOLSA (CODPRO61, MODELO, MATPRIMA, MAOOBRA22, RETENCAO, PRECO, DTVIGENCIA)
            VALUES (
                NVL((SELECT CO13CODPRO FROM CO13T WHERE CO13EMP06 = 61 AND SUBSTR(REPLACE(TRIM(CO13DESCRX), '.', ''), 1, 11) = :MODELO AND CO13BLOQ = 'N'), 0),
                :MODELO,
                :MATPRIMA,
                :MAOOBRA22,
                :RETENCAO,
                :PRECO,
                to_date(:DTVIGENCIA, 'dd/mm/yyyy')
            )
        ''', [
            {
                'MODELO': data.get('MODELO', None),
                'MATPRIMA': data.get('MATPRIMA', None),
                'MAOOBRA22': data.get('MAOOBRA22', None),
                'RETENCAO': data.get('RETENCAO', None),
                'PRECO': data.get('PRECO', None),
                'DTVIGENCIA': data['DTVIGENCIA']
            } for data in lista_de_dados
        ])
    except cx_Oracle.Error as error:
        print("Erro durante a inserção:", error)

    conexao.commit()
    conexao.close()
