from flask import Blueprint, jsonify, request, current_app
from database import connect_to_oracle
import cx_Oracle

CancEnvNf = Blueprint('cancEnvNf', __name__)



@CancEnvNf.route('/api/canEnvNf', methods=['GET'])
def get_query_canEnvNf():
    app = current_app
    sql_Nf = """SELECT A.FT07CODEMP as EMPNF, A.FI16MODELO AS MODNF, A.FT07CODIGO AS NF, B.FI15CODCF AS COD_FOR, B.FI15FLAGCF AS TIP_CLI, B.FI15NOME AS NOME_FOR, to_char(A.FT07DTEMI,'DD/MM/YYYY') AS DT_EMI, A.GTFT07ENVIADO AS ENVIADO
                FROM FT07T A 
                 INNER JOIN FI15T B 
                 ON A.FI15EMP05 = B.FI15EMP05 
                 AND A.FI15FLAGCF = B.FI15FLAGCF 
                 AND A.FI15CODCF = B.FI15CODCF 
                 WHERE  A.FT07CODEMP = :empNf
                 AND A.FI16MODELO = :modNf
                 AND A.FT07CODIGO = :nota
    """
    
    
    empNf = request.args.get('empNf')
    modNf = request.args.get('modNf')
    nota = request.args.get('nota')
    
    connection = connect_to_oracle(app)
    cursor = connection.cursor()

    try:
        
        paramsNf = {'empNf': empNf,
                  'modNf': modNf,
                  'nota': nota}

        # Executa a consulta SQL com os parâmetros fornecidos
        cursor.execute(sql_Nf, paramsNf)

        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        response = jsonify(results)
        response.headers.add("Access-Control-Allow-Origin", "*")  
        return response
        

    finally:
        cursor.close()
        connection.close()
        
@CancEnvNf.route('/api/atuaEnvNf', methods=['GET'])
def get_query_atuaEnvNf():    
    app = current_app   
    empNf = request.args.get('empNf')
    modNf = request.args.get('modNf')
    nota = request.args.get('nota')
    connection = connect_to_oracle(app)
    cursor = connection.cursor()    
    try:        
        query_delete = f"""DELETE FROM FT78T
                            WHERE FT78CODEMP = '{empNf}' 
                                AND FT78MODELO = '{modNf}' 
                                AND FT78CODIGO = {nota} 
                                AND FT78CODSIS = 'GESTOR'"""
        cursor.execute(query_delete)

        query_update = f"""UPDATE FT07T
                                SET GTFT07ENVIADO = 'N'
                                WHERE FT07CODEMP = '{empNf}' 
                                    AND FI16MODELO = '{modNf}' 
                                    AND FT07CODIGO = {nota}"""
        cursor.execute(query_update)
        connection.commit()

        # Retorne uma resposta de sucesso para o front-end
        response_data = {"message": "Consultas executadas com sucesso"}
        return jsonify(response_data), 200

    except cx_Oracle.Error as error:
        # Em caso de erro, retorne uma resposta de erro
        error_message = str(error)
        return jsonify({"error": error_message}), 500

    finally:
        # Sempre feche o cursor e a conexão
        cursor.close()
        connection.close()
