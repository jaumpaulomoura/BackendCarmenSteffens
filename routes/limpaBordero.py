from flask import Blueprint, Response, current_app, jsonify, request
from database import connect_to_oracle
from flask_cors import CORS
from flask import make_response


LimpaBordero = Blueprint('limpaBordero', __name__)
CORS(LimpaBordero)  # Habilita o CORS para o blueprint

def execute_query(sql_query, params=None):
    try:
        with connect_to_oracle(current_app) as connection:
            with connection.cursor() as cursor:
                if params is not None:
                    cursor.execute(sql_query, params)
                else:
                    cursor.execute(sql_query)

                # Verificar se a consulta é uma instrução SELECT
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                else:
                    # Se a consulta não retornar resultados (por exemplo, UPDATE, DELETE), retornar None
                    results = None

            # Commit aqui após a execução bem-sucedida da consulta
            connection.commit()

        return results
    except Exception as e:
        # Se ocorrer um erro, a transação será revertida automaticamente
        return {'error': str(e)}  # Retorna um dicionário com o erro




@LimpaBordero.route('/api/consultaBordero', methods=['GET'])
def get_query_consultaBordero():
    empresa = request.args.get('empresa')
    bordero = request.args.get('bordero')

    sql_queryModel = """SELECT FN08T.FN08CODEMP AS EMPRESA,
                            fn08t1.FN06NUMTIT AS Titulo, 
                            FN08T1.FN08NUMBOR AS bORDERO,
                            fn08t1.FN06DESDOB AS Desdobro, 
                            fn08t1.FN05CODPRE AS Prefixo, 
                            fn08t1.FN06CCLI1 AS Codigo, 
                            FI15T.FI15NOME AS Nome,  
                            FN08T.FN08DTGERA AS DT_GERADA, FN08T.FN08HORAGE AS HR_GERADA,FN08T.FN08USUARI AS USUARIO,  
                             ROUND((((fn06vrtitu) - (Select NVL(Sum(Fn06VrRecB + Fn06VrDesc - Fn06VrJuro),0) 
                                From fn06t2 
                                Where Fn06t2.Fn06Emp07  = fn06t.Fn06Emp07 
                                And Fn06t2.Fn06TipTit = fn06t.Fn06TipTit 
                                And Fn06t2.Fn06NumTit = fn06t.Fn06NumTit 
                                And Fn06t2.Fn06Desdob = fn06t.Fn06Desdob 
                                And Fn06t2.Fn05CodPre = fn06t.Fn05CodPre 
                                And Fn06t2.Fn06eCli1  = fn06t.Fn06eCli1 
                                And Fn06t2.Fn06tCli1  = fn06t.Fn06tCli1 
                                And Fn06t2.Fn06cCli1  = fn06t.Fn06cCli1))),2) AS Valor , 
                                FN08T.FN08NOMARQ AS ARQUIVO_TXT
                                  
                         FROM FN08T1 
                              
                         INNER JOIN FN08T ON FN08T.FN08CODEMP = fn08t1.FN08CODEMP AND FN08T.FN08NUMBOR = fn08t1.FN08NUMBOR 
                         INNER JOIN FI15T  ON FN08T1.FN06ECLI1 = FI15T.FI15EMP05   AND FN08T1.FN06TCLI1 = FI15T.FI15FLAGCF  AND FN08T1.FN06CCLI1 = FI15T.FI15CODCF 
                         INNER JOIN FN06T  ON FN08T1.FN06EMP07 = FN06T.FN06EMP07   AND FN08T1.FN06TIPTIT = FN06T.FN06TIPTIT AND FN08T1.FN06NUMTIT = FN06T.FN06NUMTIT AND FN08T1.FN06DESDOB = FN06T.FN06DESDOB AND FN08T1.FN05CODPRE = FN06T.FN05CODPRE AND FN08T1.FN06ECLI1 = FN06T.FN06ECLI1 AND FN08T1.FN06TCLI1 = FN06T.FN06TCLI1 AND FN08T1.FN06CCLI1 = FN06T.FN06CCLI1 
                              
                         WHERE FN08T1.FN08CODEMP = 61
                            -- AND FN08T1.FN08NUMBOR = 143
                             AND FN08T1.FN08TIPTIT = 'R' AND FN08T.FN08CODEMP = :empresa AND TO_NUMBER(FN08T.FN08NUMBOR DEFAULT 0 ON CONVERSION ERROR) = :bordero """
                             
                             
    params = {'empresa': empresa, 'bordero': bordero}

    print(params)
    print(sql_queryModel)
    results = execute_query(sql_queryModel, params)

    if isinstance(results, Response):
        return results

    if 'error' in results:
        return jsonify(results)

    return jsonify(results)




@LimpaBordero.route('/api/limpaBordero', methods=['GET'])
def get_query_LimpaBordero():
    empresa = request.args.get('empresa')
    bordero = request.args.get('bordero')

    sqlUPDATE = """UPDATE FN08T  
                    SET FN08DTGERA = (SELECT NULL FROM FN08T    
                                        WHERE FN08CODEMP = :empresa      
                                        AND TO_NUMBER(FN08NUMBOR DEFAULT 0 ON CONVERSION ERROR) = :bordero) 
                    , FN08HORAGE = (SELECT NULL FROM FN08T    
                                        WHERE FN08CODEMP = :empresa      
                                        AND TO_NUMBER(FN08NUMBOR DEFAULT 0 ON CONVERSION ERROR) = :bordero) 
                    , FN08NOMARQ = (SELECT NULL FROM FN08T    
                                        WHERE FN08CODEMP = :empresa      
                                        AND TO_NUMBER(FN08NUMBOR DEFAULT 0 ON CONVERSION ERROR) = :bordero) 
                    WHERE FN08CODEMP = :empresa    AND TO_NUMBER(FN08NUMBOR DEFAULT 0 ON CONVERSION ERROR) = :bordero """

    sqlUPDATE2 = """UPDATE FN08T1    
                    SET FN08SITRET = (SELECT 'N' FROM FN08T    
                                        WHERE FN08CODEMP = :empresa      
                                        AND TO_NUMBER(FN08NUMBOR DEFAULT 0 ON CONVERSION ERROR) = :bordero) 
                    WHERE FN08CODEMP = :empresa   AND TO_NUMBER(FN08NUMBOR DEFAULT 0 ON CONVERSION ERROR) = :bordero """

    params = {'empresa': empresa, 'bordero': bordero}

    print(params)

    # Execute as consultas e obtenha os resultados
    results_update = execute_query(sqlUPDATE, params)
    results_update2 = execute_query(sqlUPDATE2, params)

    # Verifique se há erros nos resultados
    if results_update is not None and 'error' in results_update:
        return jsonify(results_update)

    # Repita a verificação para results_update2
    if results_update2 is not None and 'error' in results_update2:
        return jsonify(results_update2)

    # Se não houver erros, retorne uma resposta de sucesso
    return jsonify({'success': True})







    # sqlINSERT = """INSERT INTO CF03T (CF03EMP, CF03LOGIN, CF03DTHR, CF03PRG, CF03ACAO, CF03ADM) VALUES ( 
    #                     :empresa  , 
    #                     :login,  
    #                     to_date(:dataHoraAtual, 'DD/MM/YYYY HH24:MI:SS'), 
    #                     'Limpar Geracao Bordero',  
    #                     'Limpou a Geracaodo Bordero   bordero  ', 
    #                     'Z') """