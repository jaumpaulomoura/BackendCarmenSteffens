
from flask import Blueprint, current_app, jsonify, request
from datetime import datetime, timedelta

from database import connect_to_oracle

PesFichaFab = Blueprint('pesFichaFab', __name__)

@PesFichaFab.route('/api/pesFichaFab', methods=['POST'])
def pesFichaFab():
    app = current_app
    conexao = connect_to_oracle(app)
    cursor = conexao.cursor()
    
    try:
        data = request.json 
        results = []  
        # print(data)
        for obj in data:
            empresa = obj.get('empresa')
            ano = obj.get('ano')
            ficha = obj.get('ficha')
            fabrica = obj.get('fabrica')
            secao = obj.get('secao')
            # print(data)
            sql = ""

            where_clause = []

            if empresa:
                where_clause.append(f"  P.PC23EMP08  = '{empresa}'")
            if ano:
                where_clause.append(f"  p.PC23ANO = '{ano}'")
            if ficha:
                where_clause.append(f"  p.PC23FICHA IN ('{ficha}')")
            if secao:
                where_clause.append(f"  P1.PC23C_CUST IN ('{secao}')")
            if fabrica and secao != "3900":
                where_clause.append(f"  p.PC23CODFAB IN ('{fabrica}')")
            # print("Valor da where_clause:", where_clause) 
            if secao ==3900:
                sql = """SELECT p.PC23EMP08 as EMPRESA, P1.PC23C_CUST AS SECAO, p.PC23ANO  AS ANO, p.PC23FICHA AS FICHA, p.PC23PLANO AS PLANO, trim(p.PC23MODELO) AS MODELO, p.PC23CODFAB AS FABRICA, p2.PCFYSTATUS as STATUS,p.PC23ANO||p.PC23FICHA AS ID
                       FROM PC23T P
                       left JOIN PC23TA P1 ON p.PC23EMP08 = P1.PC23EMP08
                       AND p.PC23ANO = P1.PC23ANO
                       AND p.PC23FICHA = P1.PC23FICHA
                       LEFT JOIN PCFYTA p2 ON p2.PCFYCODEMP = p.PC23EMP08
                       AND p2.PCFYANO = p.PC23ANO
                       AND p2.PCFYFICHA = p.PC23FICHA
                       AND p2.PCFYSTATUS = 0
                       WHERE P1.PC23EMP08  IS not NULL                         
                       AND P1.PC23DATA = '01-jan-0001'"""
                sql += " AND " + " AND ".join(where_clause)
                cursor.execute(sql)
                # print("Consulta SQL construída:\n", sql)
            else:
                sql = """SELECT p.PC23EMP08 AS EMPRESA, P1.PC23C_CUST AS SECAO, p.PC23ANO AS ANO, p.PC23FICHA AS FICHA, p.PC23PLANO AS PLANO, trim(P.PC23MODELO) AS MODELO , p.PC23CODFAB AS FABRICA, 'NAO POSSUI' AS STATUS,p.PC23ANO||p.PC23FICHA AS ID FROM PC23T P
                        left JOIN PC23TA P1
                        ON p.PC23EMP08 = P1.PC23EMP08
                        AND p.PC23ANO = P1.PC23ANO
                        AND p.PC23FICHA = P1.PC23FICHA
                        WHERE P.PC23EMP08  IS not NULL
                        AND P1.PC23DATA = '01-jan-0001'"""
                sql += " AND " + " AND ".join(where_clause)
                group_by_clause = "GROUP BY p.PC23EMP08, P1.PC23C_CUST, p.PC23ANO, p.PC23FICHA, p.PC23PLANO, P.PC23MODELO, p.PC23CODFAB"
                sql += " " + group_by_clause
                cursor.execute(sql)
                # print("Consulta SQL construída:\n", sql)
            columns = [col[0] for col in cursor.description]
            query_result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            results.extend(query_result)
        # print(results)
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}),
    


@PesFichaFab.route('/api/pesFichaFab/atualizaFab', methods=['POST'])
def atualizaFab():
    try:
        data = request.json
        # print("Dados recebidos:", data)

        conexao = connect_to_oracle(current_app)
        cursor = conexao.cursor()

        current_timestamp = datetime.now()  # Captura o timestamp atual

        for i, item in enumerate(data['currentPageData']):
            empresa = item['EMPRESA']
            ano = item['ANO']
            ficha = item['FICHA']
            secao = item['SECAO']
            fabrica = item['FABRICA']
            new_fabrica = int(data['newFabrica'])
            login = data['login']

            acao = f'Alterou Fabrica da Ficha {ficha} de {fabrica} para {new_fabrica}'
            # print("Empresa:", empresa)
            # print("Ano:", ano)
            # print("Ficha:", ficha)
            # print("Seção:", secao)
            # print("Nova Fábrica:", new_fabrica)
            # print("Login:", login)

            # Verifica se a seção é "3900" para determinar a query a ser executada
            if secao == "3900":
                sql_update = '''
                    UPDATE PC23T SET PC23CODFAB = :new_fabrica
                    WHERE PC23EMP08 = :empresa
                    AND PC23ANO = :ano
                    AND PC23FICHA = :ficha
                '''
                sql1_update = '''
                    UPDATE PCFYTA SET PCFYCODFAB = :new_fabrica
                    WHERE PCFYCODEMP = :empresa
                    AND PCFYANO = :ano
                    AND PCFYFICHA = :ficha
                '''
            else:
                sql_update = '''
                    UPDATE PC23T SET PC23CODFAB = :new_fabrica
                    WHERE PC23EMP08 = :empresa
                    AND PC23ANO = :ano
                    AND PC23FICHA = :ficha
                '''

            # Executa a query correspondente
            cursor.execute(sql_update, {
                'new_fabrica': new_fabrica,
                'empresa': empresa,
                'ano': ano,
                'ficha': ficha
            })

            # Se a seção for "3900", também executa a segunda query
            if secao == "3900":
                cursor.execute(sql1_update, {
                    'new_fabrica': new_fabrica,
                    'empresa': empresa,
                    'ano': ano,
                    'ficha': ficha
                })

            # Insere o log com timestamp ajustado
            adjusted_timestamp = current_timestamp + timedelta(seconds=i)
            sql_log = '''
                INSERT INTO CF03T (CF03EMP, CF03LOGIN, CF03DTHR, CF03PRG, CF03ACAO, CF03ADM) 
                VALUES (:empresa, :login, :adjusted_timestamp, 'Troca de ficha de fabrica', :acao, 'Z')
            '''
            cursor.execute(sql_log, {
                'empresa': empresa,
                'login': login,
                'adjusted_timestamp': adjusted_timestamp,
                'acao': acao
            })

            # Restante do seu código...

        conexao.commit()
        conexao.close()
        return jsonify({'message': 'Dados atualizados com sucesso'})

    except Exception as e:
        print("Erro:", e)
        return jsonify({'message': 'Erro interno do servidor'}), 500

        


    
    
    
    
    