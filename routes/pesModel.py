from flask import Blueprint, current_app, jsonify, request, send_file
from database import connect_to_oracle
import pandas as pd
import io

pesModel = Blueprint('pesModel', __name__)
def execute_query(sql_query, params=None):
    app = current_app
    connection = connect_to_oracle(app)
    cursor = connection.cursor()

    if params is not None:
        cursor.execute(sql_query, params)
    else:
        cursor.execute(sql_query)

    columns = [col[0] for col in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()
    connection.close()

    return results

# Criar a API usando Flask

# Definir um endpoint para a rota que retornará os resultados da consulta SQL em JSON
@pesModel.route('/api/query', methods=['GET'])
def get_query_results():
    app = current_app
    connection = connect_to_oracle(app)
    cursor = connection.cursor()
    # Exemplo de consulta SQL. Substitua pela sua consulta desejada.
    sql_queryModel = "SELECT  distinct M.PC23MODELO, trim(B.PC23TAMANH)as tamanho,sum(B.PC23QTDTAM) TOTAL,t.ordem FROM PC23T M INNER JOIN PC23TA A ON M.PC23EMP08 = A.PC23EMP08 AND M.PC23ANO = A.PC23ANO AND M.PC23FICHA = A.PC23FICHA INNER JOIN PC23T1 B ON M.PC23EMP08 = B.PC23EMP08 AND M.PC23ANO = B.PC23ANO AND M.PC23FICHA = B.PC23FICHA LEFT JOIN TAMANHO T ON TRIM(B.PC23TAMANH)=T.TAMANHO      WHERE trim(M.PC23MODELO) = :modelo AND A.PC23DATA >= TO_DATE(:dataInicial, 'DD/MM/YYYY') AND A.PC23DATA <= TO_DATE(:dataFinal, 'DD/MM/YYYY')  AND A.PC23C_CUST = :custo group by M.PC23MODELO,B.PC23TAMANH,t.ordem order by M.PC23MODELO,t.ordem"


    # Recebendo os parâmetros da URL
    modelo = request.args.get('modelo')
    dataInicial = request.args.get('dataInicial')
    dataFinal = request.args.get('dataFinal')
    custo = request.args.get('custo')

    # Verifica se os parâmetros foram fornecidos e monta o dicionário de parâmetros para a consulta SQL
    params = {
        'modelo': modelo,
        'dataInicial': dataInicial,
        'dataFinal': dataFinal,
        'custo': custo
    }

    # Executa a consulta SQL com os parâmetros fornecidos
    results = execute_query(sql_queryModel, params)
    cursor.close()
    connection.close()
    return jsonify(results)

sql_queryModel = "SELECT  distinct M.PC23MODELO, trim(B.PC23TAMANH)as tamanho,sum(B.PC23QTDTAM) TOTAL,t.ordem FROM PC23T M INNER JOIN PC23TA A ON M.PC23EMP08 = A.PC23EMP08 AND M.PC23ANO = A.PC23ANO AND M.PC23FICHA = A.PC23FICHA INNER JOIN PC23T1 B ON M.PC23EMP08 = B.PC23EMP08 AND M.PC23ANO = B.PC23ANO AND M.PC23FICHA = B.PC23FICHA LEFT JOIN TAMANHO T ON TRIM(B.PC23TAMANH)=T.TAMANHO     WHERE trim(M.PC23MODELO) = :modelo AND A.PC23DATA >= TO_DATE(:dataInicial, 'DD/MM/YYYY') AND A.PC23DATA <= TO_DATE(:dataFinal, 'DD/MM/YYYY')  AND A.PC23C_CUST = :custo group by M.PC23MODELO,B.PC23TAMANH, t.ordem order by M.PC23MODELO,t.ordem"

@pesModel.route('/api/export/excel', methods=['GET'])
def export_results_to_excel():
    app = current_app
    connection = connect_to_oracle(app)
    cursor = connection.cursor()
    try:
        # Recebendo os parâmetros da URL
        modelo = request.args.get('modelo')
        dataInicial = request.args.get('dataInicial')
        dataFinal = request.args.get('dataFinal')
        custo = request.args.get('custo')

        # Verifica se os parâmetros foram fornecidos e monta o dicionário de parâmetros para a consulta SQL
        params = {
            'modelo': modelo,
            'dataInicial': dataInicial,
            'dataFinal': dataFinal,
            'custo': custo
        }

        # Executa a consulta SQL com os parâmetros fornecidos
        results = execute_query(sql_queryModel, params)

        # Cria um DataFrame pandas com os resultados
        df = pd.DataFrame(results)

        # Cria um arquivo Excel a partir do DataFrame usando a biblioteca openpyxl
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='openpyxl')
        df.to_excel(writer, sheet_name='Resultado', index=False)

        # Salvar a pasta de trabalho (workbook) no objeto BytesIO usando openpyxl
        workbook = writer.book
        worksheet = writer.sheets['Resultado']

        # Salvar o arquivo usando a planilha worksheet
        workbook.save(output)

        output.seek(0)

        # Enviar o arquivo Excel como resposta
        response = send_file(output, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        response.headers["Content-Disposition"] = "attachment; filename=resultado.xlsx"
        cursor.close()
        connection.close()
        return response

    except Exception as e:
        return jsonify({'error': str(e)})