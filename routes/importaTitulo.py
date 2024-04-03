import cx_Oracle
from flask_cors import CORS
from flask import Blueprint, current_app, jsonify, request

from database import connect_to_oracle, connect_to_oracle_loja

ImportaTitulo = Blueprint('importaTitulo', __name__)

@ImportaTitulo.route('/api/importaTitulo/empresa', methods=['GET'])
def consultar_mpresa():
    app = current_app   
    connection = connect_to_oracle(app)
    cursor = connection.cursor()
    emp = request.args.get('emp')
    sqlempresa="""select trim(cf01renome) as empresa from CF01T   where CF01CODEMP = :emp
    """
    try:
        # Monta a consulta SQL completa
        

        # Cria o dicionário de parâmetros
        paramsEmp = {'emp': emp}

        # Executa a consulta SQL com os parâmetros fornecidos
        cursor.execute(sqlempresa, paramsEmp)

        columns = [col[0] for col in cursor.description]
        resultsEmp = [dict(zip(columns, row)) for row in cursor.fetchall()]
        response = jsonify(resultsEmp)
        response.headers.add("Access-Control-Allow-Origin", "*")  
        return response
        

    finally:
        cursor.close()
        connection.close()
    
    
@ImportaTitulo.route('/api/importaTitulo', methods=['GET'])
def importa_titulos():
        app = current_app   
        emp = request.args.get('emp')
        vIni = request.args.get('vIni')
        vFin = request.args.get('vFin')
        bordero = request.args.get('bordero')
        eIni = request.args.get('eIni')
        eFin = request.args.get('eFin')
        prefixos = request.args.get('prefixos')
        cliente = request.args.get('cliente')
        tipTit = request.args.get('tipTit')
        json_filters = {
            "emp": emp,
            "vIni": vIni,
            "vFin": vFin,
            "eIni": eIni,
            "eFin": eFin,
            "bordero": bordero,
            "prefixos": prefixos,
            "cliente": cliente,
            "tipTit": tipTit,
        }
        
        where_clause = []
        if "emp" in json_filters and json_filters["emp"]:
            where_clause.append(f"  a.fn06emp07 = '{json_filters['emp']}'")
        if "vIni" in json_filters and json_filters["vIni"]:
            where_clause.append(f"  a.Fn06DtVenc >= to_date('{json_filters['vIni']}', 'DD/MM/YYYY')")
        if "vFin" in json_filters and json_filters["vFin"]:
            where_clause.append(f"  a.Fn06DtVenc <= to_date('{json_filters['vFin']}', 'DD/MM/YYYY')")
        if "eIni" in json_filters and json_filters["eIni"]:
            where_clause.append(f"  a.fn06dtEmis >= to_date('{json_filters['eIni']}', 'DD/MM/YYYY')")
        if "eFin" in json_filters and json_filters["eFin"]:
            where_clause.append(f"  a.fn06dtEmis <= to_date('{json_filters['eFin']}', 'DD/MM/YYYY')")
        if "bordero" in json_filters and json_filters["bordero"]:
            where_clause.append(f"  a.fn06numbor = '{json_filters['bordero']}'")
        if "prefixos" in json_filters and json_filters["prefixos"]:
            prefix_list = "','".join(json_filters["prefixos"])
            where_clause.append(f"  a.fn05codpre IN ('{prefix_list}')")
        if "cliente" in json_filters and json_filters["cliente"]:
            where_clause.append(f"  a.FN06CCLI1 IN ('{json_filters['cliente']}')")
        if "tipTit" in json_filters and json_filters["tipTit"]:
            where_clause.append(f"  a.FN06TIPTIT IN ('{json_filters['tipTit']}')")    
        if where_clause:
            where_clause = " AND ".join(where_clause)
        else:
            where_clause = "" 
        print("Valor da where_clause:", where_clause)  # Adicione esta linha para depurar

        sqlImportTit="""
        SELECT (SELECT FI15CODCF ||' - ' || FI15NOME  FROM FI15T WHERE FI15EMP05 = a.fn06ecli1 AND FI15CODCF = a.fn06ccli1) AS NOME_LOJA, a.fn06emp07 AS CODIGO_EMPRESA,
        a.fn06numbor,
        a.FN06TIPTIT AS TIPO_TITULO,
        a.FN06NUMTIT AS NUMERO_TITULO,
        a.FN06DESDOB AS DESDOBRO,
        a.FN05CODPRE AS PREFIXO,
        a.FN06ECLI1 AS EMPRESA_CLIENTE,
        a.FN06TCLI1 AS TIPO_CADASTRO,
        a.FN06CCLI1 AS CODIGO_CLIENTE,
        a.FN06ECLI2 AS EMPRESA_MATRIZ,
        a.FN06TCLI2 AS TIPO_CADASTRO_CLI,
        a.FN06CCLI1 AS CODIGO_MATRIZ,
        to_char(a.FN06DTEMIS,'DD/MM/YYYY') AS DATA_EMISSAO,
        to_char(a.FN06DTVENC,'DD/MM/YYYY') AS DATA_VENCIMENTO,
        to_char(SYSDATE,'DD/MM/YYYY') AS DATA_DIGITACAO,
        a.FN06ULTDES AS ULTIMO_DESDOBRO,
        a.FN06AVIAPR AS VISTA_APRESENTACAO,
        '0' AS QTD_COMPROVANTES_REC,
        a.FN06AT03CO AS CODIGO_MOEDA,
        a.FN06EM08 AS EMPRESA_BORDERO,
        a.FN06NUMBOR AS NUMERO_BORDERO,
        a.FN06CONTA1 AS CONTA,
        a.FN06CODBA0 AS CONTA_BANCO,
        a.FN06CODOPE AS CODIGO_OPERACAO,
        a.FN06TITBAN AS NUM_TIT_BANCO,
        TO_NUMBER((Select REPLACE(a.Fn06VrTitu - nvl(Sum(d.Fn06VrRecB + d.Fn06VrDesc - d.Fn06VrJuro),0),',','.')
        FROM fn06t2 d
        WHERE d.Fn06Emp07  = a.Fn06Emp07
        AND d.Fn06TipTit = a.Fn06TipTit
        AND d.Fn06NumTit = a.Fn06NumTit
        AND d.Fn06Desdob = a.Fn06Desdob
        AND d.Fn05CodPre = a.Fn05CodPre
        AND d.Fn06eCli1  = a.Fn06eCli1
        AND d.Fn06tCli1  = a.Fn06tCli1
        AND d.Fn06cCli1  = a.Fn06cCli1),'99999999999999.99') AS VALOR_TITULO,
        '0' AS VALOR_ABATIMENTO,
        a.FN06FDESCP AS DESCONTO_PAGAMENTO,
        a.FN06DSDIME AS DIARIO_MENSAL,
        a.FN06LIMDES AS QTD_DIAS_LIMITE,
        a.FN06VLRCOR AS VALOR_CORRECAO,
        a.FN06HISTIT AS HISTORICO,
        a.FN06PRXSEQ AS PROXIMA_SEQUENCIA,
        a.FN06ULTSEQ AS ULTIMA_SEQUENCIA,
        a.FN06SITCOB AS SITUACAO_COBRANCA,
        a.FN06NF AS NOTA_FISCAL,
        a.FN06BLOK AS BLOQUEADO,
        a.FN06EMP05 AS EMPRESA_REPRESENTANTE,
        a.FN06CODREP AS REPRESENTANTE,
        to_char(a.FN06DTVENC,'DD/MM/YYYY') AS DATA_VENCIMENTO_ORIGINAL,
        a.FN06TIPBOR AS TIPO_BORDERO,
        a.FN06CODBAR AS CODIGO_BARRAS,
        a.FN06CODEMP AS EMPRESA_NF,
        a.FN06MODELO AS MODELO_NOTA,
        a.FN06RESREP AS RESPONSABILIDADE_REP,
        '0' AS VR_OUTRAS_MOEDAS,
        'INTEGRADO' AS IDENTIFICADOR_INTEGRACAO        
        FROM fn06t a
        inner join fi15t b
        on a.FN06ecli1 = b.FI15EMP05
        AND a.FN06TCLI1 = b.FI15FLAGCF
        AND a.FN06CCLI1 = b.FI15CODCF
        inner join FI28T g        
        on B.FI15EMP05 = g.FI28CODEMP
        AND B.FI15FLAGCF = g.FI28FLAGCF
        AND B.FI15TIPOCL = g.FI28TIPOCL
        WHERE a.FN06TIPTIT = 'R'       
        AND TRUNC(a.Fn06VrTitu,2) > (SELECT nvl(Sum(c.Fn06VrRecB + c.Fn06VrDesc - c.Fn06VrJuro),0) 
                                                            FROM fn06t2 c
                                                            WHERE c.Fn06Emp07  = a.Fn06Emp07
                                                            AND c.Fn06TipTit = a.Fn06TipTit
                                                            AND c.Fn06NumTit = a.Fn06NumTit
                                                            AND c.Fn06Desdob = a.Fn06Desdob
                                                            AND c.Fn05CodPre = a.Fn05CodPre
                                                            AND c.Fn06eCli1  = a.Fn06eCli1
                                                            AND c.Fn06tCli1  = a.Fn06tCli1
                                                            AND c.Fn06cCli1  = a.Fn06cCli1)
                                                            
        
        """   
        sqlImportTit += f" and {where_clause}"

        # Agora você pode imprimir a consulta SQL completa
        # print("Consulta SQL construída:\n", sqlImportTit)
        
        
        connection = connect_to_oracle(app)
        cursor = connection.cursor()

        cursor.execute(sqlImportTit)

        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        def convert_lob_to_str(row):
            for key, value in row.items():
                if isinstance(value, cx_Oracle.LOB):
                    row[key] = value.read()
            return row

        converted_results = [convert_lob_to_str(row) for row in results]

        return jsonify(converted_results)



@ImportaTitulo.route('/api/importaTitulo/consultarCli', methods=['GET'])
def consultar_cliente():
    app = current_app   
    empCli = request.args.get('empCli')
    codCli = request.args.get('codCli')
    print("Valor de 'empCli' recebido:", empCli)  # Adicione esta linha para imprimir o valor de 'empCli'
    print("Valor de 'codCli' recebido:", codCli)    # Adicione esta linha para imprimir o valor de 'codCli'
   
    
    connection = connect_to_oracle(app)
    
    if connection:
        cursor = connection.cursor()
        query = f"SELECT FI15CODCF, TRIM(FI15NOME) AS FI15NOME FROM FI15T WHERE FI15EMP05 = :empCli AND FI15CODCF = :codCli"
        
        try:
            cursor.execute(query, empCli=empCli, codCli=codCli)
            columns = [col[0] for col in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            response = jsonify(results)
            response.headers.add("Access-Control-Allow-Origin", "*")
            return response
        except cx_Oracle.Error as error:
            print("Erro na consulta (clientes):", error)
        finally:
            cursor.close()
            connection.close()
    
    return jsonify([])


@ImportaTitulo.route('/api/importaTitulo/consultarForn', methods=['GET'])
def consultar_fornecedor():
    app = current_app   
    empresa = request.args.get('empresa')
    codigo = request.args.get('codigo')
    
    connection = connect_to_oracle_loja(app)
    
    if connection:
        cursor = connection.cursor()
        query = f"SELECT FI15CODCF, TRIM(FI15NOME) AS FI15NOME FROM FI15T WHERE FI15EMP05 = :empresa AND FI15CODCF = :codigo"
        
        try:
            cursor.execute(query, empresa=empresa, codigo=codigo)
            columns = [col[0] for col in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            response = jsonify(results)
            response.headers.add("Access-Control-Allow-Origin", "*")
            return response
        except cx_Oracle.Error as error:
            print("Erro na consulta (fornecedores):", error)
        finally:
            cursor.close()
            connection.close()
    
    return jsonify([])