from flask import Blueprint, jsonify, request, current_app
from database import connect_to_oracle
from datetime import datetime, timedelta

AutBolsa = Blueprint('autBolsa', __name__)

def fetch_data_for_date(cursor, data_atual_formatada):
    
            query_notas = """
            SELECT distinct
            C.FT71CODCF||'-'||F.DTREF||'-'||TRIM(F.MODREF) AS ID,
            C.FT71CODCF     AS CÓD,
            TRIM(D.FI15NOME) AS PRESTADOR,
            C.FT71CODNF  	AS NF,
            F.STATUSNF AS STATUS,
            C.FT71DTEMI     AS DTENTREGA,
            B.FT76ITEM      AS ITEM,
            SUBSTR(REPLACE(REPLACE(TRIM(B.FT76XPROD),' ',''),'.',''),-11,11) AS MODELOS,
            (B.FT76QCOM)  AS QTD,
            (B.FT76VUNCOM)    AS VLRUNIT,
            (B.FT76VUNCOM*B.FT76QCOM) AS VLRTOTAL,
            G.MATPRIMA*B.FT76QCOM AS VLRMATPRIMA,
            G.RETENCAO*B.FT76QCOM AS "RETENCAO",
            G.MAOOBRA*B.FT76QCOM AS VLRDEP,
            CASE
                        WHEN  F.DTREF >= '26/01/2024' and F.DTREF <= '10/02/2024' THEN TO_DATE('20/02/2024', 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '11/02/2024' and F.DTREF <= '26/02/2024' THEN TO_DATE('05/03/2024', 'DD/MM/YYYY')  
                        WHEN  F.DTREF >= '27/02/2024' and F.DTREF <= '11/03/2024' THEN TO_DATE('20/03/2024' , 'DD/MM/YYYY') 
                        WHEN  F.DTREF >= '12/03/2024' and F.DTREF <= '25/03/2024' THEN TO_DATE('05/04/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '26/02/2024' and F.DTREF <= '10/04/2024' THEN TO_DATE('20/04/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '11/04/2024' and F.DTREF <= '25/04/2024' THEN TO_DATE('05/05/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '26/04/2024' and F.DTREF <= '10/05/2024' THEN TO_DATE('20/05/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '11/05/2024' and F.DTREF <= '25/05/2024' THEN TO_DATE('05/06/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '26/05/2024' and F.DTREF <= '10/06/2024' THEN TO_DATE('20/06/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '11/06/2024' and F.DTREF <= '25/06/2024' THEN TO_DATE('05/07/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '26/06/2024'  and F.DTREF <= '10/07/2024' THEN TO_DATE('20/07/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '11/07/2024'  and F.DTREF <= '25/07/2024' THEN TO_DATE('05/08/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '26/07/2024'  and F.DTREF <= '10/08/2024' THEN TO_DATE('20/08/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '11/08/2024'  and F.DTREF <= '25/08/2024' THEN TO_DATE('05/09/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '26/08/2024' and F.DTREF <= '10/09/2024' THEN TO_DATE('20/09/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '11/09/2024'  and F.DTREF <= '25/09/2024' THEN TO_DATE('05/10/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '26/09/2024'  and F.DTREF <= '10/10/2024' THEN TO_DATE('20/10/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '11/10/2024'  and F.DTREF <= '25/10/2024' THEN TO_DATE('05/11/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '26/10/2024'  and F.DTREF <= '10/11/2024' THEN TO_DATE('20/11/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '11/11/2024'  and F.DTREF <= '25/11/2024' THEN TO_DATE('05/12/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '26/11/2024' and F.DTREF <= '10/12/2024' THEN TO_DATE('20/12/2024' , 'DD/MM/YYYY')
                        WHEN  F.DTREF >= '11/12/2024' and F.DTREF <= '25/01/2024' THEN TO_DATE('05/02/2024'   , 'DD/MM/YYYY')
                    ELSE TO_DATE('00/01/1900','DD/MM/YYYY')
                    END PAGTO,
            g.codpro61 AS CODPRO61,
            CASE 
                WHEN SUBSTR(SUBSTR(REPLACE(REPLACE(TRIM(B.FT76XPROD),' ',''),'.',''),-11,11),1,2) IN (	'BO', 'CB', 'AO', 'BS', 'BP', 'BY', 'AY', 'AS', 'KB') THEN SUBSTR(SUBSTR(REPLACE(REPLACE(TRIM(B.FT76XPROD),' ',''),'.',''),-11,11),1,5)
                ELSE SUBSTR(SUBSTR(REPLACE(REPLACE(TRIM(B.FT76XPROD),' ',''),'.',''),-11,11),1,4)
            END MOD4DIG,

            'PAGTO '||CASE
                        WHEN  F.DTREF >= '26/01/2024' and F.DTREF <= '10/02/2024' THEN '20/02'
                        WHEN  F.DTREF >= '11/02/2024' and F.DTREF <= '26/02/2024' THEN '05/03'  
                        WHEN  F.DTREF >= '27/02/2024' and F.DTREF <= '11/03/2024' THEN '20/03'  
                        WHEN  F.DTREF >= '12/03/2024' and F.DTREF <= '25/03/2024' THEN '05/04' 
                        WHEN  F.DTREF >= '26/02/2024' and F.DTREF <= '10/04/2024' THEN '20/04' 
                        WHEN  F.DTREF >= '11/04/2024' and F.DTREF <= '25/04/2024' THEN '05/05' 
                        WHEN  F.DTREF >= '26/04/2024' and F.DTREF <= '10/05/2024' THEN '20/05' 
                        WHEN  F.DTREF >= '11/05/2024' and F.DTREF <= '25/05/2024' THEN '05/06' 
                        WHEN  F.DTREF >= '26/05/2024' and F.DTREF <= '10/06/2024' THEN '20/06' 
                        WHEN  F.DTREF >= '11/06/2024' and F.DTREF <= '25/06/2024' THEN '05/07' 
                        WHEN  F.DTREF >= '26/06/2024'  and F.DTREF <= '10/07/2024' THEN '20/07' 
                        WHEN  F.DTREF >= '11/07/2024'  and F.DTREF <= '25/07/2024' THEN '05/08' 
                        WHEN  F.DTREF >= '26/07/2024'  and F.DTREF <= '10/08/2024' THEN '20/08' 
                        WHEN  F.DTREF >= '11/08/2024'  and F.DTREF <= '25/08/2024' THEN '05/09' 
                        WHEN  F.DTREF >= '26/08/2024' and F.DTREF <= '10/09/2024' THEN '20/09' 
                        WHEN  F.DTREF >= '11/09/2024'  and F.DTREF <= '25/09/2024' THEN '05/10' 
                        WHEN  F.DTREF >= '26/09/2024'  and F.DTREF <= '10/10/2024' THEN '20/10' 
                        WHEN  F.DTREF >= '11/10/2024'  and F.DTREF <= '25/10/2024' THEN '05/11' 
                        WHEN  F.DTREF >= '26/10/2024'  and F.DTREF <= '10/11/2024' THEN '20/11' 
                        WHEN  F.DTREF >= '11/11/2024'  and F.DTREF <= '25/11/2024' THEN '05/12' 
                        WHEN  F.DTREF >= '26/11/2024' and F.DTREF <= '10/12/2024' THEN '20/12' 
                        WHEN  F.DTREF >= '11/12/2024' and F.DTREF <= '25/01/2024' THEN '05/02'   
                    ELSE TO_CHAR('00/01/1900')end
                    ||' - NF '|| C.FT71CODNF AS PEDFORN,
            0 VLRDESC,
            0 VLRACRES,
            0 VLRFRETE,
            0 VLROUTRAS,
            B.FT76QCOM*1000 QTD2,
            (B.FT76VUNCOM)*10000 UNIT,
            4 OP,
            1102 CFOP,
            0 SEQ,
            0 as "DESC",
            0 ACRES,
            0  IPI,
            F.ICMS*100 ICMS,
            E.CO13DESCRI DESCITEM,
            E.CO13C_CUST CCUSTO,
            100 GRUPODESP,
            75 CODDESP,
            (F.ICMS/100*B.FT76VUNCOM) AS VLRICMS,
            last_day(C.FT71DTEMI) COMP,
            'ATUAL' AS FECGAMENTO

            FROM FT76T A
            INNER JOIN FT76T1 B ON A.FT76CHAVE = B.FT76CHAVE  
            INNER JOIN FT71t C ON B.FT76CHAVE = C.FT71CHAVE
            LEFT JOIN TB_NF_BOLSA F ON C.FT71CODCF = F.CODFOR AND C.FT71CODNF = F.NF AND B.FT76ITEM = F.ITEM
            INNER JOIN FI15T D ON C.FT71CODCF = D.FI15CODCF   AND   C.FT71EMP05 = D.FI15EMP05 
            FULL JOIN CO13T E ON TRIM(F.MODREF) = SUBSTR(REPLACE(TRIM(E.CO13DESCRI), '.', ''), 1, 11)  AND E.CO13BLOQ = 'N'

            LEFT JOIN TB_PRECO_BOLSA G ON TRIM(F.MODREF)= trim(G.MODELO) and g.DTVIGENCIA>='30/12/2090'

            WHERE D.FI33CODSEG='99'  AND B.FT76CFOP IN('5101','5102') AND C.FT71CODCF NOT IN(5000725) 
            --and C.FT71CODCF = 5118554    
            and F.STATUSNF IN ('OK','COMP')
            AND F.DTREF >= TO_DATE(:data_inicial, 'DD/MM/YYYY') 
            AND F.DTREF <= TO_DATE(:data_final, 'DD/MM/YYYY') 
            order by 1
            """


            cursor.execute(query_notas, data_inicial=data_atual_formatada, data_final=data_atual_formatada)
            notas_result = cursor.fetchall()


            query_fichas = """ 
            SELECT 
            D.PC23CODCF||'-'||D.PC23ENVIO||'-'||TRIM(A.PC23MODELO) AS ID,
            D.PC23CODCF     AS COD_PREST,
            TRIM(T.FI15NOME)      AS PRESTADOR,
            A.PC23ANO AS ANO_FICHA,
            A.PC23FICHA 	AS FICHAS,
            D.PC23ENVIO     AS DATA_ENV_FICHA,
            d.pc23horenv     AS HORA_ENV_FICHA,
            D.PC23USUENT    AS USUARIO_ENV,
            CASE WHEN D.PC23DATA >'31/01/1900' THEN D.PC23DATA ELSE NULL END AS       DATA_BX_FICHA,
            d.pc23hordat     AS HORABX_FICHA,
            D.PC23USUARI    AS USUARIO_BX,
            A.PC23PLANO AS PLANO,
            D.PC23C_CUST    AS SECAO,
            TRIM(A.PC23MODELO) 		AS MODELO,
            SUM(B.PC23QTDTAM) AS QTDE_FICHAS,
            G.preco   AS VLRUNIT,
            (SUM(B.PC23QTDTAM) *G.preco) AS VLRTOTAL,
            G.MATPRIMA*(SUM(B.PC23QTDTAM)) AS VLRMATPRIMA,
            G.RETENCAO*(SUM(B.PC23QTDTAM)) AS "RETENCAO",
            G.MAOOBRA*(SUM(B.PC23QTDTAM)) AS VLRDEP



            FROM PC23T A
            INNER JOIN PC23T1 B ON A.PC23EMP08  = B.PC23EMP08  AND A.PC23ANO    = B.PC23ANO    AND A.PC23FICHA  = B.PC23FICHA 
            INNER JOIN PC23TA D ON A.PC23EMP08  = D.PC23EMP08  AND A.PC23ANO    = D.PC23ANO    AND A.PC23FICHA  = D.PC23FICHA
            INNER JOIN FI15T T ON T.FI15EMP05  = D.PC23EMP05 AND T.FI15FLAGCF = D.PC23FLAGCF AND T.FI15CODCF  = D.PC23CODCF
            LEFT JOIN TB_PRECO_BOLSA G ON trim(A.PC23MODELO)= trim(G.MODELO) and g.DTVIGENCIA>='30/12/2090'

            WHERE A.PC23EMP08 = 61
            AND D.PC23C_CUST IN (3674,3694)	
            AND T.FI33CODSEG IN ('99','998')
            AND D.PC23CODCF NOT IN (5049970)
            --AND D.PC23CODCF = 5118554
            AND D.PC23ENVIO >= TO_DATE(:data_inicial, 'DD/MM/YYYY')
            AND D.PC23ENVIO <= TO_DATE(:data_final, 'DD/MM/YYYY') 
            

            GROUP BY  D.PC23CODCF, T.FI15NOME, A.PC23FICHA, D.PC23ENVIO, D.PC23DATA, D.PC23C_CUST, TRIM(A.PC23MODELO), D.PC23USUENT, D.PC23USUARI, d.pc23horenv, d.pc23hordat,A.PC23ANO,A.PC23PLANO ,G.preco,G.MATPRIMA,G.RETENCAO,G.MAOOBRA
            order by 1
            """


            cursor.execute(query_fichas, data_inicial=data_atual_formatada, data_final=data_atual_formatada)
            fichas_result = cursor.fetchall()

            return notas_result, fichas_result




def process_data(notas_result, fichas_result):

            accumulated_data = []


          
            for nota in notas_result:
                id_nota = nota[0]
                cód = nota[1]
                prestador = nota[2]
                nf = nota[3]
                status = nota[4]
                dtentrega = nota[5]
                item = nota[6]
                modelos = nota[7]
                qtd_nf = int(nota[8])
                vlrunit = nota[9]
                vlrtotal = nota[10]
                vlrmatprima = nota[11]
                vlrmatprima_1 = nota[12]
                vlrdep = nota[13]
                pagto = nota[14]
                codpro61 = nota[15]
                mod4dig = nota[16]
                pedforn = nota[17]
                vlrdesc = nota[18]
                vlracres = nota[19]
                vlrfrete = nota[20]
                vlroutras = nota[21]
                qtd2 = int(nota[22])
                unit = nota[23]
                op = nota[24]
                cfop = nota[25]
                seq = nota[26]
                desc = nota[27]
                acres = nota[28]
                ipi = nota[29]
                icms = nota[30]
                descitem = nota[31]
                ccusto = nota[32]
                grupodesp = nota[33]
                coddesp = nota[34]
                vlricms = nota[35]
                comp = nota[36]

                fichas_disponiveis = []    
                for ficha in fichas_result:
                    # print(ficha)
                    id_ficha = ficha[0]
                    ano_ficha = ficha[3]
                    fichas = ficha[4]
                    data_env_ficha = ficha[5]
                    hora_env_ficha = ficha[6]
                    usuario_env = ficha[7]
                    data_bx_ficha = ficha[8]
                    horabx_ficha = ficha[9]
                    usuario_bx = ficha[10]
                    plano = ficha[11]
                    secao = ficha[12]
                    modelo=ficha[13]
                    qtd_ficha= int(ficha[14])        
                    vlrunit = ficha[15]
                    vlrtotal = ficha[16]
                    vlrmatprima = ficha[17]
                    vlrmatprima_1 = ficha[18]
                    vlrdep = ficha[19]
                    
                    
                    
                    if id_ficha == id_nota  and qtd_ficha > 0 :
                        fichas_disponiveis.append(ficha)
                quantidade_atribuida = 0
                fichas_atribuidas = []
                for ficha in fichas_disponiveis:
                    # print(ficha)
                    id_ficha = ficha[0]
                    ano_ficha = ficha[3]
                    fichas = ficha[4]
                    data_env_ficha = ficha[5]
                    hora_env_ficha = ficha[6]
                    usuario_env = ficha[7]
                    data_bx_ficha = ficha[8]
                    horabx_ficha = ficha[9]
                    usuario_bx = ficha[10]
                    plano = ficha[11]
                    secao = ficha[12]
                    modelo=ficha[13]
                    qtd_ficha= ficha[14]        
                    vlrunit = ficha[15]
                    vlrtotal = ficha[16]
                    vlrmatprima = ficha[17]
                    vlrmatprima_1 = ficha[18]
                    vlrdep = ficha[19]
                    
                    
                    if quantidade_atribuida < qtd_nf:
                        quantidade_atribuir = min(qtd_ficha, qtd_nf - quantidade_atribuida)
                        fichas_atribuidas.append(( id_ficha, ano_ficha, fichas, data_env_ficha, hora_env_ficha, usuario_env, data_bx_ficha, horabx_ficha, usuario_bx, plano, secao, quantidade_atribuir, vlrunit, vlrtotal, vlrmatprima, vlrmatprima_1, vlrdep,modelo))
                        # print(fichas_atribuidas)
                        quantidade_atribuida += quantidade_atribuir
                if quantidade_atribuida == qtd_nf:
                    for ficha_atribuida in fichas_atribuidas:
                        row = [
                        ficha[1],
                        ficha[2],
                        ficha_atribuida[1],
                        ficha_atribuida[2],
                        ficha_atribuida[3],
                        ficha_atribuida[4],
                        ficha_atribuida[5],
                        ficha_atribuida[6],
                        ficha_atribuida[7],
                        ficha_atribuida[8],
                        ficha_atribuida[9],
                        ficha_atribuida[10],
                        ficha_atribuida[17],
                        ficha_atribuida[11],
                        ficha_atribuida[12],
                        ficha_atribuida[13],
                        ficha_atribuida[14],
                        ficha_atribuida[15],
                        ficha_atribuida[16],
                        nota[3],
                        nota[4],
                        nota[5],
                        nota[6],
                        nota[8],
                        nota[15],
                        nota[16],
                        nota[17],
                        nota[18],
                        nota[19],
                        nota[20],
                        nota[21],
                        nota[22],
                        nota[23],
                        nota[24],
                        nota[25],
                        nota[26],
                        nota[27],
                        nota[28],
                        nota[29],
                        nota[30],
                        nota[31],
                        nota[32],
                        nota[33],
                        nota[34],
                        nota[35],
                        nota[36],
                        
                        nota[14],
                        nota[37]
                        ]
                        accumulated_data.append(row)
                        for i in range(len(fichas_result)):
                            if fichas_result[i][4] == ficha_atribuida[4]:     
                                fichas_result[i] = (
                                    fichas_result[i][0],
                                    fichas_result[i][1],
                                    fichas_result[i][2],
                                    fichas_result[i][3],
                                    fichas_result[i][4],
                                    fichas_result[i][5],
                                    fichas_result[i][6],
                                    fichas_result[i][7],
                                    fichas_result[i][8],
                                    fichas_result[i][9],
                                    fichas_result[i][10],
                                    fichas_result[i][11],
                                    fichas_result[i][12],
                                    fichas_result[i][13],
                                    fichas_result[i][14] - ficha_atribuida[2],    
                                    fichas_result[i][15],
                                    fichas_result[i][16],
                                    fichas_result[i][17],
                                    fichas_result[i][18],
                                    fichas_result[i][19]
                                )
                                break
                            else:
                                valor_desejado = None

    

    
            return accumulated_data
        


@AutBolsa.route('/api/autBolsa', methods=['GET'])
def get_query_resultsFichas():
    app = current_app
    try:
        connection = connect_to_oracle(app)
        cursor = connection.cursor()

        data_inicial_str = request.args.get('data_inicial')
        data_final_str = request.args.get('data_final')
        data_inicial = datetime.strptime(data_inicial_str, '%d/%m/%Y')
        data_final = datetime.strptime(data_final_str, '%d/%m/%Y')

        accumulated_data = []
        header = [
                        'COD_PREST', 'PRESTADOR', 'ANO_FICHA', 'FICHAS', 'DATA_ENV_FICHA', 'HORA_ENV_FICHA',
                        'USUARIO_ENV', 'DATA_BX_FICHA', 'HORABX_FICHA', 'USUARIO_BX', 'PLANO', 'SECAO', 'MODELO',
                        'QTDE_FICHAS', 'VLRUNIT', 'VLRTOTAL', 'VLRMATPRIMA', 'RETENÇÃO', 'VLRDEP', 'NF', 'STATUS',
                        'DTENTREGA', 'ITEM', 'QTD_NF', 'CODPRO61', 'MOD4DIG', 'PEDFORN', 'VLRDESC', 'VLRACRES',
                        'VLRFRETE', 'VLROUTRAS', 'QTD2', 'UNIT', 'OP', 'CFOP', 'SEQ', 'DESC', 'ACRES', 'IPI', 'ICMS',
                        'DESCITEM', 'CCUSTO', 'GRUPODESP', 'CODDESP', 'VLRICMS', 'COMP', 'PAGTO', 'FECHAMENTO'
                    ]

        while data_inicial <= data_final:
            data_atual_formatada = data_inicial.strftime('%d/%m/%Y')
            notas_result, fichas_result = fetch_data_for_date(cursor, data_atual_formatada)
            accumulated_data.extend(process_data(notas_result, fichas_result))
            data_inicial += timedelta(days=1)
        
        results = [dict(zip(header, row)) for row in accumulated_data]

        response = jsonify(results)
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response

    except Exception as e:
        response = jsonify({'error': str(e)})
        response.status_code = 500
        return response
    
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()



@AutBolsa.route('/api/autBolsaNf', methods=['GET'])
def get_query_resultsFichasNf():
    app = current_app
    sql_queryFicha = """SELECT DISTINCT
                    TRIM(D.FI15NOME)      AS PRESTADOR, F.DTREF       AS DATA_REF, C.FT71CODNF  	AS NOTAS, TRIM(F.MODREF)        AS MOD_REF, (B.FT76QCOM )  AS QTDE
                    FROM FT76T A
                    INNER JOIN FT76T1 B ON A.FT76CHAVE = B.FT76CHAVE  
                    INNER JOIN FT71t C ON B.FT76CHAVE = C.FT71CHAVE
                    INNER JOIN FI15T D ON C.FT71CODCF = D.FI15CODCF   AND   C.FT71EMP05 = D.FI15EMP05 
                    FULL JOIN CO13T E ON SUBSTR(REPLACE(REPLACE(TRIM(B.FT76XPROD),' ',''),'.',''),-11,11)  = SUBSTR(REPLACE(TRIM(E.CO13DESCRI), '.', ''), 1, 11)  AND E.CO13BLOQ = 'N'
                    LEFT JOIN TB_NF_BOLSA F ON C.FT71CODCF = F.CODFOR AND C.FT71CODNF = F.NF AND B.FT76ITEM = F.ITEM
                    WHERE D.FI33CODSEG='99' AND B.FT76CFOP IN('5101','5102') 
                    AND F.DTREF >= to_date(:data_inicial, 'dd/mm/yyyy')  
                    AND F.DTREF <= to_date(:data_final, 'dd/mm/yyyy') 
                    and F.STATUSNF IN ('OK','COMP')
    """
    
    data_inicial = request.args.get('data_inicial')
    data_final = request.args.get('data_final')
   

    connection = connect_to_oracle(app)
    cursor = connection.cursor()

    try:

        # Cria o dicionário de parâmetros
        params = {'data_inicial': data_inicial, 'data_final':data_final}

        # Executa a consulta SQL com os parâmetros fornecidos
        cursor.execute(sql_queryFicha, params)

        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        response = jsonify(results)
        response.headers.add("Access-Control-Allow-Origin", "*")  
        return response
        

    finally:
        cursor.close()
        connection.close()