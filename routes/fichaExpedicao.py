from flask import Blueprint, jsonify, request, current_app
from database import connect_to_oracle

FichaExpedicao = Blueprint('fichaExpedicao', __name__)


@FichaExpedicao.route('/api/queryFicha', methods=['GET'])
def get_query_resultsFichas():
    app = current_app
    sql_queryFicha = """SELECT DISTINCT A.PC23EMP08 AS EMPRESA, A.PC23ANO AS ANO, A.PC23FICHA AS FICHA,(select distinct sum(pc23qtdtam) from pc23t1 a1 WHERE A1.PC23EMP08 = 61 AND  A1.PC23ANO=A.PC23ANO and A1.PC23FICHA =  A.PC23FICHA) qtde,
                    TRIM((SELECT CAST(LISTAGG(DISTINCT trim(PC75T1.PC75TAMANH) || '/' || trim(PC75T1.PC75QTDPAR)|| ' ' )as varchar2(200))  FROM PC75T1 PC75T1 WHERE   PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0))GRADE_CORRIDA,
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='25' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "25",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='26' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "26",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='27' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "27",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='28' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "28",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='29' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "29",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='30' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "30",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='31' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "31",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='32' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "32",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='33' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "33",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='34' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "34",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='35' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "35",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='36' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "36",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='37' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "37",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='38' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "38",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='39' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "39",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='40' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "40",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='41' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "41",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='42' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "42",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='43' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "43",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='44' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "44",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='46' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "46",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='PP' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "PP",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='P' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "P",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='M' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "M",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='G' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "G",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='GG' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "GG",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='XG' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "XG",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='XGG' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "XGG",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='XXG' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "XXG",
                    TRIM((SELECT DISTINCT trim(PC75T1.PC75QTDPAR)  FROM PC75T1 PC75T1 WHERE  pc75t1.PC75TAMANH='UN' and PC75T1.PC75CODIGO = a.pc23codgpd AND PC75T1.PC75QTDPAR > 0)) "UN",
                    a.PC23PLANO AS PLANO,a.PC23PLA72 AS PLANO_ORIGINAL, a.PC23MODELO AS MODELO, o.pc10descr as Cor, C.PC13CODCOL AS COD_COL, H.PCAIDESC AS COLECAO, C.PC13CODLAN AS COD_LAN, I.PCBMDESCR AS LANCAMENTO, C.PC13CODCTG AS COD_CATEGORIA, G.PCBJDESCR AS  CATEGORIA, C.PC13CLAITE AS COD_CLASS_ITEM, J.PC16DESCR AS CLAS_ITEM, C.PC13CLASS AS COD_CLASSIFICACAO, M.PC04DESCR  AS CLASSIFICACAO, C.PC13TIPPRO AS TIPO_PRODUTO, a.PC23CODFAB AS FABRICA, K.PC45DESCR AS NOME_FABRICA, C.PC13FORMA AS FORMA, N.PC02DESCR AS DESC_FORMA, C.PC13CODCGE AS COD_GESTOR, O.PCBKDESCR AS DESC_GESTOR,  a.pc23codgpd AS grade,b.PC23ENVIO AS DATA_ENTRADA, b.PC23HORENV AS HORA_ENTRADA, b.PC23USUENT AS USUARIO_ENTRADA, b.PC23DATA AS DATA_SAIDA, b.PC23HORDAT AS HORA_SAIDA, b.PC23USUARI AS USUARIO_SAIDA ,  t1.PC17ANO AnoPedido, t1.pc17pedido Pedido, t2.pc17seq Item_Ped ,t3.PCFYSTATUS StatusPed, t4.PCFWCODIGO as embarque,t5.PC17CODCLI 	AS Codigo_Cliente,t6.FI15NOME 		AS DESC_CLI       FROM PC23T A left JOIN PC23Ta  B ON A.PC23EMP08 = B.PC23EMP08 AND A.PC23ANO = B.PC23ANO AND A.PC23FICHA = B.PC23FICHA AND b.PC23C_CUST = 3900 INNER JOIN PC13T  C ON B.PC23EMP08 = C.PC13EMP08 AND a.PC23MODELO = C.PC13CODIGO AND a.PC23COR = C.PC13COR AND C.PC13ANOPED = 0 INNER JOIN PC10T o ON a.PC23COR = o.pc10codigo INNER JOIN PC23T1 D ON A.PC23EMP08 = D.PC23EMP08 AND A.PC23ANO = D.PC23ANO AND A.PC23FICHA = D.PC23FICHA  LEFT  JOIN PCBJT  G ON C.PC13EMP08 = G.PCBJCODEMP AND C.PC13CODCTG = G.PCBJCODIGO  LEFT  JOIN PCAIT  H ON C.PC13EMP08 = H.PCAICODEMP AND C.PC13CODCOL = H.PCAICODIGO  LEFT  JOIN PCBMT  I ON C.PC13EMP08 = I.PCBMCODEMP AND C.PC13CODLAN = I.PCBMCODIGO  LEFT  JOIN PC16T  J ON C.PC13CLAITE = J.PC16CODIGO  INNER  JOIN PC45T K ON a.PC23CODFAB = K.PC45CODIGO  INNER  JOIN CF01T L ON K.PC45EMPRFA = L.CF01CODEMP  INNER  JOIN PC04T M ON M.PC04CODIGO = C.PC13CLASS  INNER JOIN PC02T N ON C.PC13EMPFOR = N.PC02CODEMP  AND C.PC13FORMA = N.PC02CODIGO INNER JOIN PCBKT O ON C.PC13CODCGE = O.PCBKCODIGO left join pc17tb t1 on a.pc23ano = t1.pc17anofpa and a.pc23ficha= t1.pc17numfpa left JOIN PC17T312 t2 ON t1.pc17pedido = t2.pc17pedido AND t1.pc17emp08 = t2.pc17emp08f AND t1.pc17anofpa = t2.pc17anofic and t1.PC17ANO= t2.pc17ano AND t1.pc17numfpa = t2.pc17ficha left JOIN PCFYTA t3 ON  a.PC23FICHA = t3.PCFYFICHA and a.pc23ano = t3.PCFYANO left JOIN PCFWTA t4 ON t2.PC17SEQ = t4.PCFWSEQ AND t1.PC17PEDIDO = t4.PCFWPEDIDO  and t2.pc17ano = t4.PCFWANOPED left join pc17t t5 on  t1.pc17pedido = t5.pc17pedido AND t1.pc17emp08 = t5.pc17emp08 and t1.PC17ANO= t5.pc17ano LEFT JOIN FI15T t6 ON t5.PC17EMP08   = t6.FI15EMP05 AND t5.PC17FLACLI 	= t6.FI15FLAGCF AND t5.PC17CODCLI = t6.FI15CODCF  WHERE A.PC23EMP08 = 61 AND A.PC23ANO = :ANO AND A.PC23FICHA IN ({ficha})
    """
    
    
    ano = request.args.get('ano')
    ficha = request.args.get('ficha')
    ficha_list = ficha.split(',')

    connection = connect_to_oracle(app)
    cursor = connection.cursor()

    try:
        # Monta a consulta SQL completa
        full_sql_query = sql_queryFicha.format(ficha=','.join(ficha_list))

        # Cria o dicionário de parâmetros
        params = {'ANO': ano}

        # Executa a consulta SQL com os parâmetros fornecidos
        cursor.execute(full_sql_query, params)

        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        response = jsonify(results)
        response.headers.add("Access-Control-Allow-Origin", "*")  
        return response
        

    finally:
        cursor.close()
        connection.close()