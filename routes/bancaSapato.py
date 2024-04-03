from flask import Blueprint, current_app, jsonify, request, send_file
from database import connect_to_oracle

from flask import Flask, jsonify, request
from datetime import datetime, timedelta
BancaSapato = Blueprint('bancaSapato', __name__)

@BancaSapato.route('/api/sapato', methods=['GET'])
def banca_sapato():
    try:
        app = current_app
        data_inicial_str = request.args.get('data_inicial')
        data_final_str = request.args.get('data_final')
        data_inicial = datetime.strptime(data_inicial_str, '%d/%m/%Y')
        data_final = datetime.strptime(data_final_str, '%d/%m/%Y')
        connection = connect_to_oracle(app)
        
        cursor = connection.cursor()
        
        dados_para_front = []
        
        data_atual = data_inicial
        while data_atual <= data_final:
            print(data_atual)
            
            query_notas = """
            SELECT DISTINCT A.FT07CODIGO as nota, A.FT07PQTD as notas_QTDE, B.FT07DTEMI||'-'||trim(B.FI15CODCF)||'-'||TRIM(CASE  WHEN A.CO13CODPRO=3096439   THEN 3059 WHEN A.CO13CODPRO=2550659   THEN 3076 WHEN A.CO13CODPRO=89072   THEN 3070 WHEN A.CO13CODPRO=3219112   THEN TO_NUMBER('20'||TO_CHAR(SYSDATE,'MM')) else 9999 end) as id_nota,B.FT07DTEMI AS DT_EMI,trim(B.FI15CODCF) AS FORNECEDOR,CASE WHEN A.CO13CODPRO=2550659   THEN 3076 WHEN A.CO13CODPRO=89072   THEN 3070 WHEN A.CO13CODPRO=3219112   THEN TO_NUMBER('20'||TO_CHAR(SYSDATE,'MM')) else 9999 end as secao_nota,A.CO13CODPRO AS cod_pro,trim(c.FI15NOME) as desc_for,a.FT07PDESCR as desc_cod_pro,A.FT07PVRUNI AS vr_unit,ADD_MONTHS(TRUNC(b.FT07DTEMI,'MM'),-1) AS dt_envio_simples,ADD_MONTHS(LAST_DAY(b.FT07DTDIG),-1) AS dt_retorno_simples    
            FROM FT07T1 A INNER JOIN FT07T B ON B.FT07CODEMP = A.FT07CODEMP AND B.FI16MODELO = A.FI16MODELO AND B.FT07CODIGO = A.FT07CODIGO inner join fi15t c on b.fi15emp05 = c.fi15emp05 and b.fi15flagcf = c.fi15flagcf and b.fi15codcf = c.fi15codcf
            WHERE A.FT07CODEMP = 61 and a.fi16modelo='CM' AND A.CO13CODPRO in ('89072','3219112','2550659') AND C.FI33CODSEG IN ('99') AND A.FT07CODIGO NOT IN(315566,315563,320394)
            AND B.FT07DTEMI >= TO_DATE(:data_inicial, 'DD/MM/YYYY') AND B.FT07DTEMI <= TO_DATE(:data_final, 'DD/MM/YYYY') 
        -- and trim(B.FI15CODCF) not in(5000415)
            ORDER BY 5,4,6, 1"""
            cursor.execute(query_notas, data_inicial=data_atual.strftime('%d/%m/%Y'), data_final=data_atual.strftime('%d/%m/%Y'))

            notas_result = cursor.fetchall()

            query_fichas = """ 
            SELECT a.PC23ENVIO||'-'||trim(A.PC23CODCF)||'-'||TRIM(A.PC23C_CUST) AS ID_FICHA, A.PC23FICHA as ficha, (SELECT DISTINCT SUM(PC23QTDTAM) FROM PC23T1 A1 WHERE A1.PC23EMP08 = 61 AND A1.PC23ANO = A.PC23ANO AND A1.PC23FICHA = A.PC23FICHA) as FICHA_QTDE, a.PC23ENVIO as data_envio, a.PC23HORENV as hora_envio, trim(T.FI15NOME) AS PRESTADORES, CASE WHEN A.PC23DATA >'31/01/1900' THEN A.PC23DATA ELSE NULL END AS RETORNO, D.PC23PLANO AS PLANO,D.PC23MODELO AS MODELO, A.PC23C_CUST  AS SECAO, a.pc23ano as ANO_FICHA, trim(A.PC23CODCF) as COD_FOR, trim(A.PC23CODCF)||'-'||a.pc23ano||'-'||a.pc23ficha||'-'||TRIM(A.PC23C_CUST) as id_secundario, T1.PC48PRECO    AS vlr_unit_ficha, ((SELECT DISTINCT SUM(PC23QTDTAM) FROM PC23T1 A1 WHERE A1.PC23EMP08 = 61 AND A1.PC23ANO = A.PC23ANO AND A1.PC23FICHA = A.PC23FICHA)* T1.PC48PRECO) AS vlr_total_ficha,A.PC23USUENT AS USUARIO_ENVIO, A.PC23USUARI AS USUARIO_SAIDA
            FROM PC23TA A INNER JOIN PC23T D  ON D.PC23EMP08  = A.PC23EMP08  AND D.PC23ANO    = A.PC23ANO  AND D.PC23FICHA  = A.PC23FICHA INNER JOIN FI15T T ON T.FI15EMP05 = A.PC23EMP05 AND T.FI15FLAGCF = A.PC23FLAGCF AND T.FI15CODCF = A.PC23CODCF FULL OUTER JOIN PC48T1 T1 ON D.PC23EMP08  = T1.PC48CODEMP AND D.PC23MODELO = T1.PC48MODELO AND D.PC23EMP08  = T1.PC48CODEMP AND A.PC23EMP01  = T1.PC48EMP01S AND A.PC23C_CUST = T1.PC48CODSEC AND A.PC23EMP08  = T1.PC48EMPIND AND A.PC23PRECO  = T1.PC48CODIND
            WHERE A.PC23EMP08 IN ('61') AND A.PC23C_CUST in(3070,3076)  
            AND A.PC23ENVIO >= TO_DATE(:data_inicial, 'DD/MM/YYYY') AND A.PC23ENVIO <= TO_DATE(:data_final, 'DD/MM/YYYY') AND T.FI33CODSEG IN ('99') 
            --and trim(A.PC23CODCF) in(5000415)
            ORDER BY trim(A.PC23CODCF),a.PC23ENVIO,a.pc23c_cust, a.PC23HORENV

            """
            cursor.execute(query_fichas, data_inicial=data_atual.strftime('%d/%m/%Y'), data_final=data_atual.strftime('%d/%m/%Y'))
            fichas_result = cursor.fetchall()


                
            for nota in notas_result:
                nota_id = nota[0]
                nota_quantidade = nota[1]
                ID_nota = nota[2]
                cod_pro = nota[6]
                secao_nota=nota[5]
                desc_for=nota[7]
                cod_for=nota[4]
                fichas_disponiveis = []    
                for ficha in fichas_result:
                    ID_ficha = ficha[0]
                    ficha_numero = ficha[1]
                    ficha_quantidade = ficha[2]
                    if ID_ficha == ID_nota and cod_pro != 3219112 and ficha_quantidade > 0 :
                        fichas_disponiveis.append(ficha)
                quantidade_atribuida = 0
                fichas_atribuidas = []
                
                
                
                for ficha in fichas_disponiveis:
                    ID_ficha=ficha[0]
                    ficha_numero = ficha[1]
                    ficha_quantidade = ficha[2]
                    data_envio = ficha[3]
                    hora_envio = ficha[4]
                    PRESTADORES= ficha[5]
                    RETORNO= ficha[6]
                    PLANO= ficha[7]
                    MODELO= ficha[8]
                    SECAO= ficha[9]
                    ANO_FICHA= ficha[10]
                    COD_FOR_ficha = ficha[11]
                    id_secundario= ficha[12]
                    vlr_unit_ficha = ficha[13]
                    vlr_total_ficha = ficha[14]
                    usuario_entrada =ficha[15]
                    usuario_saida = ficha [16]
                    if quantidade_atribuida < nota_quantidade:
                        quantidade_atribuir = min(ficha_quantidade, nota_quantidade - quantidade_atribuida)
                        fichas_atribuidas.append(( ID_ficha,ficha_numero, quantidade_atribuir, data_envio, hora_envio,PRESTADORES, RETORNO, PLANO, MODELO, SECAO,ANO_FICHA,COD_FOR_ficha,id_secundario,vlr_unit_ficha,vlr_total_ficha,usuario_entrada,usuario_saida))
                        quantidade_atribuida += quantidade_atribuir
                    
                    
                    
                if quantidade_atribuida == nota_quantidade:
                    for ficha_atribuida in fichas_atribuidas:
                        dados_para_front.append({
                            'PRESTADORES': ficha_atribuida[5],
                            'COD_FOR': ficha_atribuida[11],
                            'Data de Envio': ficha_atribuida[3],
                            'Hora de Envio': ficha_atribuida[4],
                            'RETORNO': ficha_atribuida[6],
                            'Ficha': ficha_atribuida[1],
                            'Quantidade_Ficha': ficha_atribuida[2],
                            'PLANO': ficha_atribuida[7],
                            'MODELO': ficha_atribuida[8],
                            'VR_UNIT_FICHA': ficha_atribuida[13],
                            'VR_TOTAL_FICHA': ficha_atribuida[14],
                            'RETORNO - ATUALIZADO': '',
                            'VR_UNIT_FICHA - ATUALIZADO': '',
                            'VR_TOTAL_FICHA - ATUALIZADO': '',
                            'SEÇÃO': ficha_atribuida[9],
                            'ANO_FICHA': ficha_atribuida[10],
                            'DATA_EMISSAO_NOTA': nota[3],
                            'NOTA': nota_id,
                            'Quantidade_Nota': nota_quantidade,
                            'COD_PRO': cod_pro,
                            'DESC_COD_PRO': nota[8],
                            'VR_UNIT_NF': nota[9],
                            'VR_TOTAL_NF': nota[9] * ficha_atribuida[2],
                            'VR_NOTA': '',
                            'ID_SECUNDARIO': ficha_atribuida[12],
                            'PGTO': '',
                            'FECHAMENTO': '',
                            'Erro': '',
                            'USUARIO_ENVIO': usuario_entrada,
                            'USUARIO_SAIDA': usuario_saida,

                        })
                        for i in range(len(fichas_result)):
                            if fichas_result[i][1] == ficha_atribuida[1]:
                                fichas_result[i] = (
                                    fichas_result[i][11],
                                    fichas_result[i][1],
                                    fichas_result[i][2] - ficha_atribuida[2],
                                    fichas_result[i][3],
                                    fichas_result[i][4])                    
                                break
                
                
                elif cod_pro != 3219112 :
                    dados_para_front.append({
                        'PRESTADORES': desc_for,
                        'COD_FOR': cod_for,
                        'Data de Envio': '',
                        'Hora de Envio': '',
                        'RETORNO': '',
                        'Ficha': '',
                        'Quantidade_Ficha': nota_quantidade,
                        'PLANO': '',
                        'MODELO': '',
                        'VR_UNIT_FICHA': '',
                        'VR_TOTAL_FICHA': '',
                        'RETORNO - ATUALIZADO': '',
                        'VR_UNIT_FICHA - ATUALIZADO': '',
                        'VR_TOTAL_FICHA - ATUALIZADO': '',
                        'SEÇÃO': secao_nota,
                        'ANO_FICHA': '',
                        'DATA_EMISSAO_NOTA': nota[3],
                        'NOTA': nota_id,
                        'Quantidade_Nota': nota_quantidade,
                        'COD_PRO': cod_pro,
                        'DESC_COD_PRO': nota[8],
                        'VR_UNIT_NF': nota[9],
                        'VR_TOTAL_NF': nota[9] * nota_quantidade,
                        'VR_NOTA': '',
                        'ID_SECUNDARIO': '',
                        'PGTO': '',
                        'FECHAMENTO': '',
                        'Erro': 'NF sem ficha: Cancelar NF',
                        'USUARIO_ENVIO': '',
                        'USUARIO_SAIDA': '',
                    })   
                else :
                    dados_para_front.append({
                        'PRESTADORES': desc_for,
                        'COD_FOR': cod_for,
                        'Data de Envio': nota[10],
                        'Hora de Envio': '',
                        'RETORNO': nota[11],
                        'Ficha': '',
                        'Quantidade_Ficha': nota_quantidade,
                        'PLANO': '',
                        'MODELO': '',
                        'VR_UNIT_FICHA': '',
                        'VR_TOTAL_FICHA': '',
                        'RETORNO - ATUALIZADO': nota[11],
                        'VR_UNIT_FICHA - ATUALIZADO': '',
                        'VR_TOTAL_FICHA - ATUALIZADO': '',
                        'SEÇÃO': secao_nota,
                        'ANO_FICHA': '',
                        'DATA_EMISSAO_NOTA': nota[3],
                        'NOTA': nota_id,
                        'Quantidade_Nota': nota_quantidade,
                        'COD_PRO': cod_pro,
                        'DESC_COD_PRO': nota[8],
                        'VR_UNIT_NF': nota[9],
                        'VR_TOTAL_NF': nota[9] * nota_quantidade,
                        'VR_NOTA': '',
                        'ID_SECUNDARIO': '',
                        'PGTO': '',
                        'FECHAMENTO': '',
                        'Erro': 'NF Simples',
                        'USUARIO_ENVIO': '',
                        'USUARIO_SAIDA': '',
                    })

            for ficha in fichas_result:
                ID_ficha = ficha[0]
                ficha_numero = ficha[1]
                ficha_quantidade = ficha[2]
                if ficha_quantidade > 0:
                    dados_para_front.append({
                        'PRESTADORES': ficha[5],
                        'COD_FOR': ficha[11],
                        'Data de Envio': ficha[3],
                        'Hora de Envio': ficha[4],
                        'RETORNO': ficha[6],
                        'Ficha': ficha_numero,
                        'Quantidade_Ficha': ficha_quantidade,
                        'PLANO': ficha[7],
                        'MODELO': ficha[8],
                        'VR_UNIT_FICHA': ficha[13],
                        'VR_TOTAL_FICHA': ficha[14],
                        'RETORNO - ATUALIZADO': '',
                        'VR_UNIT_FICHA - ATUALIZADO': '',
                        'VR_TOTAL_FICHA - ATUALIZADO': '',
                        'SEÇÃO': ficha[9],
                        'ANO_FICHA': ficha[10],
                        'DATA_EMISSAO_NOTA': '',
                        'NOTA': '',
                        'Quantidade_Nota': '',
                        'COD_PRO': '',
                        'DESC_COD_PRO': '',
                        'VR_UNIT_NF': '',
                        'VR_TOTAL_NF': '',
                        'VR_NOTA': '',
                        'ID_SECUNDARIO': ficha[12],
                        'PGTO': '',
                        'FECHAMENTO': '',
                        'Erro':'Ficha sem NF: Reenviar fichas hoje e emitir NF',
                        'USUARIO_ENVIO': ficha[15],
                        'USUARIO_SAIDA': ficha[16],
                    })

            
            data_atual += timedelta(days=1)
        print("Process finished successfully!")
        df = pd.DataFrame(dados_para_front)

        # Exibir o DataFrame
        print(df)
        cursor.close()
        connection.close()
        return jsonify({"data": dados_para_front, "success": True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})