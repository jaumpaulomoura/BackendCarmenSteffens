
import cx_Oracle
from flask import Blueprint, current_app, jsonify, request

from database import connect_to_oracle
GrupoPecista = Blueprint('grupoPecista', __name__)

@GrupoPecista.route('/api/grupoPecista/colaborador', methods=['POST'])
def consultar_colaborador():
    app = current_app
    connection = connect_to_oracle(app)
    cursor = connection.cursor()
    try:
        empresa = request.json.get('empresa')  # Ajuste aqui para obter dados do corpo da solicitação JSON

        # print("Valor de empresa recebido:", empresa)

        
        SQL = """
        SELECT distinct
            b.PE01CODEMP EMP,
            CASE
                WHEN b.PE01CODEMP = 31 THEN 'ROSANGELA'
                WHEN b.PE01CODEMP = 32 THEN 'ALINE'
                WHEN b.PE01CODEMP = 54 THEN 'WELLINGTON'
                WHEN b.PE01CODEMP = 61 AND b.FP03DEPTO = '920.224.001' THEN 'BRUNO'
                WHEN b.PE01CODEMP = 61 AND B.FP03DEPTO <> '920.224.001' THEN 'WALTER'
                ELSE 'VERIFICAR'
            END ENCARREGADO,
            b.FP02MATRIC MATRICULA,
            B.FP02COD COD_COL,
            B.FP03DEPTO DEPTO,
            B.FP02LOCTRA DESC_DEPTO,
            B.FP02NOM COLABORADOR,
            A.FP74DTRESC DT_RESCISAO,
            CASE 
                WHEN B.FP10COD IN (3,134,136,722,763) THEN 'P'
                WHEN B.FP10COD IN (2,6,7,20,123,226,755) THEN 'C'
                WHEN B.FP10COD IN (28,224,250,639) THEN 'OP'
                ELSE 'VERIFICAR'
            END FUNCAO,
            B.FP02SITU SITUACAO
        FROM FP02T3 b
        LEFT JOIN FP74T A ON B.pe01CodEmp=A.pe01CodEmp AND B.fp02Cod=A.fp02Cod
        WHERE B.FP02OBS LIKE '%PXY%'
            AND B.FP02SITU='A'
            AND b.PE01CODEMP=:empresa
            OR B.FP02OBS LIKE '%PXY%'
            AND B.FP02SITU='D'
            AND A.FP74DTRESC>=SYSDATE-30
            AND b.PE01CODEMP=:empresa
        """

        cursor.execute(SQL, {'empresa': empresa})

        columns = [col[0] for col in cursor.description]
        resultsEmp = [dict(zip(columns, row)) for row in cursor.fetchall()]
        response = jsonify(resultsEmp)
        response.headers.add("Access-Control-Allow-Origin", "*")
        
        return response

    except Exception as e:
        print(f"Erro na consulta SQL: {e}")
        # Trate o erro como apropriado
        return jsonify({'error': 'Erro na consulta SQL'}), 500
    finally:
        cursor.close()
        connection.close()

        
@GrupoPecista.route('/api/grupoPecista/grupo', methods=['GET'])
def consultar_grupo():
    app = current_app   
    connection = connect_to_oracle(app)
    cursor = connection.cursor()

    SQL="""select DISTINCT PC47CODIGO as CodCortador, PC47DESCR as Cortador from pc47t
            where PC47CODEMP=61
            and PC47CODIGO >= '1000'
            and PC47CODSEC  in(3652,3467,3065)
            order by 2
        """
    try:
      
        cursor.execute(SQL)

        columns = [col[0] for col in cursor.description]
        resultsEmp = [dict(zip(columns, row)) for row in cursor.fetchall()]
        response = jsonify(resultsEmp)
        response.headers.add("Access-Control-Allow-Origin", "*")  
        return response
    finally:
        cursor.close()
        
@GrupoPecista.route('/api/grupoPecista/prod', methods=['GET'])
def consultar_prod():
    app = current_app
    connection = connect_to_oracle(app)
    cursor = connection.cursor()
    
    try:
        data = request.args.get('data')  # Ajuste aqui para obter dados do corpo da solicitação JSON
        grupo = request.args.get('grupo')  # Ajuste aqui para obter dados do corpo da solicitação JSON

        # print(f"Parâmetros recebidos - data: {data}, grupo: {grupo}")
        # print(f"Parâmetros recebidos - data: {data}, grupo: {grupo}")
        SQL="""
            select 
            a.pc23emp08      	    	            as empresa, 
            a.pc23ano                               as AnoFicha,    
            a.pc23ficha                             as Ficha,
            extract(day   from d.pc23data)          as DiaRet,
            extract(month from d.pc23data)          as MesRet,
            extract(year  from d.pc23data)          as AnoRet,
            extract(month from D.PC23DATA) || '/' ||
            extract(year  from d.pc23data)          as MesAno,
            d.pc23data                              as DtRetorno,
            last_day (d.pc23data )                  as Comp,
            cast(d.pc23hordat as varchar2(8))       as HoraRet,
            d.pc23tipaux                            as TipoFicha,
            sum(b.pc23qtdtam) 	                    as Qtd,
            a.pc23modelo 		                    as Modelo,
            t1.pc48codind                           as Indice,
            t1.pc48preco                            as VlrUnit,
            (sum(b.pc23qtdtam) * t1.pc48preco)      as VlrTotal,
            (sum(b.pc23qtdtam) * t1.pc48preco)*0.3 as vlr30,
            (sum(b.pc23qtdtam) * t1.pc48preco)*0.4 as vlr40,
            d.pc23c_cust                            AS Custo,
            m.ct07nomecc                            as DescrSecao,
            d.pc23codcor                            as CodCortador,
            nvl(l.pc47descr, 'nd')                  as Cortador

    from pc23t a
    inner join pc23t1 b 
            on a.pc23emp08         =   b.pc23emp08  
            and a.pc23ano            =   b.pc23ano   
            and a.pc23ficha         =   b.pc23ficha 

    inner join pc23ta d 
            on a.pc23emp08         =   d.pc23emp08  
            and a.pc23ano            =   d.pc23ano    
            and a.pc23ficha         =   d.pc23ficha
    inner join ct07t m
            on m.ct07emp01         =   d.pc23emp01
            and m.ct07c_cust        =   d.pc23c_cust
    full outer join pc47t l
                on l.pc47emp01s  =   m.ct07emp01
                --and l.pc47codsec  =   m.ct07c_cust 
                and l.pc47codemp  =   d.pc23empcor 
                and l.pc47codigo  =   d.pc23codcor

    full outer join pc48t1 t1
                on a.pc23emp08   =   t1.pc48codemp
                and a.pc23modelo  =   t1.pc48modelo

            and d.pc23emp08        =   t1.pc48codemp
            and d.pc23emp01        =   t1.pc48emp01s
            and d.pc23c_cust       =   t1.pc48codsec
            and d.pc23emp08        =   t1.pc48empind
            and d.pc23preco        =   t1.pc48codind
    where a.pc23emp08 in('61')
    and PC47CODIGO >= '1000'
      and d.pc23data = to_date(:data, 'DD/MM/YYYY')
and d.pc23codcor = :grupo

    group by a.pc23emp08, a.pc23ano, a.pc23ficha, d.pc23data, d.pc23hordat, 
    d.pc23tipaux, a.pc23modelo, t1.pc48codind, t1.pc48preco, d.pc23preco, 
    d.pc23c_cust, m.ct07nomecc, l.pc47descr, d.pc23codcor 
    order by 8,9
            """
        cursor.execute(SQL, {'data': data, 'grupo': grupo})

        columns = [col[0] for col in cursor.description]
        resultsEmp = [dict(zip(columns, row)) for row in cursor.fetchall()]
        response = jsonify(resultsEmp)
        response.headers.add("Access-Control-Allow-Origin", "*")  
        # print(resultsEmp)
        return response        

    except Exception as e:
        print(f"Erro na consulta SQL: {e}")
        # Trate o erro como apropriado
        return jsonify({'error': 'Erro na consulta SQL'}), 500
    finally:
        cursor.close()
        connection.close()








@GrupoPecista.route('/api/grupoPecista/ponto', methods=['GET'])
def consultar_ponto():
    app = current_app
    connection = connect_to_oracle(app)
    cursor = connection.cursor()
    
    try:
        empresa = request.args.get('empresa')
        dataPontoIni = request.args.get('dataPontoIni')
        dataPontoFinal = request.args.get('dataPontoFinal')
        codCol = request.args.get('codCol')
        # print(f"Parâmetros recebidos - dataPonto: {dataPonto}, codCol: {codCol}")
        SQL_PONTO=""" SELECT 
                A.PE01CODEMP EMP,
                CASE
                WHEN A.PE01CODEMP = 31 THEN 'ROSANGELA'
                WHEN A.PE01CODEMP = 32 THEN 'ALINE'
                WHEN A.PE01CODEMP = 54 THEN 'WELLINGTON'
                WHEN A.PE01CODEMP = 61 AND B.FP03DEPTO = '920.224.001' THEN 'BRUNO'
                WHEN A.PE01CODEMP = 61 AND B.FP03DEPTO <> '920.224.001' THEN 'WALTER'
                ELSE 'VERIFICAR'
                END ENCARREGADO,
                A.PE12MATRIC MATRICULA,
                B.FP02COD COD_COL,
                B.FP03DEPTO DEPTO,
                B.FP02LOCTRA DESC_DEPTO,
                B.FP02NOM COLABORADOR,
                A.PE10COD ,
                LAST_DAY(A.PE10DIA) COMP,
                A.PE10DIA DATA,
                TO_CHAR(A.PE10DIA,'DAY') DIA_SEMANA,
                A.PE10TIPDIA,
                A.PE10TIPPOR,
                A.PE09COD OC,
                C.PE09NOME DESC_OC,
                ROUND(A.PE10CAJODI/3600,2) HORA_DIA,
                ROUND(A.PE10SOCAMA/3600,2) HORA_TRAB,
                A.PE10SOCAMA-A.PE10CAJODI teste,
                CASE
                WHEN   (A.PE10SOCAMA-A.PE10CAJODI)<= 600  and (A.PE10SOCAMA-A.PE10CAJODI)>= -600 THEN ROUND(A.PE10CAJODI/3600,2)

                ELSE ROUND(A.PE10SOCAMA/3600,2)
                END HORA_TRAB_MEDIA,
                /*CASE
                WHEN A.PE10QTDHED = 0 AND (A.PE10SOCAMA-A.PE10CAJODI)<= 600 AND A.PE10HOROCO = 0 AND A.PE10DESREF = 0 THEN ROUND(A.PE10CAJODI/3600,2)
                WHEN A.PE10QTDHEN = 0 AND (A.PE10SOCAMA-A.PE10CAJODI)<= 600 AND A.PE10HOROCO = 0 AND A.PE10DESREF = 0 THEN ROUND(A.PE10CAJODI/3600,2)
                WHEN A.PE10CAJODI = 0 AND A.PE10SOCAMA > 0 THEN ROUND(A.PE10CAJODI/3600,2)
                ELSE ROUND(A.PE10SOCAMA/3600,2)
                END HORA_TRAB_AJ,*/

                CASE
                WHEN (A.PE10SOCAMA-A.PE10CAJODI)<= 600  and (A.PE10SOCAMA-A.PE10CAJODI)>= -600 THEN ROUND(A.PE10CAJODI/3600,2)
                ELSE ROUND(A.PE10SOCAMA/3600,2)
                END HORA_TRAB_AJ,


                --ROUND((A.PE10SOCAMA-A.PE10QTDHED-A.PE10QTDHEN-A.PE10DESREF)/3600,2) TST,


                ROUND(A.PE10QTDHED/3600,2) HE_DIURNO,
                ROUND(A.PE10QTDHEN/3600,2) HE_NOTURNO,
                --ROUND(A.PE10HOROCO/3600,2) HORA_OC,
                --ROUND(A.PE10DESREF/3600,2) HORA_DESC,


                CASE WHEN A.PE09COD  IN(3,	4, 6, 7, 8,	9,	10,	13,	15,	16,	17,	18,	20,	22,	24,	25,	26,	34,	35,	36,	38,	46,	47,	48,	56,	57,	58,	59) THEN ROUND(A.PE10HOROCO/3600,2)
                ELSE ROUND(A.PE10DESREF/3600,2) END HR_DESC,


                ROUND(A.PE10HORDIA/3600,2) HORA_DSR,
                A.PE10DIASEM ,
                A.PE10MOVBO MOV_BH,
                A.PE10CODJOR COD_JORN,
                A.PE10AFASTA AFAST,
                A.PE10ENT1 ENT1,
                A.PE10SAI1 SAI1, 
                A.PE10ENT2 ENT2,
                A.PE10SAI2 SAI2,
                A.PE10ENT3 ENT3,
                A.PE10SAI3 SAI3,
                A.PE10ENT4 ENT4, 
                A.PE10SAI4 SAI4,
                CASE 
                WHEN B.FP10COD IN (3,134,136,722,763) THEN 'P'
                WHEN B.FP10COD IN (2,6,7,20,123,226,755) THEN 'C'
                WHEN B.FP10COD IN (28,224,250,639) THEN 'OP'
                ELSE 'VERIFICAR' END FUNCAO 


                FROM PE10T1 A

                INNER JOIN FP02T3 B
                ON A.PE01CODEMP = B.PE01CODEMP
                AND A.PE12MATRIC = B.FP02MATRIC

                LEFT JOIN PE09T C
                ON A.PE01CODEMP = C.PE01CODEMP
                AND A.PE09COD = C.PE09COD

                WHERE B.FP02OBS LIKE '%PXY%' 
                and A.PE01CODEMP = :empresa
                and A.PE10DIA>= to_date(:dataPontoIni, 'DD/MM/YYYY') 
                and A.PE10DIA<= to_date(:dataPontoFinal, 'DD/MM/YYYY') 
                and B.FP02COD=:codCol
                --ORDER BY 8,1,6,9
            """
        cursor.execute(SQL_PONTO, {'empresa':empresa,'dataPontoIni': dataPontoIni, 'dataPontoFinal': dataPontoFinal, 'codCol': codCol})

        columns = [col[0] for col in cursor.description]
        resultsPonto = [dict(zip(columns, row)) for row in cursor.fetchall()]
        response = jsonify(resultsPonto)
        response.headers.add("Access-Control-Allow-Origin", "*")  
        # print(resultsPonto)
        return response        

    except Exception as e:
        print(f"Erro na consulta SQL: {e}")
        # Trate o erro como apropriado
        return jsonify({'error': 'Erro na consulta SQL'}), 500
    finally:
        cursor.close()
        connection.close()
        
        
   
   
@GrupoPecista.route('/api/insereDadosTab', methods=['POST'])
def insereDadosTab():
    dataPec = request.get_json()
    print("Dados recebidos:", dataPec)  # Print received data for debugging
    inserir_dados(dataPec)
    return 'Inserção de dados concluída com sucesso'
        

def inserir_dados(lista_de_dados):
    app = current_app
    conexao = connect_to_oracle(app)
    cursor = conexao.cursor()

    try:
        for data in lista_de_dados:
            cursor.execute('''
                INSERT INTO PRODUCAO.TB_PECISTA (PECEMP, PECCODCOL, PECGRUPO, PECDATA, PECPRODGRU, PECPRODIND, PECHRPAR)
                VALUES (
                    :PECEMP,
                    :PECCODCOL,
                    :PECGRUPO,
                    to_date(:PECDATA, 'dd/mm/yyyy'),
                    :PECPRODGRU,
                    :PECPRODIND,
                    :PECHRPAR
                )
            ''', data)

        conexao.commit()
        print("Inserção de dados concluída com sucesso.")
    except cx_Oracle.Error as error:
        print("Erro durante a inserção:", error)
    finally:
        cursor.close()
        conexao.close()




@GrupoPecista.route('/api/grupoPecista/consulPec', methods=['GET'])
def consultar_Pecista():
    app = current_app
    connection = connect_to_oracle(app)
    cursor = connection.cursor()
    
    try:
        dt_ini = request.args.get('dt_ini')
        dt_final = request.args.get('dt_final')
        print(f"Parâmetros recebidos - dt_ini: {dt_ini}, dt_final: {dt_final}")
        SQL_Dados=""" SELECT a.PECEMP, a.PECCODCOL, CASE 
                WHEN B.FP10COD IN (3,134,136,722,763) THEN 'P'
                WHEN B.FP10COD IN (2,6,7,20,123,226,755) THEN 'C'
                WHEN B.FP10COD IN (28,224,250,639) THEN 'OP'
                ELSE 'VERIFICAR'
            END FUNCAO,B.FP02NOM, a.PECGRUPO, to_date(a.PECDATA,'dd/mm/yyyy')PECDATA, a.PECPRODGRU, a.PECPRODIND,a.PECHRPAR
        FROM tb_pecista  a
        INNER JOIN FP02T3 b ON A.PECEMP =b.PE01CODEMP AND A.PECCODCOL=B.FP02COD 
        WHERE pecdata>=to_date(:dt_ini,'dd/mm/yyyy')
        and pecdata<=to_date(:dt_final,'dd/mm/yyyy')
            """
        cursor.execute(SQL_Dados, {'dt_ini': dt_ini, 'dt_final': dt_final})

        columns = [col[0] for col in cursor.description]
        resultsDados = [dict(zip(columns, row)) for row in cursor.fetchall()]
        response = jsonify(resultsDados)
        response.headers.add("Access-Control-Allow-Origin", "*")  
        print(resultsDados)
        return response        

    except Exception as e:
        print(f"Erro na consulta SQL: {e}")
        # Trate o erro como apropriado
        return jsonify({'error': 'Erro na consulta SQL'}), 500
    finally:
        cursor.close()
        connection.close()
        