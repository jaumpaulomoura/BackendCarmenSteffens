
import cx_Oracle
from flask import Blueprint, current_app, jsonify, request

from database import connect_to_oracle
GrupoPecista = Blueprint('grupoPecista', __name__)

@GrupoPecista.route('/api/grupoPecista/colaborador', methods=['GET'])
def consultar_colaborador():
    app = current_app   
    connection = connect_to_oracle(app)
    cursor = connection.cursor()

    SQL="""SELECT 
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
            b.fp02funcao         as Funcao

            FROM FP02T3 b


            WHERE B.FP02OBS LIKE '%PXY%'


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
        
@GrupoPecista.route('/api/grupoPecista/prod', methods=['POST'])
def consultar_prod():
    app = current_app
    connection = connect_to_oracle(app)
    cursor = connection.cursor()
    
    try:
        data = request.json.get('data')  # Ajuste aqui para obter dados do corpo da solicitação JSON
        grupo = request.json.get('grupo')  # Ajuste aqui para obter dados do corpo da solicitação JSON

        print(f"Parâmetros recebidos - data: {data}, grupo: {grupo}")
        print(f"Parâmetros recebidos - data: {data}, grupo: {grupo}")
        SQL="""select 
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
      and d.pc23data = to_date (:data, 'DD/MM/YYYY')
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
        return response        

    except Exception as e:
        print(f"Erro na consulta SQL: {e}")
        # Trate o erro como apropriado
        return jsonify({'error': 'Erro na consulta SQL'}), 500
    finally:
        cursor.close()
        connection.close()
    