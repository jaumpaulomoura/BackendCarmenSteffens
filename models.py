
import cx_Oracle

from database import connect_to_oracle


class Usuario:
    def __init__(self, login, senha):
        self.login = login
        self.senha = senha
        self.senhaCript = self.criptografarSenha(senha)

    def criptografarSenha(self, passwd):
        senhaCripto = ""
        espacos = "                    "
        usuario = self.login

        usuario = usuario + espacos
        usuario = usuario[:20]
        passwd = passwd + espacos
        passwd = passwd[:20]

        passwd = passwd[:]        
    
        for i in range(20):
            valorUsuarioI = ord(usuario[i])
            valorUsuarioF = ord(usuario[19 - i])
            valorSenhaI = ord(passwd[i])
            valorSenhaF = ord(passwd[19 - i])
            resultado = valorUsuarioI + valorUsuarioF * 3 + valorSenhaI * 7 + valorSenhaF * 11
            resultado = (resultado % 71) + 48
            resultadoChar = chr(resultado)
            senhaCripto = senhaCripto + resultadoChar

        return senhaCripto

    def verificarSenha(self, senhaCriptografada):
        return self.senhaCript == senhaCriptografada


class UsuarioDao:
    def __init__(self, user, app):
        self.usuario = user
        self.app = app

    def verificarUsuario(self):
        
        conexao = connect_to_oracle(self.app)

        cursor = conexao.cursor()
        try:
            sql = f"SELECT CF02LOGIN, CF02NOME, CF02SENHA FROM CF02T WHERE CF02LOGIN = '{self.usuario.login}'"
            cursor.execute(sql)
            for row in cursor:
                senhaCriptografada = row[2]  
                if self.usuario.verificarSenha(senhaCriptografada):
                    return True
        except cx_Oracle.Error as error:
            print("Erro durante a consulta:", error)
        finally:
            cursor.close()
            conexao.close()

        return False