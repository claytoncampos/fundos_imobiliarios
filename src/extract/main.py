import requests
import csv
import yagmail
import smtplib
import requests.exceptions
import pandas as pd

from bs4 import BeautifulSoup
from configparser import ConfigParser
from rich.console import Console
from time import sleep
from requests_html import HTMLSession
from pathlib import Path


class WebScraping:
    def __init__(self, url):
        self.console = Console()
        self.configs = ConfigParser()
        self.configs.read(f'{Path.cwd().parents[1]}/conf/confs.ini')
        # print(self.configs['FundosConfig']['Fundos'])
        try:
            session = HTMLSession()
            site = session.get(url)
            site.html.render(wait=1, timeout=5000)
            self.html_puro = site.html.html

            self.iniciar_processos()
            if site.status_code != 200:
                self.console.print("🛑 [#ff5680]Não foi possível se conectar ao site! 🛑")

        except requests.exceptions.ConnectionError:
            self.console.print("🛑 [#ff5680]O site não existe ou está fora de ar! 🛑")

    def iniciar_processos(self):

        with self.console.status("🏂 [#d57bff]Raspando Dados...", spinner="dots"):
            dados = self.raspar_dados_do_site()
            self.console.print("🏂 [#fffc58]- [#00ff9c]Raspagem Concluida!")

        with self.console.status("📝 [#d57bff]Salvando dados...", spinner="dots"):
            self.salvar_dados_em_csv(dados)
            sleep(1)
            self.console.print("📝 [#fffc58]- [#00ff9c]Salvo com êxito!")

    #     # with self.console.status("📨 [#d57bff]Enviando Email...", spinner="dots"):
    #     #     self.enviar_email()

    @staticmethod
    def transform_to_df(lista_dados: pd.DataFrame()):
        header = ["TICKER", "SETOR",
                  "PREÇO ATUAL(R$)",
                  "LIQUIDEZ DIÁRIA(R$)",
                  "P/VP",
                  "ÚLTIMO DIVIDENDO",
                  "DIVIDEND YIELD",
                  "DY(3M) ACUMULADO",
                  "DY(6M) ACUMULADO",
                  "DY(12M) ACUMULADO",
                  "DY(3M) MÉDIA",
                  "DY(6M) MÉDIA",
                  "DY(12M) MÉDIA",
                  "DY ANO",
                  "VARIAÇÃO PREÇO",
                  "RENTAB.PERÍODO",
                  "RENTAB.ACUMULADA",
                  "PATRIMÔNIO LÍQUIDO",
                  "VPA",
                  "P/VPA",
                  "DY PATRIMONIAL",
                  "VARIAÇÃO PATRIMONIAL",
                  "RENTAB.PATR.PERÍODO",
                  "RENTAB.PATR.ACUMULADA",
                  "QUANT.ATIVOS",
                  "VOLATILIDADE",
                  "NUM.COTISTAS",
                  "TAX.GESTÃO",
                  "TAX.PERFORMANCE",
                  "TAX.ADMINISTRAÇÃO"]
        df_fiis = []

        for dados in lista_dados:
            df_fiis.append(dados)
        df = pd.DataFrame(df_fiis, columns=header)
        return df

    @staticmethod
    def salvar_dados_em_csv(lista_dados: list):
        header = ["TICKER",
                  "SETOR",
                  "PREÇO ATUAL(R$)",
                  "LIQUIDEZ DIÁRIA(R$)",
                  "P/VP",
                  "ÚLTIMO DIVIDENDO",
                  "DIVIDEND YIELD",
                  "DY(3M) ACUMULADO",
                  "DY(6M) ACUMULADO",
                  "DY(12M) ACUMULADO",
                  "DY(3M) MÉDIA",
                  "DY(6M) MÉDIA",
                  "DY(12M) MÉDIA",
                  "DY ANO",
                  "VARIAÇÃO PREÇO",
                  "RENTAB.PERÍODO",
                  "RENTAB.ACUMULADA",
                  "PATRIMÔNIO LÍQUIDO",
                  "VPA",
                  "P/VPA",
                  "DY PATRIMONIAL",
                  "VARIAÇÃO PATRIMONIAL",
                  "RENTAB.PATR.PERÍODO",
                  "RENTAB.PATR.ACUMULADA",
                  "QUANT.ATIVOS",
                  "VOLATILIDADE",
                  "NUM.COTISTAS",
                  "TAX.GESTÃO",
                  "TAX.PERFORMANCE",
                  "TAX.ADMINISTRAÇÃO"]
        with open(f'{Path.cwd().parents[1]}/data/FIIs.csv', 'w', newline="") as arquivo_csv:
            fiis_escritor = csv.writer(arquivo_csv, dialect='excel')
            fiis_escritor.writerow(header)
            for dados in lista_dados:
                fiis_escritor.writerow(dados)

    def raspar_dados_do_site(self) -> list:

        dados = []

        html = BeautifulSoup(self.html_puro, "html.parser")

        tabela = html.find("tbody")
        # print(tabela)
        tabela_fundos = tabela.findAll("tr")
        for fundo in tabela_fundos:
            colunas = fundo.findAll("td")

            nome_fundo = colunas[0].find("a")
            if nome_fundo.text in self.configs['FundosConfig']['Fundos'] or self.configs['FundosConfig'][
                'Fundos'] == '[]':
                setor = colunas[1]
                preco_atual = colunas[2]
                liquidez_diaria = colunas[3]
                pvp = colunas[4]
                ultimo_dividendo = colunas[5]
                dividend_yeld = colunas[6]
                dy_3m_acumulado = colunas[7]
                dy_6m_acumulado = colunas[8]
                dy_12m_acumulado = colunas[9]
                dy_3m_media = colunas[10]
                dy_6m_media = colunas[11]
                dy_12m_media = colunas[12]
                dy_ano = colunas[13]
                variacao_preco = colunas[14]
                rentab_periodo = colunas[15]
                rentab_acumulada = colunas[16]
                patrimonio_liquido = colunas[17]
                vpa = colunas[18]
                pvpa = colunas[19]
                dy_patrimonial = colunas[20]
                variacao_patrimonial = colunas[21]
                rentab_patrimonial_periodo = colunas[22]
                rentab_patrimonial_acumulada = colunas[23]
                qtd_ativos = colunas[24]
                volatilidade = colunas[25]
                numero_cotistas = colunas[26]
                tx_gestao = colunas[27]
                tx_performance = colunas[28]
                tx_adm = colunas[29]

                dados.append([
                    nome_fundo.text,
                    setor.text,
                    preco_atual.text,
                    liquidez_diaria.text,
                    pvp.text,
                    ultimo_dividendo.text,
                    dividend_yeld.text,
                    dy_3m_acumulado.text,
                    dy_6m_acumulado.text,
                    dy_12m_acumulado.text,
                    dy_3m_media.text,
                    dy_6m_media.text,
                    dy_12m_media.text,
                    dy_ano.text,
                    variacao_preco.text,
                    rentab_periodo.text,
                    rentab_acumulada.text,
                    patrimonio_liquido.text,
                    vpa.text,
                    pvpa.text,
                    dy_patrimonial.text,
                    variacao_patrimonial.text,
                    rentab_patrimonial_periodo.text,
                    rentab_patrimonial_acumulada.text,
                    qtd_ativos.text,
                    volatilidade.text,
                    numero_cotistas.text,
                    tx_gestao.text,
                    tx_performance.text,
                    tx_adm.text
                ])

        return dados


'''
    def enviar_email(self):

        try:
            email = yagmail.SMTP(self.configs['EmailConfig']['EmailPostador'], self.configs['EmailConfig']['SenhaPostador'])
            email.send(
                to = self.configs['EmailConfig']['EmailRemetente'],
                subject = 'FIIs',
                contents = 'Aqui está os FIIs que você tem interesse em saber!',
                attachments = 'FIIs.csv'
            )

            self.console.print("📨 [#fffc58]- [#00ff9c]Email Enviado!")

        except smtplib.SMTPAuthenticationError:
            self.console.print("🛑 [#ff5680]Usuário ou senha inválidos! 🛑")

'''
WebScraping("https://www.fundsexplorer.com.br/ranking")
