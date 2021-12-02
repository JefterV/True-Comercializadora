import requests
from zipfile import ZipFile
import csv
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from time import sleep

class main():

    def __init__(self) -> None:
        url = "https://datawarehouse-true.s3-sa-east-1.amazonaws.com/teste-true/teste_true_term.zip"
        dirName = "./Dados/downloads/teste_true_term.zip"
        dirArqs = "./Dados/arquivos"
        log = "./Dados/logs/log.log "
        
        dataHora = datetime.datetime.now()
        log = open(log, "a+")

        downloadArq = self.downloadZip(url, dirName)
        if not downloadArq or type(downloadArq) != bool:
            print("Error: Requisição falhou.")
            log.writelines(f"\n[{dataHora}]:  -> Requição falhou:{downloadArq}")
            log.close()
            return 

        extraindoZip = self.extrairZip(dirName, None)
        if not isinstance(extraindoZip, list):
            print("Error: Falha na extração dos arquivos.")
            log.writelines(f"\n[{dataHora}]:  -> Falha na extração dos arquivos:{extraindoZip}")
            log.close()
            return

        csvArq = lerArquivo = self.lerArquivos("encad-termicas.csv")
        if not csvArq or type(csvArq) != dict:
            print("Error: Falha na leitura do arquivo excel")
            log.writelines(f"\n[{dataHora}]:  -> Falha na leitura do arquivo excel:{csvArq}")
            log.close()
            return

        alterarDadosvar = self.alterarDados("TERM.DAT", "TERM_TRUE.DAT", csvArq)
        if not alterarDadosvar or type(alterarDadosvar) != bool:
            print("Error: Falha na alteração de dados")
            log.writelines(f"\n[{dataHora}]:  -> Falha na alteração de dados:{alterarDadosvar}")
            log.close()
            return
        
        self.compactarArquivos(dirArqs)

    # Baixar arquivo
    def downloadZip(self, url, save_path) -> bool:
        """
        Essa função recebe:
        * url = str (URL DA REQUISIÇÃO)
        * save_path = str (DIRETORIO ONDE IRÁ SALVAR O ARQUIVO)

        RETURN True or False
        True  = Requisição bem sucedida
        False = Requição falhou
        """
        chunk_size = 128
        respostaStatus = 403
        try:
            resposta  = requests.get(url, stream=True)
            with open(save_path, 'wb') as fd:
                for chunk in resposta.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
        except Exception as e:
            return e
        else:
            respostaStatus = resposta.status_code
        
            if respostaStatus == 200:
                return True
            else:
                return resposta
       
    # Extrair dados do ZIP
    def extrairZip(self, dir, dirDestino) -> bool or list:
        try:
            extrairZip = ZipFile(dir, 'r')
            extrairZip.extractall(path=dirDestino)
            extrairZip.close()
        except Exception as e:
            return e
        else:
            return extrairZip.namelist()

    # Ler Excel
    def lerArquivos(self, dir) -> bool or dict:
        try:
            lendoArq = csv.reader(open(dir), delimiter = ',')

            dados = {}
            index = 0
            for linha in lendoArq:
                if index == 0:
                    index += 1
                    continue
                dados[linha[0]] = linha[2:] 
            
                index += 1
        except Exception as e:
            return e
        else:
            return dados


    # Alterar dados do arquivo TERM.DAT
    def alterarDados(self,dirDat, dirNew, dictCSV: dict) -> bool:
        dados = {}
        dadosLinha = []
        lenColuna  = []
        try:
            lendoArquivoDat = open(dirDat, 'r')
            newArq = open(dirNew, 'w+')
        except Exception as e:
            return e
        else:
            index = 0
            # Lendo arquivo e alterando valores
            for linha in lendoArquivoDat:
                dadosLinha.clear()
                tratandoLinha = linha.split(" ")

                # Primeira linha
                if index == 0:
                    index += 1
                    newArq.writelines(linha)
                    continue

                # Segunda linha linha    
                elif index == 1:
                    newArq.writelines(linha)
                    for valor in tratandoLinha:
                        valor = valor.strip()
                        if len(valor) > 0:
                            lenColuna.append(len(valor))
                    
                    index += 1
                    continue

                ID = linha[1:4]
                linha = linha.replace("\n", "")
                indexIDName = lenColuna[0]+lenColuna[1]+4
                tratandoLinha = linha[indexIDName:].split(" ")                     

                for valor in tratandoLinha:
                    if len(valor) > 0:
                        dadosLinha.append(valor.strip())

                dados[ID] = dadosLinha

                # ESCREVENDO NO NOVO ARQUIVO
                if ID in dictCSV.keys():
                    count = 0
                    for valor in dictCSV[ID]:
                        if dados[ID][count] != valor:
                            dados[ID][count] = valor.strip() 
                            
                        while (True):
                            if len(dados[ID][count]) < lenColuna[count+2]:
                                dados[ID][count] = " "+str(dados[ID][count])
                                
                            else: break
                
                        count += 1

                    linha2 = f"{linha[:indexIDName]}{dados[ID][0]} {dados[ID][1]}  {dados[ID][2]} {dados[ID][3]} {dados[ID][4]} {dados[ID][5]} {dados[ID][6]} {dados[ID][7]} {dados[ID][7]} {dados[ID][9]} {dados[ID][10]} {dados[ID][11]} {dados[ID][12]} {dados[ID][13]} {dados[ID][14]} {dados[ID][15]} {dados[ID][16]}\n"
                    newArq.writelines(linha2) 
        
                else:
                    newArq.writelines(linha+"\n")

            lendoArquivoDat.close()
            newArq.close()

            return True

    # Compactando arquivos 
    def compactarArquivos(self, dirName):
        import zipfile
       
        sleep(2)
        zipArqs = ZipFile(dirName+'/teste_true_term.zip', 'w', zipfile.ZIP_DEFLATED)
        
        zipArqs.write("TERM.DAT")
        zipArqs.write("TERM_TRUE.DAT")
        zipArqs.write("encad-termicas.csv")
        zipArqs.close()


         

main()
