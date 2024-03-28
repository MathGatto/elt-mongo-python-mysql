# PROJETO: Pipeline de Dados ELT

# Cenário: O time de Data Science e BI gostariam dos dados de uma API específica, porém cada um os utiliza de uma forma.
# Desafio: Montar um Pipeline ELT que:
  1 - Extrai dados de uma API (json) e os salva uma cópia no formato BRUTO no MONGODB para serem utilizados pelo time de Data Science.       
  2 - Trata e transforma os dados extraidos em Pymongo                                                                                     
  3 - Salva os dados tratados no MySql para sere consumidos no formato correto pelo time de BI
