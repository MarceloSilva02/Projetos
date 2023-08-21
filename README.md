# Projetos
Meus projetos de Data Analytics


# Projeto - Sistema para acompanhar as devoluções na granularidade de ticket

query = '''
SELECT
  rr.coluna_produto,
  rr.coluna_ticketID,
  rr.data_de_envio,
  rr.data_report,
  rr.descrição_produto,
  rr.coluna_fornecedor,
  rr.coluna_categoria,
  rr.coluna_fonte_venda,
  rr.resposta_cliente, 
  ps.coluna_beneficiamento,
  CASE
    WHEN rr.motivo_devolução IN ('Defeito técnico', 'Insatisfação', 'Produto incompleto', 'Produto Danificado', 'Produto errado enviado')
    THEN 'Motivo de qualidade'
    ELSE 'Sem motivo de qualidade'
  END AS qualidade_flag,
  rr.motivo_devolução,
  rr.motivo_detalhado,
  rr.total_retornado,
  rr.total_vendido
FROM
  base1 rr
  LEFT JOIN base2 ps on rr.FK_coluna_produto = ps.PK_coluna_produto
WHERE
  rr.data_de_envio >= '2023'
  AND ps.coluna_produto_importadoornacional = 'Nacional'
  AND ps.beneficiamento = 'no'
'''
df1 = spark.sql(query)
