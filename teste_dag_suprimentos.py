from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd

from commons.msgraph.sharepoint.site import consultar_site
from commons.msgraph.sharepoint.drive import selecionar_drive
from google.oauth2 import service_account
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# Task 1.1 - Define DAG arguments
default_args = {
    'owner': 'Allan',
    'start_date': days_ago(0),
    'email': ['allan.sousa@edglobo.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(days=1)
}

# Funcao que puxa as planilhas do datapool via commons

def helloworld(site, arquivo):
    site = consultar_site(site)
    drive = selecionar_drive(site_id=site.id, nome='Documentos')
    arquivo = drive.arquivo(arquivo)

    excel = pd.ExcelFile(arquivo.conteudo_binario)
    dataframe = pd.read_excel(excel)

    print(dataframe.info())
    
    return dataframe


#CONEXÃO COM O BANCO DE DADOS DO PROTHEUS
sqlserver = MsSqlHook(mssql_conn_id='edg_infobiprd_ods')


def inserir_ods_userservices1():

    
    df_compras_radio = sqlserver.get_pandas_df(
        sql="""SELECT [CodEmpresa]
        ,[CodFilial]
        ,[NumPedidoCompra]
        ,[CodItem]
        ,[DescAplicacao]
        ,cast([DtEmissao] as varchar) as DtEmissao
        ,[CodFornecedor]
        ,[CodLoja]
        ,[NomeFornecedor]
        ,[CodProduto]
        ,[DesProduto]
        ,[DesComplementar]
        ,[DesGrProd]
        ,[Quantidade]
        ,[PrcUnitario]
        ,[VlrTotal]
        ,[QtdEntregue]
        ,[SaldoReceber]
        ,[VlrSaldoRec]
        ,[JustificativaCompra]
        ,[CtaContabil]
        ,[DesConta]
        ,[CodCentroCusto]
        ,[DesCentroCusto]
        ,[ClasseValor]
        ,[DesClasVlr]
        ,[PedEncerrado]
        ,[CodUsuario]
    FROM [ODS].[dbo].[tbODSPedidosComprasRadio]"""
    )
    return df_compras_radio

def inserir_ods_userservices2():
    
    df_fato_requisicao = sqlserver.get_pandas_df(
        sql= """SELECT [Num_requisicao]
        ,[Item]
        ,[DataSolicitacao]
        ,[DataModificacao]
        ,[Criadopor]
        ,[CodMaterial]
        ,[TextoBreve]
        ,[Quantidade]
        ,[UnidadeMedida]
        ,[DataRemessa]
        ,[GrupoMercadorias]
        ,[Centro]
        ,[dataPedido]
        ,[GrpCompradores]
        ,[NumPedido]
        ,[TemPedido]
        ,[ItemPedido]
        ,[SlaAtendido]
        ,[CentroCusto]
        ,[TipoDocumento]
        ,[CategoriaItem]
        ,[CodEliminação] as CodEliminacao
    FROM [FINANC].[dbo].[vwCompras_FatoRequisicao]"""
    )
    return df_fato_requisicao

def inserir_ods_userservices3():
    
    df_conexao_fato_pedido = sqlserver.get_pandas_df(
        sql= """SELECT [NumPedido]
        ,[ItemPedido]
        ,[PedidoItem]
        ,[DataCriacao]
        ,[DataModificacao]
        ,[IdMaterial]
        ,[Empresa]
        ,[Quantidade]
        ,[PrecoLiquidoMoedaDoc]
        ,[ValorLiquido]
        ,[ValorBruto]
        ,[CodIVA]
        ,[CodRecusa]
        ,[FlagEntradaMercadoria]
        ,[FlagEntradaMercadoriaNaoAvaliada]
        ,[NumConfirmacaoOrdem]
        ,[NumContrato]
        ,[NumItemContrato]
        ,[CodUnidadeMedida]
        ,[ValorContrato]
        ,[ValorIvaNaoDedutivel]
        ,[QtdSolicitadaNormal]
        ,[DataDeterminacaoPreco]
        ,[ValorEfetivoItem]
        ,[CodCliente]
        ,[NumEnderecoDocCompra]
        ,[DiasPrazoEntrega]
        ,[PesoLiquido]
        ,[CodUnidadePeso]
        ,[CodDomicilioFiscal]
        ,[PesoBruto]
        ,[Volume]
        ,[CodTipoMaterial]
        ,[CodTipoDocCompras]
        ,[StatusDocCompras]
        ,[NomeResponsavel]
        ,[IntervaloItens]
        ,[UltimoNumItem]
        ,[IdFornecedor]
        ,[ChaveCondicoesPagamento]
        ,[CodOrganizacaoCompras]
        ,[CodGrupoCompradores]
        ,[IdMoeda]
        ,[DataDocCompra]
        ,[Cancelado]
        ,[TaxaCambio]
        ,[IdSituacao]
    FROM [FINANC].[dbo].[vwCompras_FatoPedido]"""
    )
    return df_conexao_fato_pedido

def joganoGCP():
    df_criado_por = helloworld('/sites/PaineldeGestao', '/Compras/POWER BI/CRIADO POR.xlsx')
    #df_gr_compradores = helloworld('/sites/PaineldeGestao', '/Compras/POWER BI/GRUPO COMPRADORES.xlsx')
    df_protheus_de_para = helloworld('/sites/PaineldeGestao', '/Compras/POWER BI/PROTHEUS_DE PARA.xlsx')
    df_fun_mes = helloworld('/sites/PaineldeGestao', '/Compras/POWER BI/Relação Func_Mês.xlsx')
    df_saving_contratos = helloworld('/sites/PaineldeGestao', '/Compras/POWER BI/Saving_Contratos.xlsx')

    #TRATAMENTO DOS DADOS
    #df_gr_compradores.rename(columns={'Categoria ':'Categoria', 'Nome ':'Nome','Célula':'Celula'}, inplace = True)
    df_protheus_de_para.rename(columns={'Cod. Usuario':'CodUsuario', 'Nome Usuario':'NomeUsuario'}, inplace = True)
    df_fun_mes.rename(columns={'CÉLULA':'CELULA', 'Quant. Analista':'QuantAnalista'}, inplace = True)
    df_saving_contratos.rename(columns={'Nome_Sav Cont':'NomeSavCont'}, inplace = True)



    #CONEXÃO CLOUD BIGQUERY
    import json

    key_path = "/opt/airflow/chaves/edg-infra-bi.json"
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])


    #CARGA DOS DATAFRAMES NO BIGQUERY
    inserir_ods_userservices1().to_gbq(credentials=credentials, destination_table = 'Compras.tb_Pedidos_Compras', if_exists='replace')
    inserir_ods_userservices2().to_gbq(credentials=credentials, destination_table = 'Compras.tb_Fato_Requisicao', if_exists='replace')
    inserir_ods_userservices3().to_gbq(credentials=credentials, destination_table = 'Compras.tb_Fato_Pedido', if_exists='replace')
    df_criado_por.to_gbq(credentials=credentials, destination_table = 'Compras.tb_criado_por', if_exists='replace')
    #df_gr_compradores.to_gbq(credentials=credentials, destination_table = 'Compras.tb_gr_compradores', if_exists='replace')
    df_protheus_de_para.to_gbq(credentials=credentials, destination_table = 'Compras.tb_protheus_de_para', if_exists='replace')
    df_fun_mes.to_gbq(credentials=credentials, destination_table = 'Compras.tb_fun_mes', if_exists='replace')
    df_saving_contratos.to_gbq(credentials=credentials, destination_table = 'Compras.tb_saving_contratos', if_exists='replace')

with DAG(
    dag_id='teste_dag_suprimentos',
    default_args=default_args,
    description='[D0-665]',
    schedule_interval=timedelta(days=1)

) as dag:
    datapool1 = PythonOperator(
        task_id='extrair_datapool_1',
        python_callable=helloworld,
        op_kwargs={
            'site': '/sites/PaineldeGestao',
            'arquivo': '/Compras/POWER BI/CRIADO POR.xlsx'
        }
    )

#datapool2 = PythonOperator(
#    dag = dag,
#    task_id='extrair_datapool_2',
#    python_callable=helloworld,
#    op_kwargs={
#        'site': '/sites/PaineldeGestao',
#        'arquivo': '/Compras/POWER BI/GRUPO COMPRADORES.xlsx'
#    }
#)

datapool2 = PythonOperator(
    dag = dag,
    task_id='extrair_datapool_2',
    python_callable=helloworld,
    op_kwargs={
        'site': '/sites/PaineldeGestao',
        'arquivo': '/Compras/POWER BI/PROTHEUS_DE PARA.xlsx'
    }
)

datapool3 = PythonOperator(
    dag = dag,
    task_id='extrair_datapool_3',
    python_callable=helloworld,
    op_kwargs={
        'site': '/sites/PaineldeGestao',
        'arquivo': '/Compras/POWER BI/Relação Func_Mês.xlsx'
    }
)

datapool4 = PythonOperator(
    dag = dag,
    task_id='extrair_datapool_4',
    python_callable=helloworld,
    op_kwargs={
        'site': '/sites/PaineldeGestao',
        'arquivo': '/Compras/POWER BI/Saving_Contratos.xlsx'
    }
)

inserirODS = PythonOperator(
    dag = dag,
    task_id='inserir_dados_ods',
    python_callable=inserir_ods_userservices1
)

inserirODS2 = PythonOperator(
    dag = dag,
    task_id='inserir_dados_ods2',
    python_callable=inserir_ods_userservices2
)

inserirODS3 = PythonOperator(
    dag = dag,
    task_id='inserir_dados_ods3',
    python_callable=inserir_ods_userservices3
)


alimentaBQ = PythonOperator(
    dag = dag,
    task_id='alimentaBQ',
    python_callable=joganoGCP
)

datapool1 >> datapool2 >> datapool3 >> datapool4 >> inserirODS >> inserirODS2 >> inserirODS3 >> alimentaBQ
