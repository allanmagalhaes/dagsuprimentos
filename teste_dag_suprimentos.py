from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas

from commons.msgraph.sharepoint.site import consultar_site
from commons.msgraph.sharepoint.drive import selecionar_drive

# Task 1.1 - Define DAG arguments
default_args = {
    'owner':'Allan',
    'start_date':days_ago(0),
    'email':['allan.sousa@edglobo.com.br'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(days = 1)
}

def helloworld(site, arquivo):
    site = consultar_site(site)
    drive = selecionar_drive(site_id=site.id, nome='Documentos')
    arquivo = drive.arquivo(arquivo)

    excel = pandas.ExcelFile(arquivo.conteudo_binario)
    dataframe = pandas.read_excel(excel)

    print(dataframe.info())

with DAG(
    dag_id='teste_dag_suprimentos',
    default_args=default_args,
    description='[D0-665]',
    schedule_interval=timedelta(days=1)

) as dag:
    datapool1 = PythonOperator(
    task_id = 'extrair_datapool_1',
    python_callable=helloworld,
    op_kwargs={
        'site': '/sites/PaineldeGestao',
        'arquivo': '/Compras/POWER BI/CRIADO POR.xlsx'
    }
)

datapool2 = PythonOperator(
    dag = dag,
    task_id='extrair_datapool_2',
    python_callable=helloworld,
    op_kwargs={
        'site': '/sites/PaineldeGestao',
        'arquivo': '/Compras/POWER BI/GRUPO COMPRADORES.xlsx'
    }
)

datapool3 = PythonOperator(
    dag = dag,
    task_id='extrair_datapool_3',
    python_callable=helloworld,
    op_kwargs={
        'site': '/sites/PaineldeGestao',
        'arquivo': '/Compras/POWER BI/PROTHEUS_DE PARA.xlsx'
    }
)

datapool4 = PythonOperator(
    dag = dag,
    task_id='extrair_datapool_4',
    python_callable=helloworld,
    op_kwargs={
        'site': '/sites/PaineldeGestao',
        'arquivo': '/Compras/POWER BI/Relação Func_Mês.xlsx'
    }
)

datapool5 = PythonOperator(
    dag = dag,
    task_id='extrair_datapool_5',
    python_callable=helloworld,
    op_kwargs={
        'site': '/sites/PaineldeGestao',
        'arquivo': '/Compras/POWER BI/SAVING.xlsx'
    }
)

datapool6 = PythonOperator(
    dag = dag,
    task_id='extrair_datapool_6',
    python_callable=helloworld,
    op_kwargs={
        'site': '/sites/PaineldeGestao',
        'arquivo': '/Compras/POWER BI/Saving_Contratos.xlsx'
    }
)

datapool1 >> datapool2 >> datapool3 >> datapool4 >> datapool5 >> datapool6
