from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
@dag
def orchestrate():

    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host="https://dbc-345da47f-f997.cloud.databricks.com",
            token= "dapi99fc367e6d6509789ca7b72e38890495"
        )
        job_trigger=ws.jobs.run_now(job_id=230803767352281)

        while True:
            job_run = ws.jobs.get_run(job_trigger.run_id)
            
            print(f"Job run status: {job_run.state.life_cycle_state}, result state: {job_run.state.result_state}")

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED,RunLifeCycleState.SKIPPED,RunLifeCycleState.INTERNAL_ERROR]: 
                if(job_run.state.result_state == RunResultState.SUCCESS):
                    print("Job completed successfully!!")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
            time.sleep(5)
    
        return "CDC Ingestion Completed"
    
    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"

    
    silver_technical=BashOperator(
        task_id='silver_technical',
        bash_command='cd /opt/airflow/walmart_project && dbt run --select silver_t'
    )

    silver_technical_tests=BashOperator(
            task_id='silver_technical_test',
            bash_command='cd /opt/airflow/walmart_project && dbt test --select silver_t'
        )

    silver_business=BashOperator(
        task_id='silver_business',
        bash_command='cd /opt/airflow/walmart_project && dbt run --select silver_b'
    )

    silver_business_tests=BashOperator(
        task_id='silver_business_test',
        bash_command='cd /opt/airflow/walmart_project && dbt test --select silver_b'
    )

    gold_ephemeral=BashOperator(
        task_id='gold_ephemeral',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/ephemeral'
    )

    gold_dimensions=BashOperator(
        task_id='gold_dimensions',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt snapshot'
    )

    gold_facts=BashOperator(
        task_id='gold_facts',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/fact'
    )
    @task.bash
    def source_freshness():
        return " cd /opt/airflow/walmart_project && dbt source freshness"

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts

orchestrate_dag=orchestrate()