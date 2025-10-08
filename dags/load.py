from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task
def create_table(columns_list: list):

    hook = PostgresHook(postgres_conn_id="postgres")
    
    sql = f"""
        CREATE TABLE IF NOT EXISTS YT_Videos(
            {columns_list[0]} TEXT NOT NULL,
            {columns_list[1]} VARCHAR(20) NOT NULL,
            {columns_list[2]} INT,
            {columns_list[3]} INT,
            {columns_list[4]} INT,
            {columns_list[5]} DATE NOT NULL
        );
        """
    hook.run(sql) 


        
@task
def load_YT_data(csv_file_path):

    hook = PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(
        sql="COPY YT_Videos FROM STDIN WITH CSV HEADER",
        filename=csv_file_path
    )
            
