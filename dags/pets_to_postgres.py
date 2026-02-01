from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

def load_pets_from_json():
    with open('/opt/airflow/dags/data/pets-data.json', 'r') as file:
        data = json.load(file)
    
    pets = data.get('pets', [])
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS pets (
            name VARCHAR(100) PRIMARY KEY,
            species VARCHAR(50),
            fav_foods JSONB DEFAULT '[]',
            birth_year INTEGER,
            photo TEXT
        )
    """)
    
    pg_hook.run("TRUNCATE TABLE pets")
    
    for pet in pets:
        pg_hook.run(
            """
            INSERT INTO pets (name, species, fav_foods, birth_year, photo)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (name) DO NOTHING
            """,
            parameters=(
                pet['name'],
                pet['species'],
                json.dumps(pet.get('favFoods', [])),  # Сохраняем как JSON
                pet.get('birthYear'),
                pet.get('photo', '')
            )
        )
    
    print(f"Загружено {len(pets)} питомцев")

with DAG(
    'pets_json_loader',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['pets', 'json', 'postgres']
) as dag:
    
    load_pets = PythonOperator(
        task_id='load_pets_data',
        python_callable=load_pets_from_json
    )