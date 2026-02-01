from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import xml.etree.ElementTree as ET

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

def xml_to_postgres():
    # Путь к XML файлу
    xml_path = '/opt/airflow/dags/data/nutrition.xml'
    
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    hook.run("""
        CREATE TABLE IF NOT EXISTS nutrition (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            manufacturer VARCHAR(100),
            serving_size NUMERIC(10,2),
            serving_unit VARCHAR(20),
            calories_total INTEGER,
            calories_from_fat INTEGER,
            total_fat NUMERIC(10,2),
            saturated_fat NUMERIC(10,2),
            cholesterol INTEGER,
            sodium INTEGER,
            carbohydrates NUMERIC(10,2),
            fiber NUMERIC(10,2),
            protein NUMERIC(10,2),
            vitamin_a INTEGER,
            vitamin_c INTEGER,
            calcium INTEGER,
            iron INTEGER
        )
    """)
    
    hook.run("TRUNCATE TABLE nutrition")
    
    for food in root.findall('food'):
        name = food.find('name').text if food.find('name') is not None else ''
        manufacturer = food.find('mfr').text if food.find('mfr') is not None else ''
        
        serving = food.find('serving')
        serving_size = serving.text if serving is not None else '0'
        serving_unit = serving.get('units', '') if serving is not None else ''
        
        calories = food.find('calories')
        calories_total = calories.get('total', '0') if calories is not None else '0'
        calories_fat = calories.get('fat', '0') if calories is not None else '0'
        
        total_fat = food.find('total-fat').text if food.find('total-fat') is not None else '0'
        saturated_fat = food.find('saturated-fat').text if food.find('saturated-fat') is not None else '0'
        cholesterol = food.find('cholesterol').text if food.find('cholesterol') is not None else '0'
        sodium = food.find('sodium').text if food.find('sodium') is not None else '0'
        carbs = food.find('carb').text if food.find('carb') is not None else '0'
        fiber = food.find('fiber').text if food.find('fiber') is not None else '0'
        protein = food.find('protein').text if food.find('protein') is not None else '0'
        
        vitamins = food.find('vitamins')
        vitamin_a = vitamins.find('a').text if vitamins is not None and vitamins.find('a') is not None else '0'
        vitamin_c = vitamins.find('c').text if vitamins is not None and vitamins.find('c') is not None else '0'
        
        minerals = food.find('minerals')
        calcium = minerals.find('ca').text if minerals is not None and minerals.find('ca') is not None else '0'
        iron = minerals.find('fe').text if minerals is not None and minerals.find('fe') is not None else '0'
        
        hook.run(
            """
            INSERT INTO nutrition 
            (name, manufacturer, serving_size, serving_unit, calories_total, 
             calories_from_fat, total_fat, saturated_fat, cholesterol, sodium, 
             carbohydrates, fiber, protein, vitamin_a, vitamin_c, calcium, iron)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                name.strip(),
                manufacturer.strip(),
                float(serving_size) if serving_size.strip() else 0,
                serving_unit.strip(),
                int(float(calories_total)) if calories_total.strip() else 0,
                int(float(calories_fat)) if calories_fat.strip() else 0,
                float(total_fat) if total_fat.strip() else 0,
                float(saturated_fat) if saturated_fat.strip() else 0,
                int(float(cholesterol)) if cholesterol.strip() else 0,
                int(float(sodium)) if sodium.strip() else 0,
                float(carbs) if carbs.strip() else 0,
                float(fiber) if fiber.strip() else 0,
                float(protein) if protein.strip() else 0,
                int(float(vitamin_a)) if vitamin_a.strip() else 0,
                int(float(vitamin_c)) if vitamin_c.strip() else 0,
                int(float(calcium)) if calcium.strip() else 0,
                int(float(iron)) if iron.strip() else 0
            )
        )
    
    print(f"Загружено {len(root.findall('food'))} продуктов")

with DAG(
    'xml_nutrition_loader',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['xml', 'nutrition', 'postgres']
) as dag:
    
    load_data = PythonOperator(
        task_id='load_xml_to_postgres',
        python_callable=xml_to_postgres
    )