from fastapi import FastAPI, UploadFile, File, BackgroundTasks, Depends
from config import settings
from kubernetes import client, config
from jinja2 import Template
import boto3
import uuid
import yaml


app = FastAPI()

def get_settings():
    return settings

try:
    config.load_incluster_config()
except:
    config.load_kube_config()

k8s_api = client.CustomObjectsApi()

# Konfiguracja MinIO
s3 = boto3.client('s3',
                  endpoint_url=settings.endpoint_url,
                  aws_access_key_id=settings.s3_username,
                  aws_secret_access_key=settings.s3_password,
                  )


def trigger_spark_job(filename: str):
    job_id = str(uuid.uuid4())[:8]
    job_name = f"rag-job-{job_id}"
    file_path = f"s3a://raw-data/{filename}"

    print(f"Generuję Job: {job_name}")

    # 1. Wczytaj szablon z pliku
    with open("spark-template.yaml", "r") as f:
        template_content = f.read()

    # 2. Wypełnij zmienne (Jinja2)
    template = Template(template_content)
    rendered_yaml = template.render(
        job_name=job_name,
        file_path=file_path
    )

    # 3. Zamień tekst YAML na słownik Python (dla biblioteki kubernetes)
    spark_manifest = yaml.safe_load(rendered_yaml)

    # 4. Wyślij do klastra
    try:
        k8s_api.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace="graphrag",
            plural="sparkapplications",
            body=spark_manifest
        )
        print("✅ Wysłano do K8s!")
    except Exception as e:
        print(f"❌ Błąd K8s: {e}")


@app.post("/upload")
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    s3.upload_fileobj(file.file, "raw-data", file.filename)
    background_tasks.add_task(trigger_spark_job, file.filename)
    return {"message": "Działam!"}