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

    manifest = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": job_name,
            "namespace": "graphrag"
        },
        "spec": {
            "deps": {
                "jars": [
                    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
                    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
                ]
            },
            "type": "Python",
            "mode": "cluster",
            "image": "spark-with-script:v1",
            "imagePullPolicy": "Never",
            "mainApplicationFile": "local:///opt/spark/scripts/etl-job.py",
            "sparkVersion": "3.5.0",
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 600,
            "arguments": [f"s3a://raw-data/{filename}"],
            "sparkConf": {
                "spark.jars.ivy": "/tmp/.ivy",
                "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp/.ivy"
            },
            "driver": {
                "cores": 1,
                "memory": "1024m",
                "serviceAccount": "spark",
                "nodeSelector": {"kubernetes.io/hostname": "rpi-server"}
            },
            "executor": {
                "cores": 1,
                "instances": 1,
                "memory": "4096m",
                "affinity": {
                    "nodeAffinity": {
                        "preferredDuringSchedulingIgnoredDuringExecution": [{
                            "weight": 100,
                            "preference": {
                                "matchExpressions": [{
                                    "key": "kubernetes.io/hostname",
                                    "operator": "In",
                                    "values": ["fedora-maciek"]
                                }]
                            }
                        }]
                    }
                }
            }
        }
    }

    try:
        k8s_api.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace="graphrag",
            plural="sparkapplications",
            body=manifest
        )
        print(f"✅ Job {job_name} wysłany!")
    except Exception as e:
        print(f"❌ Błąd K8s: {e}")


@app.post("/upload")
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    s3.upload_fileobj(file.file, "raw-data", file.filename)
    background_tasks.add_task(trigger_spark_job, file.filename)
    return {"message": "Działam!"}