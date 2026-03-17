# Imports
import boto3
from botocore.exceptions import ClientError

# Configuração do cliente S3 para MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minio",
    aws_secret_access_key="minio123",
    region_name="us-east-1"
)

# Variáveis
bucket_name = "data-lake"
file_name = "temp/dados_brutos.csv"
object_name = "raw-data/dados_brutos.csv"


# Função para verificar se o bucket existe
def verifica_cria_bucket(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' já existe.")

    except ClientError:
        print(f"Bucket '{bucket_name}' não encontrado. Criando...")
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' criado com sucesso.")


# Verifica ou cria o bucket
verifica_cria_bucket(bucket_name)

# Upload do arquivo
s3.upload_file(file_name, bucket_name, object_name)

print(
    f"\nLog - Arquivo '{file_name}' enviado para '{bucket_name}/{object_name}' com sucesso.\n"
)