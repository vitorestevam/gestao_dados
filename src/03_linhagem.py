# Imports
import json
import boto3
import hashlib
import pandas as pd
from datetime import datetime

# Configuração do cliente S3 para MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minio",
    aws_secret_access_key="minio123",
    region_name="us-east-1"
)

bucket_name = "data-lake"
object_name = "raw-data/dados_brutos.csv"


# Função para calcular o hash de um arquivo
def calcula_hash(file_name):

    sha256_hash = hashlib.sha256()

    with open(file_name, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


# Verifica se o bucket existe
def verifica_cria_bucket(bucket_name):

    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' já existe.")

    except:
        print(f"Bucket '{bucket_name}' não encontrado. Criando...")
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' criado com sucesso.")


# Garantir que o bucket existe
verifica_cria_bucket(bucket_name)


# Baixa o arquivo do MinIO
s3.download_file(bucket_name, object_name, "temp/dados_brutos.csv")


# Calcula o hash do arquivo original
hash_original = calcula_hash("temp/dados_brutos.csv")


# Carrega o dataset
df = pd.read_csv("temp/dados_brutos.csv")


# Registra informações de linhagem
linhagem = {
    "timestamp": datetime.utcnow().isoformat(),
    "arquivo_origem": object_name,
    "hash_origem": hash_original,
    "transformacoes_aplicadas": "Nenhuma - dados brutos carregados",
    "arquivo_destino": "processed-data/dados_brutos.csv"
}


# Salva informações de linhagem localmente
with open("temp/linhagem.json", "w") as f:
    json.dump(linhagem, f, indent=4)


# Enviar informações de linhagem para o MinIO
s3.upload_file(
    "temp/linhagem.json",
    bucket_name,
    "linhagem/linhagem_inicial.json"
)


print("\nLog - Arquivo inicial de linhagem dos dados enviado para o Data Lake.\n")