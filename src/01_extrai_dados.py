# Lab 4 - Linhagem, Observabilidade, Qualidade, Enriquecimento e Governança de Dados no Data Lake

# Imports
import os
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient


def conectar_mongo() -> MongoClient:
    mongo_uri_env = os.getenv("MONGO_URI")
    uris = [mongo_uri_env] if mongo_uri_env else ["mongodb://mongo:27017", "mongodb://localhost:27017"]

    ultimo_erro = None
    for uri in uris:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            print(f"Conectado ao MongoDB em: {uri}")
            return client
        except Exception as erro:
            ultimo_erro = erro

    raise RuntimeError(
        "Não foi possível conectar ao MongoDB. Defina MONGO_URI se necessário."
    ) from ultimo_erro


def main() -> None:
    mongo_db = os.getenv("MONGO_DB", "aula")
    mongo_collection = os.getenv("MONGO_COLLECTION", "aula_bigdata")

    client = conectar_mongo()
    collection = client[mongo_db][mongo_collection]

    docs = list(collection.find({}, {"_id": 0}))
    if not docs:
        raise ValueError(
            f"Nenhum documento encontrado em {mongo_db}.{mongo_collection}."
        )

    df_dsa = pd.DataFrame(docs)

    # Mantém o schema esperado pelo restante do pipeline
    df_dsa = pd.DataFrame(
        {
            "id": df_dsa.get("id"),
            "nome": df_dsa.get("nome_servidor", np.nan),
            "idade": np.nan,
            "salario": pd.to_numeric(df_dsa.get("total_proventos"), errors="coerce"),
        }
    )

    output_dir = Path(__file__).resolve().parent / "temp"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "dados_brutos.csv"

    df_dsa.to_csv(output_path, index=False)

    print("\nDados brutos extraídos do MongoDB e salvos com sucesso.\n")


if __name__ == "__main__":
    main()
