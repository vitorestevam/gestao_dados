import os

import requests
from pymongo import MongoClient


API_URL = "https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/servidores/salarios"
DEFAULT_YEAR = 2026
DEFAULT_MONTH = 1
DEFAULT_START_PAGE = 1


def fetch_page(session, year, month, page):
    params = {"year": year, "month": month, "page": page}
    response = session.get(
        API_URL,
        params=params,
        headers={"accept": "application/json"},
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()

    if "data" not in payload:
        raise ValueError("Resposta da API sem chave 'data'.")

    return payload


def insert_salaries(collection, salaries):
    if not salaries:
        return 0

    result = collection.insert_many(salaries)
    return len(result.inserted_ids)


def main():
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    db_name = "aula"
    collection_name = "aula_bigdata"

    year = int(os.getenv("SALARIOS_YEAR", str(DEFAULT_YEAR)))
    month = int(os.getenv("SALARIOS_MONTH", str(DEFAULT_MONTH)))
    start_page = int(os.getenv("SALARIOS_START_PAGE", str(DEFAULT_START_PAGE)))

    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    session = requests.Session()

    total_inserted = 0

    try:
        first_payload = fetch_page(session, year, month, start_page)
        summary = first_payload.get("sumary", {})
        total_pages = int(summary.get("total_pages", start_page))
        total_records = int(summary.get("total_records", 0))

        print(f"Competencia: {year}-{month:02d}")
        print(f"Total de paginas: {total_pages}")
        print(f"Total de registros na API: {total_records}")

        for page in range(start_page, total_pages + 1):
            payload = first_payload if page == start_page else fetch_page(session, year, month, page)
            salaries = payload.get("data", [])

            inserted = insert_salaries(collection, salaries)
            total_inserted += inserted

            print(f"Pagina {page}/{total_pages} | inseridos: {inserted}")

        print("\nResumo final:")
        print(f"Documentos inseridos: {total_inserted}")
    finally:
        client.close()
        session.close()


if __name__ == "__main__":
    main()
