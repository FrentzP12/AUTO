import os
import requests
from pyunpack import Archive
import json
import psycopg2
from psycopg2.extras import execute_values

def download_and_extract(download_url, download_dir, extract_dir):
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    rar_filename = "latest_downloaded.rar"
    rar_filepath = os.path.join(download_dir, rar_filename)

    try:
        print(f"Descargando desde {download_url}...")
        response = requests.get(download_url, stream=True)
        response.raise_for_status()

        with open(rar_filepath, 'wb') as rar_file:
            for chunk in response.iter_content(chunk_size=8192):
                rar_file.write(chunk)

        print(f"Archivo descargado exitosamente en {rar_filepath}")
        print(f"Extrayendo contenido en {extract_dir}...")
        Archive(rar_filepath).extractall(extract_dir)
        print(f"Extracción completada en {extract_dir}")

    except Exception as e:
        print(f"Error durante la descarga o extracción: {e}")


def insert_data_batch(cursor, query, data, table_name):
    if data:
        try:
            cursor.execute("SAVEPOINT before_insert")
            execute_values(cursor, query, data)
            inserted_count = cursor.rowcount
            print(f"{inserted_count} registros nuevos insertados en {table_name}.")
            cursor.execute("RELEASE SAVEPOINT before_insert")
        except Exception as e:
            print(f"Error al insertar en {table_name}: {e}")
            cursor.execute("ROLLBACK TO SAVEPOINT before_insert")


def process_json_and_insert_to_db(json_dir, dsn):
    json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]
    if not json_files:
        print("No se encontró ningún archivo JSON en el directorio extraído.")
        return

    input_file = os.path.join(json_dir, json_files[0])
    print(f"Procesando archivo JSON: {input_file}")

    with open(input_file, encoding='utf-8') as file:
        data = json.load(file)

    records = data.get('records', [])
    print(f"Se encontraron {len(records)} registros para procesar.")

    try:
        conn = psycopg2.connect(dsn)
        cursor = conn.cursor()

        compiled_releases_data = []
        parties_data = []
        buyers_data = []
        tenders_data = []
        items_data = []
        documents_data = []
        tenderers_data = []
        planning_data = []

        for record in records:
            cr = record.get('compiledRelease', {})
            ocid = cr.get('ocid')

            # compiled_releases
            if ocid:
                compiled_releases_data.append((
                    ocid,
                    cr.get('id'),
                    cr.get('date'),
                    cr.get('publishedDate'),
                    cr.get('initiationType'),
                ))

            # parties
            for party in cr.get('parties', []):
                parties_data.append((
                    party.get('id'),
                    party.get('name'),
                    party.get('identifier', {}).get('scheme'),
                    party.get('identifier', {}).get('id'),
                    party.get('identifier', {}).get('legalName'),
                    party.get('address', {}).get('streetAddress'),
                    party.get('address', {}).get('locality'),
                    party.get('address', {}).get('region'),
                    party.get('address', {}).get('department'),
                    party.get('address', {}).get('countryName'),
                    ", ".join(party.get('roles', [])),
                    cr.get('tender', {}).get('datePublished'),
                ))

            # buyers
            buyer = cr.get('buyer', {})
            buyers_data.append((
                buyer.get('id'),
                buyer.get('name'),
            ))

            # tenders
            tender = cr.get('tender', {})
            tenders_data.append((
                tender.get('id'),
                ocid,
                buyer.get('id'),
                tender.get('title'),
                tender.get('description'),
                tender.get('procurementMethod', 'unknown'),
                tender.get('procurementMethodDetails'),
                tender.get('mainProcurementCategory'),
                tender.get('numberOfTenderers', 0),
                tender.get('value', {}).get('currency', 'PEN'),
                tender.get('value', {}).get('amount', 0.0),
                tender.get('datePublished'),
            ))

            # items
            for item in tender.get('items', []):
                items_data.append((
                    item.get('id'),
                    tender.get('id'),   
                    item.get('description'),
                    item.get('status'),
                    item.get('classification', {}).get('id'),
                    item.get('classification', {}).get('description'),
                    item.get('quantity', 0.0),
                    item.get('unit', {}).get('id'),
                    item.get('unit', {}).get('name'),
                    item.get('totalValue', {}).get('amount', 0.0),
                ))

            # documents
            for document in tender.get('documents', []):
                documents_data.append((
                    document.get('id'),
                    tender.get('id'),
                    document.get('url'),
                    document.get('datePublished'),
                    document.get('format'),
                    document.get('documentType'),
                    document.get('title'),
                    document.get('language'),
                ))

            # tenderers
            for tenderer in tender.get('tenderers', []):
                tenderers_data.append((
                    tenderer.get('id'),
                    tender.get('id'),
                    tenderer.get('name'),
                ))

            # planning
            planning_data.append((
                ocid,
                cr.get('planning', {}).get('budget', {}).get('description'),
            ))

        # Inserciones en lotes
        insert_data_batch(cursor, """
            INSERT INTO compiled_releases (id, ocid, date, published_date, initiation_type)
            VALUES %s ON CONFLICT (id) DO NOTHING;
        """, compiled_releases_data, "compiled_releases")

        insert_data_batch(cursor, """
            INSERT INTO parties (id, name, identifier_scheme, identifier_id, legal_name, street_address, locality, region, department, country_name, roles, date_published)
            VALUES %s ON CONFLICT (id) DO NOTHING;
        """, parties_data, "parties")

        insert_data_batch(cursor, """
            INSERT INTO buyers (id, name)
            VALUES %s ON CONFLICT (id) DO NOTHING;
        """, buyers_data, "buyers")

        insert_data_batch(cursor, """
            INSERT INTO tenders (id, compiled_release_id, buyer_id, title, description, procurement_method, procurement_method_details, main_procurement_category, number_of_tenderers, currency, value_amount, date_published)
            VALUES %s ON CONFLICT (id) DO NOTHING;
        """, tenders_data, "tenders")

        insert_data_batch(cursor, """
            INSERT INTO items (id, tender_id, description, status, classification_id, classification_description, quantity, unit_id, unit_name, total_value_amount)
            VALUES %s ON CONFLICT (id) DO NOTHING;
        """, items_data, "items")

        insert_data_batch(cursor, """
            INSERT INTO documents (id, tender_id, url, date_published, format, document_type, title, language)
            VALUES %s ON CONFLICT (id) DO NOTHING;
        """, documents_data, "documents")

        insert_data_batch(cursor, """
            INSERT INTO tenderers (id, tender_id, name)
            VALUES %s ON CONFLICT (id, tender_id) DO NOTHING;
        """, tenderers_data, "tenderers")

        insert_data_batch(cursor, """
            INSERT INTO planning (compiled_release_id, budget_description)
            VALUES %s ON CONFLICT (compiled_release_id) DO NOTHING;
        """, planning_data, "planning")

        conn.commit()
        print("Sincronización completada exitosamente.")

    except Exception as e:
        print(f"Error durante la sincronización: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


# ... (funciones anteriores se mantienen igual)

if __name__ == "__main__":
    import datetime
    today = datetime.date.today()
    current_year = today.year
    current_month = today.month
    months_to_process = []

    # Determinar si se procesa el mes anterior
    if today.day <= 10:
        first_day_current = today.replace(day=1)
        last_day_previous = first_day_current - datetime.timedelta(days=1)
        previous_year = last_day_previous.year
        previous_month = last_day_previous.month
        months_to_process.append((previous_year, previous_month))
    
    # Siempre procesar el mes actual
    months_to_process.append((current_year, current_month))

    # Eliminar duplicados (por si el día 1 cae en el cálculo)
    unique_months = list(set(months_to_process))
    unique_months.sort()  # Ordenar cronológicamente

    DSN = os.getenv("DSN")
    if not DSN:
        raise ValueError("Error: No se encontró la variable de entorno DSN")
    
    for year, month in unique_months:
        formatted_month = f"{month:02d}"
        download_url = f'https://contratacionesabiertas.osce.gob.pe/api/v1/file/seace_v3/json/{year}/{formatted_month}'
        download_dir = os.path.join('ctc_dwn', f"{year}_{formatted_month}")
        extract_dir = os.path.join('extracted_files', f"{year}_{formatted_month}")
        
        print(f"\nProcesando: {year}-{formatted_month}")
        download_and_extract(download_url, download_dir, extract_dir)
        process_json_and_insert_to_db(extract_dir, DSN)