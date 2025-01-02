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
    """Inserta datos en lotes y devuelve el número de registros realmente añadidos."""
    if data:
        try:
            execute_values(cursor, query, data)
            # `cursor.rowcount` devuelve el número de registros afectados por la última consulta
            return cursor.rowcount
        except Exception as e:
            print(f"Error al insertar en {table_name}: {e}")
            return 0
    return 0


def process_json_and_insert_to_db(json_dir, dsn):
    try:
        # Localizar archivos JSON
        print(f"Buscando archivos JSON en {json_dir}...")
        json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]
        if not json_files:
            print("No se encontró ningún archivo JSON en el directorio extraído.")
            return

        input_file = os.path.join(json_dir, json_files[0])
        print(f"Procesando archivo JSON: {input_file}")

        # Cargar datos JSON
        with open(input_file, encoding='utf-8') as file:
            data = json.load(file)
            print(f"Archivo JSON cargado correctamente.")

        records = data.get('records', [])
        print(f"Se encontraron {len(records)} registros para procesar.")

        # Conexión a la base de datos
        print("Conectando a la base de datos...")
        conn = psycopg2.connect(dsn)
        cursor = conn.cursor()
        print("Conexión exitosa.")

        # Inicializar listas para inserciones en lotes
        compiled_releases_data = []
        parties_data = []
        buyers_data = []
        tenders_data = []
        items_data = []
        documents_data = []
        tenderers_data = []
        planning_data = []

        for idx, record in enumerate(records):
            if idx % 500 == 0:
                print(f"Procesando registro {idx + 1}/{len(records)}...")

            cr = record.get('compiledRelease', {})
            ocid = cr.get('ocid')

            if ocid:
                compiled_releases_data.append((
                    ocid,
                    cr.get('id'),
                    cr.get('date'),
                    cr.get('publishedDate'),
                    cr.get('initiationType'),
                ))

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

        # Inicializar contadores
        insert_counts = {
            "compiled_releases": 0,
            "parties": 0,
            "buyers": 0,
            "tenders": 0,
            "items": 0,
            "documents": 0,
            "tenderers": 0,
            "planning": 0,
        }

        # Inserciones en la base de datos
        print("Insertando datos en la base de datos...")
        insert_counts["compiled_releases"] += insert_data_batch(cursor, """
            INSERT INTO compiled_releases (id, ocid, date, published_date, initiation_type)
            VALUES %s ON CONFLICT (id) DO NOTHING;
        """, compiled_releases_data, "compiled_releases")

        insert_counts["parties"] += insert_data_batch(cursor, """
            INSERT INTO parties (id, name, identifier_scheme, identifier_id, legal_name, street_address, locality, region, department, country_name, roles, date_published)
            VALUES %s ON CONFLICT (id) DO NOTHING;
        """, parties_data, "parties")

        # Commit final
        conn.commit()

        # Mostrar el resumen de inserciones reales
        print("Resumen de inserciones reales:")
        for table, count in insert_counts.items():
            print(f"- {table}: {count} nuevas entradas")

    except Exception as e:
        print(f"Error durante la ejecución: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    DOWNLOAD_URL = 'https://contratacionesabiertas.osce.gob.pe/api/v1/file/seace_v3/json/2025/01/'
    DOWNLOAD_DIR = 'ctc_dwn'
    EXTRACT_DIR = 'extracted_files'
    DSN = "postgresql://neondb_owner:VbdvNRPr2au7@ep-shrill-wind-a43e78up.us-east-1.aws.neon.tech/neondb?sslmode=require"

    download_and_extract(DOWNLOAD_URL, DOWNLOAD_DIR, EXTRACT_DIR)
    process_json_and_insert_to_db(EXTRACT_DIR, DSN)