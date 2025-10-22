
import pandas as pd
import json

def stream_transactions_as_json():
    """
    Lee transacciones de varios archivos CSV, las transforma a un formato JSON anidado
    y simula un flujo de datos imprimiendo cada transacción en la consola.
    """
    # Lista de archivos CSV a procesar
    csv_files = [
        'fraudTrain_part_01.csv',
        'fraudTrain_part_02.csv',
        'fraudTrain_part_03.csv',
        'fraudTrain_part_04.csv'
    ]

    # Columnas que se extraerán del CSV
    required_columns = [
        'trans_num', 'cc_num', 'trans_date_trans_time', 'unix_time', 'amt',
        'merchant', 'category', 'merch_lat', 'merch_long'
    ]

    print("Iniciando la simulación del flujo de datos de transacciones...")

    # Iterar sobre la lista de archivos CSV
    for file_path in csv_files:
        try:
            # Procesar cada archivo en trozos (chunks) para optimizar el uso de memoria
            chunk_size = 10000  # Ajusta este valor según la memoria disponible y el tamaño del archivo
            for chunk in pd.read_csv(file_path, usecols=required_columns, chunksize=chunk_size):
                # Iterar sobre cada fila del trozo
                for index, row in chunk.iterrows():
                    # Crear el documento JSON con la estructura anidada especificada
                    json_document = {
                        "transaction_id": row['trans_num'],
                        "customer_id": str(row['cc_num']),  # Convertir a string por si acaso
                        "transaction_details": {
                            "timestamp_utc": row['trans_date_trans_time'],
                            "unix_timestamp": row['unix_time'],
                            "amount": row['amt'],
                            "currency": "USD"  # Valor fijo como se especificó
                        },
                        "merchant_info": {
                            "name": row['merchant'],
                            "category": row['category'],
                            "location": {
                                "latitude": row['merch_lat'],
                                "longitude": row['merch_long']
                            }
                        }
                    }

                    # Imprimir el objeto JSON en la consola para simular el flujo de datos
                    print(json.dumps(json_document, indent=4))

        except FileNotFoundError:
            print(f"Error: El archivo '{file_path}' no fue encontrado.")
        except Exception as e:
            print(f"Ocurrió un error al procesar el archivo '{file_path}': {e}")

    print("Simulación del flujo de datos completada.")

if __name__ == "__main__":
    stream_transactions_as_json()
