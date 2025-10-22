import pandas as pd
import json

def transaction_generator(csv_files, required_columns, chunk_size=10000):
    """
    Generador que lee transacciones de múltiples archivos CSV en trozos (chunks)
    y produce (yields) cada transacción como un diccionario de Python estructurado.

    Args:
        csv_files (list): Lista de rutas a los archivos CSV.
        required_columns (list): Columnas a extraer de los CSV.
        chunk_size (int): Número de filas a leer por trozo para optimizar memoria.

    Yields:
        dict: Un diccionario representando el documento JSON de una transacción.
    """
    # Iterar sobre la lista de archivos CSV
    for file_path in csv_files:
        try:
            # Procesar cada archivo en trozos para no cargar todo en memoria
            for chunk in pd.read_csv(file_path, usecols=required_columns, chunksize=chunk_size):
                # Iterar sobre cada fila del trozo
                for _, row in chunk.iterrows():
                    # Crear el diccionario con la estructura anidada
                    transaction_document = {
                        "transaction_id": row['trans_num'],
                        "customer_id": str(row['cc_num']),
                        "transaction_details": {
                            "timestamp_utc": row['trans_date_trans_time'],
                            "unix_timestamp": row['unix_time'],
                            "amount": row['amt'],
                            "currency": "USD"
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
                    # Producir el documento en lugar de imprimirlo
                    yield transaction_document
        except FileNotFoundError:
            print(f"Advertencia: El archivo '{file_path}' no fue encontrado. Omitiendo.")
        except Exception as e:
            print(f"Ocurrió un error al procesar el archivo '{file_path}': {e}")

def main():
    """
    Función principal que orquesta la lectura, generación y guardado de los datos.
    """
    csv_files = [
        'fraudTrain_part_01.csv',
        'fraudTrain_part_02.csv',
        'fraudTrain_part_03.csv',
        'fraudTrain_part_04.csv'
    ]

    required_columns = [
        'trans_num', 'cc_num', 'trans_date_trans_time', 'unix_time', 'amt',
        'merchant', 'category', 'merch_lat', 'merch_long'
    ]

    output_file = 'transactions.json'

    print("Iniciando la extracción y transformación de datos...")

    # Usar el generador para procesar las transacciones una por una
    # Esto crea una lista en memoria con todos los resultados.
    all_transactions = list(transaction_generator(csv_files, required_columns))

    print(f"Se procesaron {len(all_transactions)} transacciones.")

    # Guardar la lista completa de transacciones en un único archivo JSON
    try:
        with open(output_file, 'w') as f:
            json.dump(all_transactions, f, indent=4)
        print(f"Datos guardados exitosamente en '{output_file}'.")
    except Exception as e:
        print(f"Error al guardar el archivo JSON: {e}")

if __name__ == "__main__":
    main()