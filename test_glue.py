import pandas as pd
from datetime import datetime
from src.s3 import S3Manager

def test_upload_with_glue():
    """
    Test para verificar la funcionalidad de carga con particionamiento.
    """
    # Crear un DataFrame de prueba con columna pa_date
    today = datetime.now().strftime('%Y-%m-%d')
    df = pd.DataFrame({
        'rut': ['12345678', '87654321'],
        'razon_social': ['Empresa A', 'Empresa B'],
        'pa_date': [today, today]
    })
    
    # Inicializar S3Manager
    s3_manager = S3Manager()
    
    # Intentar subir con particionamiento
    result = s3_manager.upload_processed(df)
    
    print(f"Resultado: {result}")
    print(f"DataFrame subido con {len(df)} registros")
    
if __name__ == "__main__":
    test_upload_with_glue()