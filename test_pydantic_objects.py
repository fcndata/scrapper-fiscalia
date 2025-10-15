#!/usr/bin/env python3
"""
Script de prueba para verificar la creación de objetos Pydantic
"""
import pandas as pd
from datetime import datetime
from src.models import EmpresaData, FuncionarioData, SufData

def test_empresa_creation():
    """Prueba creación de EmpresaData desde DataFrame"""
    print("=== Test EmpresaData ===")
    
    # Simular datos como los devuelve Athena
    data = {
        'rut_cliente': [12345678, 87654321],
        'rut_cliente_dv': ['9', 'K'],
        'plataforma': ['DIGITAL', 'PRESENCIAL'],
        'segmento': ['PYME', 'EMPRESA'],
        'ejec_cod': [100, 200],
        'fecha_proceso': ['2024-01-15 10:30:00', '2024-01-16 11:45:00']
    }
    
    df = pd.DataFrame(data)
    print(f"DataFrame creado: {len(df)} filas")
    print(f"Tipos de columnas:\n{df.dtypes}")
    
    # Probar creación de objetos
    empresas_list = []
    for _, row in df.iterrows():
        try:
            print(f"\nProcesando fila: {row.to_dict()}")
            empresa = EmpresaData(**row.to_dict())
            print(f"Objeto creado: {type(empresa)}")
            print(f"¿Tiene model_dump?: {hasattr(empresa, 'model_dump')}")
            
            if hasattr(empresa, 'model_dump'):
                print(f"model_dump(): {empresa.model_dump()}")
            
            
            empresa_python = empresa.model_dump()
            empresas_json = empresa.model_dump(mode="json")
            print(f'Objeto tipo python: {empresa_python.get("rut_cliente")}')
            print(f'Objeto tipo json: {empresas_json.get("rut_cliente")}')
            
            empresas_list.append(empresa)
            
        except Exception as e:
            print(f"ERROR: {e}")
            print(f"Tipo de error: {type(e)}")
    
    return empresas_list

def test_funcionario_creation():
    """Prueba creación de FuncionarioData desde DataFrame"""
    print("\n=== Test FuncionarioData ===")
    
    data = {
        'rut_funcionario': [11111111, 22222222],
        'rut_funcionario_dv': ['1', '2'],
        'nombre_funcionario': ['Juan Perez', 'Maria Lopez'],
        'nombre_puesto': ['Ejecutivo', 'Gerente'],
        'dependencia': ['Sucursal A', 'Sucursal B'],
        'correo': ['juan@banco.cl', 'maria@banco.cl'],
        'fecha_carga_dl': ['2024-01-15', '2024-01-16'],
        'ejc_cod': [100, 200]
    }
    
    df = pd.DataFrame(data)
    print(f"DataFrame creado: {len(df)} filas")
    
    funcionarios_list = []
    for _, row in df.iterrows():
        try:
            print(f"\nProcesando fila: {row.to_dict()}")
            funcionario = FuncionarioData(**row.to_dict())
            print(f"Objeto creado: {type(funcionario)}")
            print(f"¿Tiene model_dump?: {hasattr(funcionario, 'model_dump')}")
            
            if hasattr(funcionario, 'model_dump'):
                print(f"model_dump(): {funcionario.model_dump()}")
            
            funcionarios_list.append(funcionario)
            
        except Exception as e:
            print(f"ERROR: {e}")
            print(f"Tipo de error: {type(e)}")
    
    return funcionarios_list

if __name__ == "__main__":
    print("Iniciando pruebas de objetos Pydantic...")
    
    empresas = test_empresa_creation()
    funcionarios = test_funcionario_creation()
    
    print(f"\n=== Resumen ===")
    print(f"Empresas creadas: {len(empresas)}")
    print(f"Funcionarios creados: {len(funcionarios)}")