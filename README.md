# Facturas XML CFDI Extractor Incremental

Este script de Python está diseñado para automatizar la extracción de datos clave de archivos CFDI (XML del SAT) y consolidarlos en un único reporte de Excel (`.xlsx`), evitando duplicados mediante la validación del UUID.

## Requisitos del Entorno

Este proyecto fue desarrollado y probado en el Subsystem for Linux (WSL) de Windows.

### 1. Librerías de Python

Para instalar todas las dependencias necesarias con sus versiones exactas, utiliza el archivo `requirements.txt`:

```bash
# Asegúrate de tener Python y pip instalados.
# Opcional: Crear y activar un entorno virtual
python3 -m venv venv
source venv/bin/activate

# Instalar todas las dependencias
pip install -r requirements.txt