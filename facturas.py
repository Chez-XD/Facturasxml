import xmltodict
import pandas as pd
from pathlib import Path
import sys
import os

# --- CONFIGURACIÓN ---
RUTA_XMLS = '/home/chez/Facturas' 
NOMBRE_EXCEL = '/mnt/d/amart/Chrome/papeles/Facturas/Noviembre 2025/Reporte_Facturas_CFDI.xlsx'
# ---------------------

def obtener_datos_cfdi_diccionario(ruta_xml):
    """
    Extrae los datos del XML (Misma función robusta que ya teníamos).
    """
    try:
        with open(ruta_xml, 'r', encoding='utf-8') as f:
            xml_string = f.read()

        data_dict = xmltodict.parse(xml_string, process_namespaces=True, namespace_separator='|')
        
        comprobante = None
        namespace_prefix = ''
        
        for key in data_dict.keys():
            if key.endswith('Comprobante'):
                comprobante = data_dict[key]
                namespace_prefix = key.replace('Comprobante', '') 
                break
        
        if not comprobante:
            raise ValueError("No se encontró el nodo principal 'Comprobante'.")

        # Extracción segura de datos
        
        # --- LÓGICA DE SEPARACIÓN DE FECHA Y HORA (CORREGIDA) ---
        fecha_completa_iso = comprobante.get('@Fecha', 'N/A')
        
        fecha = 'N/A'
        hora = 'N/A'
        
        if fecha_completa_iso != 'N/A' and 'T' in fecha_completa_iso:
            # Divide la cadena en la letra 'T'
            partes = fecha_completa_iso.split('T')
            fecha = partes[0]  # Ejemplo: '2025-11-27'
            hora = partes[1]   # Ejemplo: '20:15:41'
        # ---------------------------------------------------------
            
        total = comprobante.get('@Total', 'N/A')
        subtotal = comprobante.get('@SubTotal', 'N/A')
        forma_pago = comprobante.get('@FormaPago', 'N/A')
        metodo_pago = comprobante.get('@MetodoPago', 'N/A')
        folio = comprobante.get('@Folio', 'N/A')
        serie = comprobante.get('@Serie', 'N/A')
        
        emisor = comprobante.get(namespace_prefix + 'Emisor')
        receptor = comprobante.get(namespace_prefix + 'Receptor')
        complemento_dict = comprobante.get(namespace_prefix + 'Complemento')
        impuestos_totales = comprobante.get(namespace_prefix + 'Impuestos', {})

        emisor_rfc = emisor.get('@Rfc', 'N/A') if emisor else 'N/A'
        emisor_nombre = emisor.get('@Nombre', 'N/A') if emisor else 'N/A'
        receptor_rfc = receptor.get('@Rfc', 'N/A') if receptor else 'N/A'
        uso_cfdi = receptor.get('@UsoCFDI', 'N/A') if receptor else 'N/A'
        
        uuid = 'N/A'
        if complemento_dict:
            timbre = complemento_dict.get('tfd|TimbreFiscalDigital')
            if not timbre:
                for k in complemento_dict.keys():
                    if k.endswith('TimbreFiscalDigital'):
                        timbre = complemento_dict[k]
                        break
            if timbre:
                uuid = timbre.get('@UUID', 'N/A')

        total_retenciones = impuestos_totales.get('@TotalImpuestosRetenidos', '0.00')
        total_traslados = impuestos_totales.get('@TotalImpuestosTrasladados', '0.00')

        # --- DICCIONARIO DE RETORNO (CON FECHA Y HORA SEPARADAS) ---
        return {
            'UUID': uuid, 
            'Fecha': fecha,        # Columna de fecha (2025-11-27)
            'Hora': hora,          # Columna de hora (20:15:41)
            'RFC Emisor': emisor_rfc, 
            'Nombre Emisor': emisor_nombre, 
            'RFC Receptor': receptor_rfc,
            'Uso CFDI': uso_cfdi,
            'SubTotal': float(subtotal) if subtotal != 'N/A' else 0.0,
            'Total Trasladado': float(total_traslados) if total_traslados != 'N/A' else 0.0,
            'Total Retenido': float(total_retenciones) if total_retenciones != 'N/A' else 0.0,
            'Total': float(total) if total != 'N/A' else 0.0,
            'Forma Pago': forma_pago,
            'Metodo Pago': metodo_pago,
            'Folio': folio,
            'Serie': serie,
            'Archivo XML': ruta_xml.name
        }

    except Exception as e:
        print(f"⚠️ Error en {ruta_xml.name}: {e}", file=sys.stderr)
        return {'UUID': 'ERROR', 'Fecha': f"Error: {e}", 'Archivo XML': ruta_xml.name}


def procesar_archivos_incremental(directorio_xmls, nombre_excel):
    """
    Carga el Excel existente, verifica UUIDs y agrega solo las filas nuevas.
    """
    ruta_directorio = Path(directorio_xmls)
    ruta_excel = Path(nombre_excel)
    
    print(f"--- PROCESO INCREMENTAL ---")

    # 1. Cargar Excel Existente para obtener UUIDs
    uuids_existentes = set()
    df_existente = pd.DataFrame()
    archivo_existe = ruta_excel.exists()

    if archivo_existe:
        print(f"📂 Leyendo archivo Excel existente: {nombre_excel}")
        try:
            df_existente = pd.read_excel(nombre_excel)
            # Verificar si existe la columna UUID
            if 'UUID' in df_existente.columns:
                # Convertimos a string y guardamos en un 'set' para búsqueda rápida
                uuids_existentes = set(df_existente['UUID'].astype(str).dropna())
                print(f"📊 Se encontraron {len(uuids_existentes)} facturas ya registradas.")
            else:
                print("⚠️ El archivo existe pero no tiene columna UUID. Se procesará como nuevo.")
        except Exception as e:
            print(f"❌ Error al leer Excel existente: {e}. Se intentará crear uno nuevo.")
            archivo_existe = False
    else:
        print(f"🆕 El archivo Excel no existe. Se creará uno nuevo.")

    # 2. Buscar y Procesar XMLs
    if not ruta_directorio.is_dir():
        print(f"❌ ERROR: El directorio '{directorio_xmls}' no existe.")
        return

    archivos_encontrados = sorted(list(ruta_directorio.glob('*.xml')))
    if not archivos_encontrados:
        print("⚠️ No hay XMLs en la carpeta.")
        return

    nuevos_registros = []
    print(f"🔎 Analizando {len(archivos_encontrados)} archivos XML...")

    contador_agregados = 0
    contador_omitidos = 0

    for archivo_xml in archivos_encontrados:
        datos = obtener_datos_cfdi_diccionario(archivo_xml)
        
        uuid_actual = str(datos.get('UUID', 'N/A'))

        # --- LÓGICA DE VALIDACIÓN ---
        if uuid_actual in uuids_existentes:
            contador_omitidos += 1
        elif uuid_actual == 'ERROR' or uuid_actual == 'N/A':
            # Agregar el error para que se vea en el Excel
            nuevos_registros.append(datos)
            contador_agregados += 1
        else:
            # Es nuevo y válido
            nuevos_registros.append(datos)
            uuids_existentes.add(uuid_actual) 
            contador_agregados += 1
            print(f"   [NUEVO] {archivo_xml.name}")

    # 3. Guardar resultados
    print(f"\nResumen:")
    print(f"   ✔ Existentes (Omitidos): {contador_omitidos}")
    print(f"   Nuevos (Para agregar): {contador_agregados}")

    if nuevos_registros:
        df_nuevos = pd.DataFrame(nuevos_registros)
        
        # Concatenar (Unir) los existentes con los nuevos
        if not df_existente.empty:
            df_final = pd.concat([df_existente, df_nuevos], ignore_index=True)
        else:
            df_final = df_nuevos

        print(f"💾 Guardando Excel actualizado...")
        df_final.to_excel(nombre_excel, index=False)
        print(f"✅ ¡Hecho! Archivo actualizado: {ruta_excel.resolve()}")
    else:
        print("✅ No hubo datos nuevos que agregar. El Excel se mantiene igual.")

# --- EJECUCIÓN ---
if __name__ == "__main__":
    procesar_archivos_incremental(RUTA_XMLS, NOMBRE_EXCEL)