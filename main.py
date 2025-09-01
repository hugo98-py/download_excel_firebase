# -*- coding: utf-8 -*-
"""
Created on Wed Jun  4 21:17:50 2025
Modified to return a download URL and export only la colección "registro" con columnas ordenadas y renombradas.
Ahora también agrega una segunda hoja "ListadoForestal" con los datos de la colección "forestal" filtrada por campanaID.
"""

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from firebase_admin import firestore, credentials, initialize_app
import pandas as pd
import firebase_admin
from datetime import datetime
import os
import re
import base64

app = FastAPI()

# ⚙️ Configuración CORS
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # para pruebas (sin credenciales)
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# 📁 Directorio donde se guardarán los excels
download_folder = "downloads"
os.makedirs(download_folder, exist_ok=True)

# Montamos la carpeta como estática
app.mount("/downloads", StaticFiles(directory=download_folder), name="downloads")

# 🔐 Cargar clave de Firebase desde variable de entorno
FIREBASE_KEY_B64 = os.getenv("FIREBASE_KEY_B64")
if not FIREBASE_KEY_B64:
    raise RuntimeError("❌ No se encontró la variable de entorno FIREBASE_KEY_B64.")

# 📝 Escribir archivo temporal con las credenciales
with open("firebase_key.json", "wb") as f:
    f.write(base64.b64decode(FIREBASE_KEY_B64))

# ✅ Inicializar Firebase una sola vez
if not firebase_admin._apps:
    cred = credentials.Certificate("firebase_key.json")
    initialize_app(cred)

db = firestore.client()

# 🔧 Limpia zonas horarias de los datos
def clean_data(data):
    for key, value in data.items():
        if isinstance(value, datetime) and value.tzinfo is not None:
            data[key] = value.replace(tzinfo=None)
    return data

# 📑 Configuración de columnas para la hoja "registro"
COLUMN_ORDER = [
    'nameCamp', 'createdByEmailCamp', 'startDateCamp', 'endDateCamp',
    'createdByCamp', 'createdAtCamp', 'accesListCamp',
    'nameEst', 'tamanoEst', 'exposicionEst', 'pendienteEst',
    'otraCoberturaEst', 'cobertura1Est', 'cobertura2Est', 'comentarioEst',
    'type', 'registroAnoDate', 'registrosMesDate', 'registrosDiaDate',
    'registrosHoraDate', 'registroDate', 'date', 'nInd', 'protocoloMuestreo',
    'tipoDeComponente','estadoDelOrganismo', 'tipoDeRegistro', 'unidadDeLaMuestra', 'unidadDeValor',
    'Reino', 'division', 'clase', 'familia',
    'genero', 'nameSp', 'habito',
    'cobertura', 'comentarios', 'parametro', 'tipoCuantificacion',
    'estadosFenologicos', 'estadosFitosanitarios', 'agrupacionesForestales',
    'campanaID', 'estacionID', 'registroID'
]

COLUMN_RENAME = {
    'nameCamp': 'Nombre de la Campaña',
    'createdByEmailCamp': 'Responsable (Email)',
    'startDateCamp': 'Fecha de inicio de la campaña',
    'endDateCamp': 'Fecha de término de la campaña',
    'createdByCamp': 'Responsable',
    'createdAtCamp': 'Fecha de creación de la campaña',
    'accesListCamp': 'Lista de compartidos (Emails)',
    'nameEst': 'Nombre de la estación',
    'tamanoEst': 'Tamaño de la estación',
    'exposicionEst': 'Exposición de la estación',
    'pendienteEst': 'Pendiente de la estación',
    'otraCoberturaEst': 'Otra cobertura estación',
    'cobertura1Est': 'Cobertura 1 estación',
    'cobertura2Est': 'Cobertura 2 estación',
    'comentarioEst': 'Comentarios de la estación',
    'type': 'Tipo (Flora o Forestal)',
    'registroAnoDate': 'Año del registro',
    'registrosMesDate': 'Mes del registro',
    'registrosDiaDate': 'Día del registro',
    'registrosHoraDate': 'Hora del registro',
    'registroDate': 'Fecha del registro',
    'date': 'Fecha',
    'nInd': 'Número de individuos',
    'protocoloMuestreo': 'Protocolo de muestreo',
    'tipoDeComponente': 'Tipo de componente',
    'estadoDelOrganismo': 'Estado del organismo',
    'tipoDeRegistro': 'Tipo de registro',
    'unidadDeLaMuestra': 'Unidad de la muestra',
    'unidadDeValor': 'Unidad de valor',
    'Reino': 'Reino',
    'division': 'División',
    'clase': 'Clase',
    'familia': 'Familia',
    'genero': 'Género',
    'nameSp': 'Nombre especie',
    'habito': 'Hábito',
    'cobertura': 'Cobertura',
    'comentarios': 'Comentarios registro',
    'parametro': 'Parámetro',
    'tipoCuantificacion': 'Tipo de cuantificación',
    'estadosFenologicos': 'Estados fenológicos',
    'estadosFitosanitarios': 'Estados fitosanitarios',
    'agrupacionesForestales': 'Agrupaciones forestales',
    'campanaID': 'CampañaID',
    'estacionID': 'EstaciónID',
    'registroID': 'RegistroID'
}

# 🔍 Consulta la colección "registro" filtrando por campanaID
def get_registro_df(campana_id: str) -> pd.DataFrame:
    docs = db.collection("registro").where("campanaID", "==", campana_id).stream()
    data = [clean_data(doc.to_dict() | {"id": doc.id}) for doc in docs]
    return pd.DataFrame(data)

# 🔍 NUEVO: Consulta la colección "forestal" filtrando por campanaID
# (Se exporta tal cual viene; no se reordenan ni renombran columnas)
def get_forestal_df(campana_id: str) -> pd.DataFrame:
    docs = db.collection("forestal").where("campanaID", "==", campana_id).stream()
    data = [clean_data(doc.to_dict()) for doc in docs]   # sin agregar campos extra
    return pd.DataFrame(data)

# 🧼 Sanitiza el nombre del archivo
def sanitize_filename(name: str) -> str:
    return re.sub(r'[^\w\-]+', '-', name)

# 📤 Endpoint principal
@app.get("/export")
def export_registro(
    request: Request,
    campana_id: str = Query(..., description="ID de la campaña a filtrar")
):
    safe_campana_id = sanitize_filename(campana_id)
    output_filename = f"export_registro_{safe_campana_id}.xlsx"
    output_path = os.path.join(download_folder, output_filename)

    # Obtener DataFrames
    df = get_registro_df(campana_id)
    if df.empty:
        # Se mantiene la lógica original: si no hay "registro", 404
        raise HTTPException(status_code=404, detail="No se encontraron registros para la campaña dada.")

    df_forestal = get_forestal_df(campana_id)  # puede venir vacío

    # Reordenar y renombrar columnas de "registro" (igual que antes)
    df = df.reindex(columns=COLUMN_ORDER)
    df = df.rename(columns=COLUMN_RENAME)

    # Modificar campos especiales segun requerimiento (solo en hoja "registro")
    cols = ['Fecha de inicio de la campaña', 'Fecha de término de la campaña']
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c]).dt.date

    # Generar el Excel con la hoja "registro" y la nueva hoja "ListadoForestal"
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name="registro", index=False)

        # Hoja "ListadoForestal": si df_forestal está vacío, igual se crea hoja vacía
        if df_forestal.empty:
            # Crear una hoja vacía explícitamente
            pd.DataFrame().to_excel(writer, sheet_name="ListadoForestal", index=False)
        else:
            df_forestal.to_excel(writer, sheet_name="ListadoForestal", index=False)

    # Construir URL de descarga
    base_url = str(request.base_url).rstrip("/")
    download_url = f"{base_url}/downloads/{output_filename}"

    return JSONResponse({"download_url": download_url})




