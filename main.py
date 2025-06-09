# -*- coding: utf-8 -*-
"""
Created on Wed Jun  4 21:17:50 2025
Modified to return a download URL
"""

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from firebase_admin import firestore
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from datetime import datetime
import os
import re
import base64

app = FastAPI()

# 📁 Directorio donde se guardarán los excels
DOWNLOAD_FOLDER = "downloads"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Montamos la carpeta como estática
app.mount("/downloads", StaticFiles(directory=DOWNLOAD_FOLDER), name="downloads")

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
    firebase_admin.initialize_app(cred)

db = firestore.client()

# 🔧 Limpia zonas horarias de los datos
def clean_data(data):
    for key, value in data.items():
        if isinstance(value, datetime) and value.tzinfo is not None:
            data[key] = value.replace(tzinfo=None)
    return data

# 🔍 Consulta una colección filtrando por campanaID
def get_collection_as_df(collection_name, campana_id):
    docs = db.collection(collection_name).where("campanaID", "==", campana_id).stream()
    data = [clean_data(doc.to_dict() | {"id": doc.id}) for doc in docs]
    return pd.DataFrame(data)

# 🧼 Sanitiza el nombre del archivo
def sanitize_filename(name: str) -> str:
    return re.sub(r'[^\w\-]+', '-', name)

# 📤 Endpoint principal
@app.get("/export")
def export_data(
    request: Request,
    campana_id: str = Query(..., description="ID de la campaña a filtrar")
):
    collection_names = ["registro", "campana", "estacion"]
    safe_campana_id = sanitize_filename(campana_id)
    output_filename = f"export_campana_{safe_campana_id}.xlsx"
    output_path = os.path.join(DOWNLOAD_FOLDER, output_filename)

    # Generar el Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for name in collection_names:
            df = get_collection_as_df(name, campana_id)
            if not df.empty:
                df.to_excel(writer, sheet_name=name, index=False)

    # Construir URL de descarga
    base_url = str(request.base_url).rstrip("/")  # elimina la barra final
    download_url = f"{base_url}/downloads/{output_filename}"

    return JSONResponse({"download_url": download_url})
