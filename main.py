# -*- coding: utf-8 -*-
"""
Created on Wed Jun  4 21:17:50 2025

@author: Hugo
"""
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from firebase_admin import firestore
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from datetime import datetime
import os
import re 

# Inicializar Firebase una sola vez
cred = credentials.Certificate("firebase_key.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

app = FastAPI()

# 🔧 Función que elimina zonas horarias
def clean_data(data):
    for key, value in data.items():
        if isinstance(value, datetime) and value.tzinfo is not None:
            data[key] = value.replace(tzinfo=None)
    return data

# 🔧 Ahora recibe el filtro de campaña
def get_collection_as_df(collection_name, campana_id):
    print(f"[DEBUG] Consultando colección '{collection_name}' con campanaID: '{campana_id}'")
    docs = db.collection(collection_name).where("campanaID", "==", campana_id).stream()
    data = [clean_data(doc.to_dict() | {"id": doc.id}) for doc in docs]
    print(f"[DEBUG] Registros encontrados: {len(data)}")
    return pd.DataFrame(data)

def sanitize_filename(name: str) -> str:
    # Reemplaza cualquier carácter que no sea alfanumérico, guion o guion bajo por guion
    return re.sub(r'[^\w\-]+', '-', name)

@app.get("/export")
def export_data(campana_id: str = Query(..., description="ID de la campaña a filtrar")):
    collection_names = ["registro", "campana", "estacion"]
    
    # Sanitiza el campana_id para usarlo en el nombre del archivo
    safe_campana_id = sanitize_filename(campana_id)
    output_filename = f"export_campana_{safe_campana_id}.xlsx"

    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        for name in collection_names:
            df = get_collection_as_df(name, campana_id)
            df.to_excel(writer, sheet_name=name, index=False)

    return FileResponse(output_filename, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename=output_filename)

