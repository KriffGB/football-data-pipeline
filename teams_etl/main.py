import os
import requests
import pandas as pd
from google.cloud import bigquery

# Esta es la función principal que ejecutará Google
def actualizar_posiciones(request):
    print("1. Iniciando extracción desde RapidAPI...")
    
    # --- RELLENA CON TUS DATOS EXACTOS ---
    API_KEY = "TU_API_KEY_AQUI"
    PROJECT_ID = "TU_PROYECTO_GCP_AQUI"
    SEASON_ID = "76986"
    # -------------------------------------
    
    # URL del endpoint de standings
    url = "https://sofascore.p.rapidapi.com/tournaments/get-standings"
    
    querystring = {"tournamentId": "17", "seasonId": SEASON_ID, "type": "total"}
    
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "sofascore.p.rapidapi.com"
    }
    
    # Extracción (E)
    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()
    
    print("2. Transformando datos con Pandas...")
    equipos_list = data["standings"][0]["rows"]
    filas_limpias = []
    
    for fila in equipos_list:
        equipo_info = fila["team"]
        dict_limpio = {
            "team_id": equipo_info["id"],
            "tournament_id": 17,
            "name": equipo_info["name"],
            "slug": equipo_info["slug"],
            "position": fila["position"],
            "matches": fila["matches"],
            "wins": fila["wins"],
            "draws": fila["draws"],
            "losses": fila["losses"],
            "goals_for": fila["scoresFor"],
            "goals_against": fila["scoresAgainst"],
            "points": fila["points"]
        }
        filas_limpias.append(dict_limpio)
        
    df = pd.DataFrame(filas_limpias)
    
    print("3. Cargando a BigQuery...")
    table_id = f"{PROJECT_ID}.football_data.premier_league_teams"
    cliente_bq = bigquery.Client()
    
    # Sobrescribimos la tabla para tener siempre la foto actual
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    cliente_bq.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    
    print("¡Pipeline ETL ejecutado con éxito en la nube!")
    
    # Cloud Functions requiere devolver una respuesta HTTP
    return "OK", 200