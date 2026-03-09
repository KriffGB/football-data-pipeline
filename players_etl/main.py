import time
import requests
import pandas as pd
from google.cloud import bigquery

def actualizar_estadisticas_jugadores(request):
    print("1. Iniciando extracción de estadísticas de jugadores...")
    
    API_KEY = "TU_API_KEY_AQUI"
    PROJECT_ID = "TU_PROYECTO_GCP_AQUI"
    
    ligas_temporadas = {
        "17": "76986", # Premier League
        "18": "77347", # Championship
        "24": "77352", # League One
        "25": "77351"  # League Two
    }
    
    filas_limpias = []
    url = "https://sofascore.p.rapidapi.com/tournaments/get-player-statistics"
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "sofascore.p.rapidapi.com"
    }
    
    for torneo_id, season_id in ligas_temporadas.items():
        print(f"-> Procesando jugadores del torneo ID: {torneo_id}...")
        
        # Empezamos en la página 0
        offset = 0 
        
        # Bucle infinito que se detendrá cuando la liga se quede sin jugadores
        while True: 
            querystring = {
                "tournamentId": torneo_id, 
                "seasonId": season_id, 
                "limit": "100",  # Pedimos de 100 en 100
                "offset": str(offset), # Le decimos a la API qué "página" queremos
                "order": "-rating", # Lo dejamos como le gusta a la API
                "accumulation": "total", 
                "group": "summary"
            }
            
            response = requests.get(url, headers=headers, params=querystring)
            
            if response.status_code == 200:
                data = response.json()
                resultados = data.get("results", [])
                
                # Si la página viene vacía, significa que ya sacamos a todos los jugadores de esta liga
                if not resultados:
                    print(f"   - ¡Todos los jugadores de la liga {torneo_id} extraídos!")
                    break
                    
                print(f"   - Descargando página (offset: {offset})...")
                
                for item in resultados:
                    jugador = item.get("player", {})
                    equipo = item.get("team", {})
                    
                    dict_limpio = {
                        "player_id": jugador.get("id"),
                        "team_id": equipo.get("id"),
                        "tournament_id": int(torneo_id),
                        "name": jugador.get("name"),
                        "goals": item.get("goals", 0),
                        "expected_goals": item.get("expectedGoals", 0.0),
                        "assists": item.get("assists", 0),
                        "successful_dribbles": item.get("successfulDribbles", 0),
                        "tackles": item.get("tackles", 0),
                        "pass_percentage": item.get("accuratePassesPercentage", 0.0),
                        "rating": item.get("rating", 0.0)
                    }
                    filas_limpias.append(dict_limpio)
                
                # Preparamos la siguiente página sumando 100
                offset += 100
                time.sleep(1.5) # Pausa obligatoria para no saturar RapidAPI
            else:
                print(f"Error HTTP {response.status_code} en torneo {torneo_id}")
                break
    df = pd.DataFrame(filas_limpias)
    print(f"Total de jugadores procesados: {len(df)}")
    
    print("3. Cargando a BigQuery...")
    table_id = f"{PROJECT_ID}.football_data.england_all_leagues_players"
    cliente_bq = bigquery.Client()
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    cliente_bq.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    
    return "OK", 200