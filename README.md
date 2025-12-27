# Valorant API
How to run:
 1. Create your own `config.py` and `.env` file with your database information in this format: 
    DB_HOST = your_host
    DB_PORT = your_port
    DB_NAME = your_name
    DB_USER = your_user
    DB_PASSWORD = your_password
 2. Run the command `docker compose up --build`
 3. Create the schema for the database: `docker exec -i player_db psql -U postgres -d "val-project" < "YOURFILEPATH"`
 4. Open the container: `docker exec -it player_api bash`. Then run the following scripts:
  * `python database/load_team_regions.py`
  * `python database/load_player_data.py`
  * `python database/load_match_data.py`
  * `python database/load_rosters.py`

 5. Connect to the database via the terminal with the command: `docker exec -it player_db psql -U postgres -d "val-project"`