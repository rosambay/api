import duckdb

async def create_tables(dbd: duckdb.DuckDBPyConnection):
    dbd.execute("""
                
        CREATE TABLE IF NOT EXISTS snap_time (
                client_id INTEGER PRIMARY KEY,
                jobs TIMESTAMP,
                styles TIMESTAMP,
                resources TIMESTAMP,
                resource_windows TIMESTAMP,
                teams TIMESTAMP,
                job_types TIMESTAMP,
                job_status TIMESTAMP,
                places TIMESTAMP,
                address TIMESTAMP,
                geopos TIMESTAMP
        );
                
        CREATE TABLE IF NOT EXISTS semaforo AS (
                select 'calcDistance' AS process, 0 status
                union
                select 'checkPendencias' AS process, 0 status 
                union
                select 'getBackup' AS process, 0 status
        );
                
        CREATE SEQUENCE IF NOT EXISTS seq_job_id;
        CREATE TABLE IF NOT EXISTS jobs (
            uid                     UUID UNIQUE DEFAULT gen_random_uuid(),
            client_id               INTEGER NOT NULL,
            job_id                  INTEGER NOT NULL,
            client_job_id           VARCHAR NOT NULL,
            team_id                 INTEGER NULL,
            resource_id             INTEGER NULL,
            job_status_id           INTEGER NULL,
            job_type_id             INTEGER NULL,
            address_id              INTEGER NULL,
            place_id                INTEGER NULL,
            priority                INTEGER DEFAULT 0 NOT NULL,
            time_setup              INTEGER,
            time_service            INTEGER,
            time_overlap            INTEGER,
            distance                INTEGER NULL,
            time_distance           INTEGER NULL,
            first_distance          INTEGER NULL,
            first_time_distance     INTEGER NULL,
            last_distance           INTEGER NULL,
            last_time_distance      INTEGER NULL,
            work_duration           INTEGER DEFAULT 0 NOT NULL,
            plan_start_date         TIMESTAMP NOT NULL,
            plan_end_date           TIMESTAMP NOT NULL,
            ajustment_start_date    TIMESTAMP NULL,
            ajustment_end_date      TIMESTAMP NULL,
            actual_start_date       TIMESTAMP NULL,
            actual_end_date         TIMESTAMP NULL,
            time_limit_start        TIMESTAMP NULL,
            time_limit_end          TIMESTAMP NULL,
            complements             JSON NULL,
            created_by              VARCHAR NOT NULL,
            created_date            TIMESTAMP NOT NULL,
            modified_by             VARCHAR NOT NULL,
            modified_date           TIMESTAMP NOT NULL,
            PRIMARY KEY (client_id, job_id),
            UNIQUE (client_id, client_job_id) );

        CREATE SEQUENCE IF NOT EXISTS seq_resource_id;        
        CREATE TABLE IF NOT EXISTS resources (
            uid                     UUID UNIQUE DEFAULT gen_random_uuid(),
            client_id               INTEGER NOT NULL,
            resource_id             INTEGER NOT NULL,
            client_resource_id      VARCHAR NOT NULL,
            description             VARCHAR NOT NULL,
            actual_geocode_lat DOUBLE,
            actual_geocode_long DOUBLE,
            geocode_lat_from DOUBLE,
            geocode_long_from DOUBLE,
            geocode_lat_at DOUBLE,
            geocode_long_at DOUBLE,
            fl_off_shift INTEGER DEFAULT 0 NOT NULL,
            logged_in TIMESTAMP,
            logged_out TIMESTAMP,
            time_setup INTEGER,
            time_service INTEGER,
            time_overlap INTEGER,
            created_by              VARCHAR NOT NULL,
            created_date            TIMESTAMP NOT NULL,
            modified_by             VARCHAR NOT NULL,
            modified_date           TIMESTAMP NOT NULL,
            modified_date_geo TIMESTAMP NOT NULL,
            modified_date_login TIMESTAMP NOT NULL,
                 PRIMARY KEY (client_id, resource_id),
                 UNIQUE (client_id, client_resource_id)
        );

        CREATE SEQUENCE IF NOT EXISTS seq_team_id;
        CREATE TABLE IF NOT EXISTS teams (
            client_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            uid UUID UNIQUE DEFAULT gen_random_uuid(),
            client_team_id VARCHAR NOT NULL,
            team_name VARCHAR NOT NULL,
            time_setup INTEGER,
            time_overlap INTEGER,
            time_service INTEGER,
            start_time TIME NOT NULL DEFAULT '08:00:00',
            end_time TIME NOT NULL DEFAULT '18:00:00',
            geocode_lat DOUBLE,
            geocode_long DOUBLE,
            created_by VARCHAR NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR NOT NULL,
            modified_date TIMESTAMP NOT NULL,
            PRIMARY KEY (client_id, team_id),
            UNIQUE (client_id, client_team_id)
        );

        CREATE INDEX IF NOT EXISTS idx_team_00 ON teams (client_team_id, client_id);
        CREATE INDEX IF NOT EXISTS idx_team_01 ON teams (modified_date, client_id);

        -- 3. Tabela: job_status
        CREATE SEQUENCE IF NOT EXISTS seq_job_status_id;

        CREATE TABLE IF NOT EXISTS job_status (
            uid UUID UNIQUE DEFAULT gen_random_uuid(),
            client_id INTEGER NOT NULL,
            job_status_id INTEGER NOT NULL,
            client_job_status_id VARCHAR NOT NULL,
            style_id INTEGER,
            description VARCHAR NOT NULL,
            internal_code_status VARCHAR, 
            created_by VARCHAR NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR NOT NULL,
            modified_date TIMESTAMP NOT NULL,

            PRIMARY KEY (client_id, job_status_id),
            UNIQUE (client_id, client_job_status_id),
            
        );

        -- 4. Tabela: job_types
        CREATE SEQUENCE IF NOT EXISTS seq_job_type_id;
        CREATE TABLE IF NOT EXISTS job_types (
            uid UUID UNIQUE DEFAULT gen_random_uuid(),
            client_id INTEGER NOT NULL,
            job_type_id INTEGER NOT NULL,
            client_job_type_id VARCHAR NOT NULL,
            style_id INTEGER,
            description VARCHAR NOT NULL,
           
            priority INTEGER NOT NULL DEFAULT 25,
            
            time_setup INTEGER,
            time_service INTEGER,
            time_overlap INTEGER,
            
            created_by VARCHAR NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR NOT NULL,
            modified_date TIMESTAMP NOT NULL,
            
            -- Constraints de Integridade
            PRIMARY KEY (client_id, job_type_id),
            UNIQUE (client_id, client_job_type_id),
            
        );
        CREATE SEQUENCE IF NOT EXISTS seq_style_id;

        -- 2. Tabela: styles
        CREATE TABLE IF NOT EXISTS styles (
            client_id INTEGER NOT NULL,
            
            style_id INTEGER NOT NULL,
            
            uid UUID UNIQUE DEFAULT gen_random_uuid(),
            
            client_style_id VARCHAR,
            font_weight VARCHAR,
            
            background VARCHAR NOT NULL DEFAULT '#FFFFFF',
            foreground VARCHAR NOT NULL DEFAULT '#000000',
            
            created_by VARCHAR NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR NOT NULL,
            modified_date TIMESTAMP NOT NULL,
            
            PRIMARY KEY (client_id, style_id),
            
        );

        CREATE INDEX IF NOT EXISTS idx_styles_01 ON styles (modified_date, client_id);
        
        -- 2. Tabela: places

        CREATE SEQUENCE IF NOT EXISTS seq_place_id;
                
        CREATE TABLE IF NOT EXISTS places (
            client_id INTEGER NOT NULL,
            place_id INTEGER NOT NULL,
            uid UUID UNIQUE DEFAULT gen_random_uuid(),
            client_place_id VARCHAR NOT NULL,
            trade_name VARCHAR,
            cnpj VARCHAR,
            created_by VARCHAR NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR NOT NULL,
            modified_date TIMESTAMP NOT NULL,
            PRIMARY KEY (client_id, place_id),
            
            CONSTRAINT uk_place UNIQUE (client_id, client_place_id),
            
        );

        CREATE INDEX IF NOT EXISTS idx_place_00 ON places (client_place_id, client_id);
        CREATE INDEX IF NOT EXISTS idx_place_01 ON places (modified_date, client_id);

        -- 2. Tabela: address

        CREATE SEQUENCE IF NOT EXISTS seq_address_id;
        CREATE TABLE IF NOT EXISTS address (
            client_id INTEGER NOT NULL,
            address_id INTEGER NOT NULL,
            uid UUID UNIQUE DEFAULT gen_random_uuid(),
            client_address_id VARCHAR NOT NULL,
            geocode_lat DOUBLE,
            geocode_long DOUBLE,
            address VARCHAR,
            city VARCHAR,
            state_prov VARCHAR,
            zippost VARCHAR,
            time_setup INTEGER,
            created_by VARCHAR NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR NOT NULL,
            modified_date TIMESTAMP NOT NULL,
            PRIMARY KEY (client_id, address_id),
            
            CONSTRAINT uk_address UNIQUE (client_id, client_address_id)
        );
            
        CREATE INDEX IF NOT EXISTS idx_address_00 ON address (client_address_id, client_id);
        CREATE INDEX IF NOT EXISTS idx_address_01 ON address (modified_date, client_id);
                
        -- 2. Tabela: team_members

        CREATE TABLE IF NOT EXISTS team_members (
            client_id INTEGER NOT NULL,
            uid UUID UNIQUE DEFAULT gen_random_uuid(),
            team_id INTEGER NOT NULL,
            resource_id INTEGER NOT NULL,
            created_by VARCHAR NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR NOT NULL,
            modified_date TIMESTAMP NOT NULL,
            PRIMARY KEY (client_id, team_id, resource_id)
        );
        
        -- resource windows
        CREATE SEQUENCE IF NOT EXISTS seq_rw_id;

        CREATE TABLE IF NOT EXISTS resource_windows (
            client_id INTEGER NOT NULL,
            resource_id INTEGER NOT NULL,
            rw_id INTEGER PRIMARY KEY,
            client_rw_id VARCHAR NOT NULL,
            
            uid UUID UNIQUE DEFAULT gen_random_uuid(),
            
            week_day INTEGER NOT NULL,
            style_id INTEGER,
            description VARCHAR NOT NULL,

            start_time TIME NOT NULL,
            end_time TIME NOT NULL,
            
            created_by VARCHAR NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR NOT NULL,
            modified_date TIMESTAMP NOT NULL,

            CONSTRAINT fk_resource_windows_styles 
                FOREIGN KEY (client_id, style_id) REFERENCES styles (client_id, style_id),
                
            CONSTRAINT uk_resource_windows 
                UNIQUE (client_id, resource_id, week_day, start_time, end_time),

            CONSTRAINT ck_resource_window_week_day 
                CHECK (week_day >= 1 AND week_day <= 7)
        );

        CREATE INDEX IF NOT EXISTS idx_resource_windows_01 ON resource_windows (modified_date, client_id);

        CREATE TABLE IF NOT EXISTS reports (
            client_id INTEGER NOT NULL,
            report_id INTEGER PRIMARY KEY NOT NULL,
            client_team_id VARCHAR NOT NULL,
            report_date TIMESTAMP NOT NULL,
            rebuild INTEGER DEFAULT 0 NOT NULL
        );
                
        -- 1. O DuckDB usa Sequences para lidar com autoincrement (equivalente ao SERIAL do Postgres)
        CREATE SEQUENCE IF NOT EXISTS seq_address_window_id;

        -- 2. Criação da Tabela
        CREATE TABLE address_windows (
            uid UUID UNIQUE DEFAULT uuid(),
            client_id INTEGER NOT NULL,
            address_id INTEGER NOT NULL,
            address_window_id INTEGER NOT NULL,
            week_day INTEGER NOT NULL,
            description VARCHAR NOT NULL,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL,
            created_by VARCHAR NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR NOT NULL,
            modified_date TIMESTAMP NOT NULL,
            
            PRIMARY KEY (client_id, address_id, address_window_id),
            CONSTRAINT fk_address_windows_address FOREIGN KEY (client_id, address_id) REFERENCES address (client_id, address_id),
            CONSTRAINT ck_address_window_week_day CHECK (week_day >= 1 AND week_day <= 7)
        );

        -- 3. Criação do Índice
        CREATE INDEX idx_address_windows_00 ON address_windows (modified_date, client_id);
        
        -- 2. Criação da Tabela PRIORITY
        CREATE SEQUENCE IF NOT EXISTS seq_priority_id;
                
        CREATE TABLE priority (
            uid UUID UNIQUE DEFAULT uuid(),
            client_id INTEGER NOT NULL,
            priority_id INTEGER DEFAULT nextval('seq_priority_id') NOT NULL,
            client_priority_id VARCHAR NOT NULL,
            priority INTEGER DEFAULT 0 NOT NULL,
            created_by VARCHAR NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR NOT NULL,
            modified_date TIMESTAMP NOT NULL,
            
            PRIMARY KEY (client_id, priority_id),
        );

        -- Criação de uma sequência para simular o autoincrement, 
        -- já que chaves primárias compostas não aceitam SERIAL diretamente no DuckDB.
        CREATE SEQUENCE IF NOT EXISTS seq_simulation_id;

        CREATE TABLE simulation (
            uid UUID UNIQUE DEFAULT uuid(),
            client_id INTEGER NOT NULL,
            simulation_id INTEGER NOT NULL DEFAULT nextval('seq_simulation_id'),
            team_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            simulation_date DATE NOT NULL,
            session UUID NOT NULL,
            sequence INTEGER NOT NULL,
            json_dado JSON,
            fl_calc_board INTEGER NOT NULL DEFAULT 0,
            fl_calc_plan INTEGER NOT NULL DEFAULT 0,
            fl_calc_history INTEGER NOT NULL DEFAULT 0,
            fl_calc_arround INTEGER NOT NULL DEFAULT 0,
            created_by VARCHAR(32) NOT NULL,
            created_date TIMESTAMP NOT NULL,
            modified_by VARCHAR(32) NOT NULL,
            modified_date TIMESTAMP NOT NULL,
            
            -- Constraints
            PRIMARY KEY (client_id, simulation_id),
            UNIQUE (client_id, user_id, team_id, simulation_date, session, sequence),
            FOREIGN KEY (client_id, team_id) REFERENCES teams(client_id, team_id)
        );
        
        CREATE OR REPLACE VIEW simulation_view AS
        WITH rotas_unnest AS (
            -- 1. Descompacta o array JSON principal (raiz) em múltiplas linhas
            SELECT 
                simulation_id,
                session,
                sequence,
                client_id, 
                simulation_date,
                -- DuckDB: Converte o JSON para um array e descompacta
                UNNEST(CAST(json_dado AS JSON[])) AS rota_json 
            FROM simulation
        ),
        jobs_unnest AS (
            -- 2. Extrai os campos da rota e descompacta o array "jobs" interno
            SELECT 
                simulation_id, session, sequence, client_id, simulation_date,
                
                -- Extração dos campos do objeto JSON "rota" usando o operador ->> e CAST (::)
                (rota_json->>'resource_id')::INT AS resource_id,
                rota_json->>'resource_name' AS resource_name,
                rota_json->>'client_resource_id' AS client_resource_id,
                (rota_json->>'client_id')::INT AS rota_client_id,
                (rota_json->>'job_day')::DATE AS rota_job_day,
                (rota_json->>'date_from')::TIMESTAMP AS date_from,
                (rota_json->>'date_at')::TIMESTAMP AS date_at,
                (rota_json->>'arrival_from')::INT AS arrival_from,
                (rota_json->>'arrival_at')::INT AS arrival_at,
                (rota_json->>'distance_from')::INT AS distance_from,
                (rota_json->>'distance_at')::INT AS distance_at,
                (rota_json->>'total_distance')::INT AS total_distance,
                (rota_json->>'time_distance_from')::INT AS time_distance_from,
                (rota_json->>'time_distance_at')::INT AS time_distance_at,
                (rota_json->>'total_time_distance')::INT AS total_time_distance,
                (rota_json->>'geocode_lat_from')::DOUBLE AS geocode_lat_from,
                (rota_json->>'geocode_long_from')::DOUBLE AS geocode_long_from,
                (rota_json->>'geocode_lat_at')::DOUBLE AS geocode_lat_at,
                (rota_json->>'geocode_long_at')::DOUBLE AS geocode_long_at,
                (rota_json->>'total_jobs')::INT AS total_jobs,
                
                -- DuckDB: Pega o array "jobs" de dentro da rota e descompacta
                UNNEST(CAST(rota_json->>'jobs' AS JSON[])) AS job_json
            FROM rotas_unnest
        )
        SELECT 
            -- CAMPOS DO RECURSO/ROTA (Nível Raiz)
            simulation_id, session, sequence, client_id, simulation_date,
            resource_id, resource_name, client_resource_id, rota_client_id, rota_job_day,
            date_from, date_at, arrival_from, arrival_at, distance_from, distance_at,
            total_distance, time_distance_from, time_distance_at, total_time_distance,
            geocode_lat_from, geocode_long_from, geocode_lat_at, geocode_long_at, total_jobs,
            
            -- CAMPOS DO JOB (Extraindo do JSON individual do job gerado no passo anterior)
            (job_json->>'job_id')::INT AS job_id,
            job_json->>'client_job_id' AS client_job_id,
            (job_json->>'client_id')::INT AS job_client_id,
            (job_json->>'job_day')::DATE AS job_job_day,
            job_json->>'trade_name' AS trade_name,
            job_json->>'cnpj' AS cnpj,
            job_json->>'type_description' AS type_description,
            job_json->>'status_description' AS status_description,
            (job_json->>'service')::INT AS service,
            (job_json->>'team_id')::INT AS team_id,
            job_json->>'address' AS address,
            job_json->>'city' AS city,
            job_json->>'state_prov' AS state_prov,
            job_json->>'zippost' AS zippost,
            (job_json->>'start_date')::TIMESTAMP AS start_date,
            (job_json->>'end_date')::TIMESTAMP AS end_date,
            (job_json->>'arrival')::INT AS arrival,
            (job_json->>'distance')::INT AS distance,
            (job_json->>'time_distance')::INT AS time_distance,
            (job_json->>'geocode_lat')::DOUBLE AS geocode_lat,
            (job_json->>'geocode_lang')::DOUBLE AS geocode_lang
        FROM jobs_unnest;
    """)