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


        
    """)