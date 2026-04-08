# models.py
from sqlalchemy import Column, Date, Float, Integer, String, DateTime, Index, Text, UniqueConstraint, CheckConstraint, text, ForeignKey, ForeignKeyConstraint,Time
from sqlalchemy.dialects.postgresql import UUID, JSONB
from database import Base


class Clients(Base):
    __tablename__ = "clients"

    client_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    domain = Column(String(64), nullable=False)    
    name = Column(String(128), nullable=False)    
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
        UniqueConstraint('domain', name='uk_client_dominio'),
    )

class Users(Base):
    __tablename__ = "users"
    client_id = Column(Integer, ForeignKey("clients.client_id"), primary_key=True, nullable=False)
    user_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    user_name = Column(String(32), nullable=False)    
    name = Column(String(128), nullable=False)    
    passwd = Column(Text, nullable=False) 
    super_user = Column(Integer, nullable=False, server_default=text("0"))
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)

class UserTeam(Base):
    __tablename__ = "user_team"
    client_id = Column(Integer, ForeignKey("clients.client_id"), nullable=False)
    user_id = Column(Integer, nullable=False)
    team_id = Column(Integer, nullable=False)
    uid = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
      ForeignKeyConstraint(
                ['client_id', 'user_id'],
                ['users.client_id', 'users.user_id'],
                name='fk_user_team_users'
            ),
      ForeignKeyConstraint(
                ['client_id', 'team_id'],
                ['teams.client_id', 'teams.team_id'],
                name='fk_user_team_teams'
            ),
      UniqueConstraint('client_id','user_id','team_id', name='uk_user_team')
    )

class Styles(Base):
    __tablename__ = "styles"
    client_id = Column(Integer, ForeignKey("clients.client_id"), primary_key=True, nullable=False)
    style_id = Column(Integer, primary_key=True, autoincrement=True)
    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_style_id = Column(String(32), nullable=True)
    font_weight = Column(String(32), nullable=True)
    background = Column(String(32), nullable=False, default='#FFFFFF')    
    foreground = Column(String(32), nullable=False, default='#000000')    
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)

class Teams(Base):
    __tablename__ = "teams"

    client_id = Column(Integer, ForeignKey("clients.client_id"), primary_key=True, nullable=False)
    team_id = Column(Integer, primary_key=True, autoincrement=True)
    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_team_id = Column(String(32), nullable=False)
    team_name = Column(String(128), nullable=False)
    time_setup = Column(Integer, nullable=True)
    time_service = Column(Integer, nullable=True)
    geocode_lat = Column(String(32), nullable=True)
    geocode_long = Column(String(32), nullable=True)    
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    
    __table_args__ = (
            UniqueConstraint('client_id','team_id','client_team_id', name='uk_team'),
            Index('idx_team_00', 'client_team_id','client_id'),
            Index('idx_team_01', 'modified_date','client_id')
        )
    
class Places(Base):
    __tablename__ = "places"

    client_id = Column(Integer, ForeignKey("clients.client_id"), primary_key=True, nullable=False)
    place_id = Column(Integer, primary_key=True, autoincrement=True)
    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_place_id = Column(String(32), nullable=False)
    trade_name = Column(String(128), nullable=True)
    cnpj = Column(String(32), nullable=True)
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    
    __table_args__ = (
            UniqueConstraint('client_id','place_id','client_place_id', name='uk_place'),
            Index('idx_place_00', 'client_place_id','client_id'),
            Index('idx_place_01', 'modified_date','client_id')
        )

class TeamMembers(Base):
    __tablename__ = "team_members"

    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_id = Column(Integer, primary_key=True, nullable=False)
    team_id = Column(Integer, primary_key=True, nullable=False)
    resource_id = Column(Integer, primary_key=True, nullable=False)
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    
    __table_args__ = (
            ForeignKeyConstraint(
                ['client_id', 'resource_id'],
                ['resources.client_id', 'resources.resource_id'],
                name='fk_resource_windows_resources'
            ),
            ForeignKeyConstraint(
                ['client_id', 'team_id'],
                ['teams.client_id', 'teams.team_id'],
                name='fk_resource_windows_team'
            ),
            Index('idx_team_members_00', 'modified_date','client_id')
        )

    
class Resources(Base):
    __tablename__ = "resources"
    client_id = Column(Integer, ForeignKey("clients.client_id"), primary_key=True, nullable=False)
    resource_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_resource_id = Column(String(32), nullable=False)
    description = Column(String(128), nullable=False)    
    actual_geocode_lat = Column(String(32), nullable=True)
    actual_geocode_long = Column(String(32), nullable=True)    
    geocode_lat_from = Column(String(32), nullable=True)
    geocode_long_from = Column(String(32), nullable=True)
    geocode_lat_at = Column(String(32), nullable=True)
    geocode_long_at = Column(String(32), nullable=True)
    fl_off_shift = Column(Integer, nullable=False, server_default=text("0"))
    logged_in = Column(DateTime, nullable=True)
    logged_out = Column(DateTime, nullable=True)
    time_setup = Column(Integer, nullable=True)
    time_service = Column(Integer, nullable=True)
    time_overlap = Column(Integer, nullable=True)
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    modified_date_geo = Column(DateTime, nullable=False)
    modified_date_login = Column(DateTime, nullable=False)
    __table_args__ = (
            UniqueConstraint('client_id','resource_id','client_resource_id', name='uk_resources'),
            Index('idx_resources_00', 'client_resource_id','client_id'),
            Index('idx_resources_01', 'modified_date','client_id'),
            Index('idx_resources_02', 'modified_date_geo','client_id'),
            Index('idx_resources_03', 'modified_date_login','client_id')
        )

class ResourceWindows(Base):
    __tablename__ = "resource_windows"

    client_id = Column(Integer, nullable=False)
    resource_id = Column(Integer, nullable=False)
    rw_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_rw_id = Column(String(32), nullable=False)
    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    week_day = Column(Integer, nullable=False)
    style_id = Column(Integer, nullable=True)
    description = Column(String(128), nullable=False)
    start_time = Column(Time, nullable=False)
    end_time = Column(Time, nullable=False)
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
            ForeignKeyConstraint(
                ['client_id', 'resource_id'],
                ['resources.client_id', 'resources.resource_id'],
                name='fk_resource_windows_resources'
            ),
            ForeignKeyConstraint(
                ['client_id', 'style_id'],
                ['styles.client_id', 'styles.style_id'],
                name='fk_resource_windows_styles'
            ),
            ForeignKeyConstraint(
                ['client_id'],
                ['clients.client_id'],
                name='fk_resource_windows_clients'
            ),
            UniqueConstraint('client_id','resource_id','week_day','start_time','end_time', name='uk_resource_windows'),
            CheckConstraint('week_day >= 1 AND week_day <= 7', name='ck_resource_window_week_day'),
            Index('idx_resource_windows_01', 'modified_date','client_id')
        )

class Address(Base):
    __tablename__ = "address"
    client_id = Column(Integer, ForeignKey("clients.client_id"), primary_key=True, nullable=False)
    address_id = Column(Integer, primary_key=True, autoincrement=True)
    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_address_id = Column(String(32), nullable=False)
    geocode_lat = Column(String(32), nullable=True)
    geocode_long = Column(String(32), nullable=True)    
    address = Column(String(128), nullable=True)
    city = Column(String(128), nullable=True)
    state_prov = Column(String(32), nullable=True) 
    zippost = Column(String(64), nullable=True) 
    time_setup = Column(Integer, nullable=True)
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
            UniqueConstraint('client_id','address_id','client_address_id', name='uk_address'),
            Index('idx_address_00', 'client_address_id','client_id'),
            Index('idx_address_01', 'modified_date','client_id')
        )

class AddressWindows(Base):
    __tablename__ = "address_windows"

    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_id = Column(Integer, primary_key=True, nullable=False)
    address_id = Column(Integer, primary_key=True, nullable=False)
    address_window_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    week_day = Column(Integer, nullable=False)
    description = Column(String(128), nullable=False)
    start_time = Column(Time, nullable=False)
    end_time = Column(Time, nullable=False)
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
            ForeignKeyConstraint(
                ['client_id', 'address_id'],
                ['address.client_id', 'address.address_id'],
                name='fk_address_windows_address'
            ),
            CheckConstraint('week_day >= 1 AND week_day <= 7', name='ck_address_window_week_day'),
            Index('idx_address_windows_00', 'modified_date','client_id')
        )
    
class JobStatus(Base):
    __tablename__ = "job_status"

    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_id = Column(Integer, ForeignKey("clients.client_id"), primary_key=True, nullable=False)
    job_status_id = Column(Integer, primary_key=True, autoincrement=True)
    client_job_status_id = Column(String(32), nullable=False)
    style_id = Column(Integer, nullable=True)
    description  = Column(String(128), nullable=False)
    internal_code_status = Column(String(6), nullable=True) #A-Agendado, B-Em Andamento, C-Concluído, D-Cancelado
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
        ForeignKeyConstraint(
                ['client_id', 'style_id'],
                ['styles.client_id', 'styles.style_id'],
                name='fk_job_status_styles'
            ),
            UniqueConstraint('client_id','job_status_id','client_job_status_id', name='uk_job_status'),
            Index('idx_job_status_00', 'client_job_status_id','client_id'),
            Index('idx_job_status_01', 'modified_date','client_id')
        )

class JobType(Base):
    __tablename__ = "job_types"

    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_id = Column(Integer, ForeignKey("clients.client_id"), primary_key=True, nullable=False)
    job_type_id = Column(Integer, primary_key=True, autoincrement=True)
    client_job_type_id = Column(String(32), nullable=False)
    style_id = Column(Integer, nullable=True)
    description  = Column(String(128), nullable=False)
    priority = Column(Integer, nullable=False, server_default=text("25"))
    time_setup = Column(Integer, nullable=True)
    time_service = Column(Integer, nullable=True)
    time_overlap = Column(Integer, nullable=True)
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
        ForeignKeyConstraint(
                ['client_id', 'style_id'],
                ['styles.client_id', 'styles.style_id'],
                name='fk_job_types_styles'
            ),
            UniqueConstraint('client_id','job_type_id','client_job_type_id', name='uk_job_types'),
            Index('idx_job_types_00', 'client_job_type_id','client_id'),
            Index('idx_job_types_01', 'modified_date','client_id')
        )

class Jobs(Base):
    __tablename__ = "jobs"

    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_id = Column(Integer, ForeignKey("clients.client_id"), primary_key=True, nullable=False)
    job_id = Column(Integer, primary_key=True, autoincrement=True)
    client_job_id = Column(String(128), nullable=False)
    team_id  = Column(Integer, nullable=False)
    resource_id  = Column(Integer, nullable=True)
    job_status_id  = Column(Integer, nullable=False)
    job_type_id  = Column(Integer, nullable=False)
    address_id = Column(Integer, nullable=False)
    place_id  = Column(Integer, nullable=False)
    time_setup = Column(Integer, nullable=True)
    time_service = Column(Integer, nullable=True)
    time_overlap = Column(Integer, nullable=True)
    work_duration = Column(Integer, nullable=True)
    plan_start_date = Column(DateTime, nullable=False)
    plan_end_date = Column(DateTime, nullable=False)
    actual_start_date = Column(DateTime, nullable=True)
    actual_end_date = Column(DateTime, nullable=True)
    time_limit_start = Column(DateTime, nullable=True)
    time_limit_end = Column(DateTime, nullable=True)
    pp_resource_id = Column(String(32), nullable=True)
    pp_start_date = Column(DateTime, nullable=True)
    pp_end_date = Column(DateTime, nullable=True)
    pt_job_id = Column(String(32), nullable=True)
    pt_start_date = Column(DateTime, nullable=True)
    pt_end_date = Column(DateTime, nullable=True)
    pt_geocode_lat = Column(String(32), nullable=True)
    pt_geocode_long = Column(String(32), nullable=True)    
    complements = Column(JSONB, nullable=True)
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
            ForeignKeyConstraint(
                ['client_id', 'resource_id'],
                ['resources.client_id', 'resources.resource_id'],
                name='fk_jobs_resource'
            ),
            ForeignKeyConstraint(
                ['client_id', 'team_id'],
                ['teams.client_id', 'teams.team_id'],
                name='fk_jobs_team'
            ),
            ForeignKeyConstraint(
                ['client_id', 'job_status_id'],
                ['job_status.client_id', 'job_status.job_status_id'],
                name='fk_jobs_job_status'
            ),
            ForeignKeyConstraint(
                ['client_id', 'job_type_id'],
                ['job_types.client_id', 'job_types.job_type_id'],
                name='fk_jobs_job_types'
            ),
            ForeignKeyConstraint(
                ['client_id', 'address_id'],
                ['address.client_id', 'address.address_id'],
                name='fk_jobs_address'
            ),
            ForeignKeyConstraint(
                ['client_id', 'place_id'],
                ['places.client_id', 'places.place_id'],
                name='fk_jobs_place'
            ),
            UniqueConstraint('client_id','job_id','client_job_id', name='uk_jobs'),
            Index('idx_jobs_00', 'client_job_id','client_id'),
            Index('idx_jobs_01', 'modified_date','client_id')
        )

class Simulation(Base):
    __tablename__ = "simulation"

    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_id = Column(Integer, primary_key=True, nullable=False)
    simulation_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    simulation_date = Column(Date, nullable=False)
    sequence = Column(Integer, nullable=False)
    fl_calc_board = Column(Integer, nullable=False, server_default=text("0"))
    fl_calc_plan = Column(Integer, nullable=False, server_default=text("0"))
    fl_calc_history = Column(Integer, nullable=False, server_default=text("0"))
    fl_calc_arround = Column(Integer, nullable=False, server_default=text("0"))
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
            ForeignKeyConstraint(
                ['client_id', 'user_id'],
                ['users.client_id', 'users.user_id'],
                name='fk_simulation_users'
            ),
            UniqueConstraint('client_id','user_id', 'simulation_date', 'sequence', name='uk_simulation'),
        )    

class SimulationJobs(Base):
    __tablename__ = "simulation_jobs"

    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_id = Column(Integer, primary_key=True, nullable=False)
    simulation_id = Column(Integer, primary_key=True, nullable=False)
    job_id = Column(Integer, primary_key=True, nullable=False)
    client_job_id = Column(String(128), nullable=False)
    team_id  = Column(Integer, nullable=False)
    resource_id  = Column(Integer, nullable=True)
    job_status_id  = Column(Integer, nullable=False)
    job_type_id  = Column(Integer, nullable=False)
    address_id = Column(Integer, nullable=False)
    place_id  = Column(Integer, nullable=False)
    actual_time_setup = Column(Integer, nullable=True)
    simulated_time_setup = Column(Integer, nullable=True)
    simulated_window_time_setup = Column(Integer, nullable=True)
    actual_time_service = Column(Integer, nullable=True)
    actual_work_duration = Column(Integer, nullable=True)
    simulated_work_duration = Column(Integer, nullable=True)
    actual_start_date = Column(DateTime, nullable=True)
    actual_end_date = Column(DateTime, nullable=True)
    simulated_start_date = Column(DateTime, nullable=True)
    simulated_end_date = Column(DateTime, nullable=True)
    simulated_window_start_date = Column(DateTime, nullable=True)
    simulated_window_end_date = Column(DateTime, nullable=True)
    time_limit_start = Column(DateTime, nullable=True)
    time_limit_end = Column(DateTime, nullable=True)
    complements = Column(JSONB, nullable=True)
    actual_order = Column(Integer, nullable=False, server_default=text("0"))
    simulated_order = Column(Integer, nullable=False, server_default=text("0"))
    simulated_window_order = Column(Integer, nullable=False, server_default=text("0"))
    actual_distance = Column(Integer, nullable=True)
    simulated_distance = Column(Integer, nullable=True)
    simulated_window_distance = Column(Integer, nullable=True)
    actual_time_distance = Column(Integer, nullable=True)
    simulated_time_distance = Column(Integer, nullable=True)
    simulated_window_time_distance = Column(Integer, nullable=True)
    status_priority = Column(Integer, nullable=False, server_default=text("25"))
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
            ForeignKeyConstraint(
                ['client_id', 'simulation_id'],
                ['simulation.client_id', 'simulation.simulation_id'],
                name='fk_simulation_jobs_simulation'
            ),
            ForeignKeyConstraint(
                ['client_id', 'job_id'],
                ['jobs.client_id', 'jobs.job_id'],
                name='fk_simulation_jobs_jobs'
            ),
            UniqueConstraint('client_id','simulation_id','client_job_id', name='uk_simulation_jobs'),
        )
    
class SimulationResources(Base):
    __tablename__ = "simulation_resources"

    uid = Column(UUID(as_uuid=True), unique=True, server_default=text("gen_random_uuid()"))
    client_id = Column(Integer, primary_key=True, nullable=False)
    simulation_id = Column(Integer, primary_key=True, nullable=False)
    resource_id  = Column(Integer, primary_key=True, nullable=False)
    actual_distance_end = Column(Integer, nullable=True)
    simulated_distance_end = Column(Integer, nullable=True)
    simulated_window_distance_start = Column(Integer, nullable=True)
    simulated_window_distance_end = Column(Integer, nullable=True)
    actual_time_distance_start = Column(Integer, nullable=True)
    actual_time_distance_end = Column(Integer, nullable=True)
    simulated_time_distance_start = Column(Integer, nullable=True)
    simulated_time_distance_end = Column(Integer, nullable=True)
    simulated_window_time_distance_start = Column(Integer, nullable=True)
    simulated_window_time_distance_end = Column(Integer, nullable=True)
    actual_end_date = Column(DateTime, nullable=True)
    simulated_end_date = Column(DateTime, nullable=True)
    simulated_window_end_date = Column(DateTime, nullable=True)
    simulated_window_start_date = Column(DateTime, nullable=True)
    created_by = Column(String(32), nullable=False)
    created_date = Column(DateTime, nullable=False)
    modified_by = Column(String(32), nullable=False)
    modified_date = Column(DateTime, nullable=False)
    __table_args__ = (
            ForeignKeyConstraint(
                ['client_id', 'simulation_id'],
                ['simulation.client_id', 'simulation.simulation_id'],
                name='fk_simulation_jobs_simulation'
            ),
            ForeignKeyConstraint(
                ['client_id', 'resource_id'],
                ['resources.client_id', 'resources.resource_id'],
                name='fk_simulation_jobs_resources'
            )
        )    