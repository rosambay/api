from pydantic import BaseModel, EmailStr, UUID4
from typing import Optional, List, Dict, Any
from datetime import datetime, time, date

# --- Auth & User ---
class LoginRequest(BaseModel):
    domain: str
    user: str
    pwd: str
    

class Token(BaseModel):
    access_token: str
    token_type: str
    session: UUID4

class ViewClientResponse(BaseModel):
    client_id: int
    uid: UUID4
    domain: str
    name: str

class ViewTeamResponse(BaseModel):
    team_id: int
    uid: UUID4
    client_team_id: str
    team_name: str


class ViewStylesResponse(BaseModel):
    style_id: int
    uid: UUID4
    font_weight: Optional[str] = None
    background: str
    foreground: str

class JobStatusResponse(BaseModel):
    job_status_id: int
    client_job_status_id: str
    style_id: Optional[int] = None
    description: str
    internal_code_status: Optional[str] = None

    class Config:
        from_attributes = True

class JobStatusUpdateRequest(BaseModel):
    style_id: Optional[int] = None
    description: Optional[str] = None
    internal_code_status: Optional[str] = None

class JobRescheduleRequest(BaseModel):
    plan_start_date: datetime
    plan_end_date: datetime

class JobRescheduleResponse(BaseModel):
    job_id: int
    plan_start_date: datetime
    plan_end_date: datetime
    time_service: Optional[int] = None

    class Config:
        from_attributes = True

class ResourceUpdateRequest(BaseModel):
    off_shift_flag: Optional[int] = None
    time_setup: Optional[int] = None
    time_service: Optional[int] = None
    time_overlap: Optional[int] = None

class ResourceUpdateResponse(BaseModel):
    resource_id: int
    client_resource_id: str
    off_shift_flag: int
    time_setup: Optional[int]
    time_service: Optional[int]
    time_overlap: Optional[int]

    class Config:
        from_attributes = True

class NewSimulationRequest(BaseModel):
    team_id: int
    session: UUID4
    p_date: str
    # resources: Optional[List[int]] = None

class ScheduleJobsRequest(BaseModel):
    simulation_id: int
    action: str
    resources: List[int] | None = None
    jobs: List[int] | None = None
    per_resource: Dict[str, Any] | None = None

class SimulationRealTimeJobsRequest(BaseModel):
    team_id: int
    p_date: str
    type: str # pode ser JED - Jobs estimated Done,  JRD-Jobs Real Done
    resources: List[int]

class SimulationBestRouteJobsRequest(BaseModel):
    team_id: int
    p_date: str
    type: str # pode ser BRAC - Best Resources Actual,  BRAA Best Resrouces All Actual

class HistoryBestRouteJobsRequest(BaseModel):
    team_id: int
    p_date: str

class BestRouteJobsByDateRequest(BaseModel):
    team_id: int
    p_start_date: str
    p_end_date: str

class SimulationComparisonRequest(BaseModel):
    p_date: str
    simulation_ids: List[int]

class ClearScheduleJobsRequest(BaseModel):
    resource_id: int
    simulation_id: int
class ScheduleResponse(BaseModel):
    schedule_id: int
    team_id: int
    schedule_start_date: date
    schedule_start_time: time
    status: str
    update_tasks: int
    next_schedule_date: datetime
    frequency: str
    type_resources: str
    resources: Optional[Any] = None

    class Config:
        from_attributes = True

class ScheduleCreateRequest(BaseModel):
    team_id: int
    schedule_start_date: date
    schedule_start_time: time
    next_schedule_date: datetime
    frequency: str = "DAILY"
    type_resources: str = "A"
    update_tasks: int = 0
    resources: Optional[Any] = None

class ScheduleUpdateRequest(BaseModel):
    team_id: Optional[int] = None
    schedule_start_date: Optional[date] = None
    schedule_start_time: Optional[time] = None
    status: Optional[str] = None
    next_schedule_date: Optional[datetime] = None
    frequency: Optional[str] = None
    type_resources: Optional[str] = None
    update_tasks: Optional[int] = None
    resources: Optional[Any] = None

class ViewResourceWindowsResponse(BaseModel):
    rw_id: int
    uid: UUID4
    resource_id: int
    week_day: int
    style_id: Optional[int] = None
    description: str
    start_time: time
    end_time: time

    class Config:
        from_attributes = True
# class UserBase(BaseModel):
#     usuario: str
#     nome: str
#     email: EmailStr
#     status: str = 'A'
#     flag_foto_perfil: str = 'N'
#     flag_tipo_foto: str = 'U'
#     aprovador: str = 'N'
#     avatar_id: Optional[str] = None

# class UserCreate(UserBase):
#     senha: str
#     super_user: str = 'N'

# class FamilyRegister(BaseModel):
#     # Dados da Família (Domínio)
#     nome_familia: str # Será mapeado para 'dominio'
#     descricao_familia: Optional[str] = None
#     email_familia: EmailStr
    
#     # Dados do Usuário Admin (Pai/Mãe)
#     usuario_admin: str
#     nome_admin: str
#     email_admin: EmailStr
#     senha_admin: str

# class UserUpdate(BaseModel):
#     nome: Optional[str] = None
#     email: Optional[EmailStr] = None
#     senha: Optional[str] = None
#     status: Optional[str] = None
#     flag_foto_perfil: Optional[str] = None
#     flag_tipo_foto: Optional[str] = None
#     avatar_id: Optional[str] = None

# class UserResponse(UserBase):
#     uid: UUID4
#     user_id: int
#     dominio_id: int
#     super_user: str
#     class Config:
#         from_attributes = True

# # --- User Setup ---
# class UserSetupBase(BaseModel):
#     periodo: str = 'S'
#     debcred: str
#     tipo: str
#     tipo_valor: str = 'PR'
#     days_off: Optional[Dict[str, Any]] = None
#     aprovacao: str = 'S'
#     show_valor: Optional[str] = 'S' # S-Sim, N-Não
#     valor: float
#     valor_bonus: Optional[float] = None

# class UserSetupCreate(UserSetupBase):
#     pass

# class UserSetupUpdate(BaseModel):
#     periodo: Optional[str] = None
#     debcred: Optional[str] = None
#     days_off: Optional[Dict[str, Any]] = None
#     tipo: Optional[str] = None
#     show_valor: Optional[str] = 'S' # S-Sim, N-Não
#     tipo_valor: Optional[str] = None
#     valor: Optional[float] = None

# class UserSetupResponse(UserSetupBase):
#     uid: UUID4
#     user_created: UUID4
#     date_created: datetime
#     class Config:
#         from_attributes = True

# # --- Dominios ---
# class DominioBase(BaseModel):
#     dominio: str
#     descricao: Optional[str] = None
#     email: EmailStr
#     status: str = 'A'

# class DominioCreate(DominioBase):
#     pass

# class DominioUpdate(BaseModel):
#     uid: UUID4
#     days_off: Optional[Dict[str, Any]] = None


# class DominioResponse(DominioBase):
#     uid: UUID4
#     dominio_id: int
#     days_off: Optional[Dict[str, Any]]
#     class Config:
#         from_attributes = True

# # --- Tasks & Types ---
# class TaskTypeBase(BaseModel):
#     descricao: str
#     status: str = 'A'

# class TaskTypeCreate(TaskTypeBase):
#     pass

# class TaskTypeUpdate(BaseModel):
#     descricao: Optional[str] = None
#     status: Optional[str] = None

# class TaskTypeResponse(TaskTypeBase):
#     task_type_id: int
#     uid: UUID4
#     dominio_id: int
    
#     class Config:
#         from_attributes = True

# class TaskBase(BaseModel):
#     tarefa: str
#     detalhes: Optional[str] = None
#     status: str = 'A'

# class TaskCreate(TaskBase):
#     tipo_tarefa_uid: UUID4

# class TaskUpdate(BaseModel):
#     tarefa: Optional[str] = None
#     detalhes: Optional[str] = None
#     status: Optional[str] = None
#     tipo_tarefa_uid: Optional[UUID4] = None

# class TaskResponse(TaskBase):
#     uid: UUID4
#     tarefa: str
#     detalhes: Optional[str]
#     status: str
#     tipo_tarefa_id: int
    
#     class Config:
#         from_attributes = True

# # --- Schedules ---
# class ScheduleBase(BaseModel):
#     periodo: str 
#     periodicidade: Optional[Dict[str, Any]] = None
#     peso: int = 1
#     hora_inicio: Optional[time] = None
#     hora_fim: Optional[time] = None
#     data_inicio: datetime
#     data_fim: Optional[datetime] = None
#     status: str = 'A'
#     todo_periodo: str = 'N'
#     tipo: str = 'P'

# class ScheduleCreate(ScheduleBase):
#     user_uid: UUID4
#     tarefa_uid: UUID4

# class ScheduleUpdate(BaseModel):
#     periodo: Optional[str] = None
#     periodicidade: Optional[Dict[str, Any]] = None
#     peso: Optional[int] = None
#     hora_inicio: Optional[time] = None
#     hora_fim: Optional[time] = None
#     data_inicio: Optional[datetime] = None
#     data_fim: Optional[datetime] = None
#     status: Optional[str] = None
#     user_uid: Optional[UUID4] = None
#     tarefa_uid: Optional[UUID4] = None
#     todo_periodo: str = 'N'

# class ScheduleResponse(ScheduleBase):
#     uid: UUID4
#     dominio_id: int
#     user_created: UUID4
#     date_created: datetime
#     user_uid: Optional[UUID4] = None
#     tarefa_uid: Optional[UUID4] = None

#     class Config:
#         from_attributes = True

# class ScheduleBonusResponse(BaseModel):
#     uid: Optional[UUID4] = None
#     tarefa: Optional[str] = None
#     detalhes: Optional[str] = None
#     show_valor: Optional[str] = 'S' # S-Sim, N-Não
#     valor_bonus: Optional[float] = None
#     tipo: Optional[str] = None
#     class Config:
#         from_attributes = True
# # --- View Schedule Output (Atualizado) ---
# # Arquivo: schemas.py

# # ... outros schemas ...

# # --- View Schedule Output ---
# class ViewScheduleResponse(BaseModel):
#     schedule_id: int
#     dominio_id: int
#     user_id: int
#     data_ref: date
#     startdate: date
#     enddate: date
    
#     # ADICIONE ESTA LINHA ABAIXO:
#     uid_schedule: Optional[UUID4] = None 
#     report_uid: Optional[UUID4] = None 
    
#     usuario: Optional[str] = None
#     tarefa: Optional[str] = None
#     valor_previsto: Optional[float] = None
#     valor_meta: Optional[float] = None
#     status_execucao: Optional[str] = None
#     tipo: Optional[str] = None
#     todo_periodo: Optional[str] = None
#     transfer: Optional[str] = None
#     show_valor: Optional[str] = 'S' # S-Sim, N-Não
#     pontuacao: Optional[float] = None
#     user_id_origem: Optional[int] = None
#     usuario_origem: Optional[str] = None
#     user_id_destino: Optional[int] = None
#     usuario_destino: Optional[str] = None
#     # --- ADICIONE AQUI ---
#     detalhes: Optional[str] = None
#     obs: Optional[str] = None
#     hora_inicio: Optional[time] = None
#     hora_fim: Optional[time] = None
#     image_link: Optional[str] = None
    
#     class Config:
#         from_attributes = True

# # --- NOVO: Task Report Schemas ---
# class TaskReportBase(BaseModel):
#     data_task: datetime
#     valor: Optional[float] = None
#     detalhes: Optional[str] = None
#     status: str = 'P' # A-Atendido N-Não Atendido

# class TaskReportCreate(TaskReportBase):
#     schedule_uid: UUID4 # Cliente envia UID do schedule
#     transfer_user_uid: Optional[UUID4] = None

# class TaskReportUpdate(BaseModel):
#     valor: Optional[float] = None
#     detalhes: Optional[str] = None
#     status: Optional[str] = None # Pode alterar para 'E' (Excluído)
#     transfer_user_uid: Optional[UUID4] = None

# class TaskReportResponse(TaskReportBase):
#     uid: UUID4
#     dominio_id: int
#     schedule_id: int # ID interno, se quiser expor
#     user_created: UUID4
#     date_created: datetime
#     image_link: Optional[str] = None
#     user_id_origem: Optional[int] = None

#     class Config:
#         from_attributes = True

# class TaskReportApproveResponse(TaskReportBase):
#     uid: UUID4
#     dominio_id: int
#     schedule_id: int # ID interno, se quiser expor
#     perc_aprova: int
#     user_id_aprova: int
#     date_approved: datetime
#     user_updated: UUID4
#     date_updated: datetime
    
#     class Config:
#         from_attributes = True

# class TaskReportApproveUpdate(BaseModel):
#     perc_aprova: Optional[int] = None
#     user_id_aprova: int

# class ViewScheduleReportResponse(BaseModel):
#     user_id: int
#     apuracao: str
#     uid_schedule: UUID4
#     schedule_uid: Optional[UUID4] = None
#     detalhe_apuracao: str
#     usuario: str
#     data_ref: datetime
#     data_inicio_periodo: datetime
#     data_fim_periodo: datetime
#     tarefa: str
#     image_link: Optional[str] = None
#     valor: Optional[float] = None
#     valor_meta: Optional[float] = None
#     tipo: str
#     debcred: str
#     show_valor: str
#     perc_aprova: int

# # schemas.py (Adicione ao final)

# class CheckAvailabilityResponse(BaseModel):
#     available: bool
#     message: str
#     suggestion: Optional[str] = None

# class FamilyRegisterInit(BaseModel):
#     nome_familia: str
#     descricao_familia: Optional[str] = None
#     email_familia: EmailStr
#     usuario_admin: str
#     nome_admin: str
#     senha_admin: str

# class FamilyRegisterConfirm(BaseModel):
#     token: str

# class Notificacao(BaseModel):
#     target_uid: str  # ID de quem vai receber
#     titulo: str
#     mensagem: str