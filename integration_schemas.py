from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, time, date


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = 60 * 24 * 60 ##24 horas em segundos

class StyleUpsertRequest(BaseModel):
    client_style_id: str = Field(..., description="ID do Estilo no sistema do cliente")
    background: str = Field(..., description="Cor do fundo")
    foreground: str = Field(..., description="Cor do texto")
    modified_date: datetime = Field(..., description="Data/hora de última modificação do estilo")

class JobUpsertRequest(BaseModel):
    client_job_id: str = Field(..., description="ID do tarefa no sistema do cliente")
    client_team_id: str = Field(..., description="ID da equipe da tarefa no sistema do cliente")
    desc_team: str = Field(..., description="Descrição da equipe da tarefa no sistema do cliente")
    team_modified_date: datetime = Field(..., description="Data/hora de última modificação da equipe, se houver, da tarefa no sistema do cliente")
    client_resource_id: Optional[str] = Field(None, description="ID do recurso, se houver, da tarefa no sistema do cliente")
    client_status_id: str = Field(..., description="ID da situação (status) da tarefa no sistema do cliente")
    desc_status: str = Field(..., description="Descrição da situação (status) da tarefa no sistema do cliente")
    client_style_id: Optional[str] = Field(None, description="ID do estilo (style) da tarefa no sistema do cliente")
    status_modified_date: datetime = Field(..., description="Data/hora de última modificação da situação (status) da tarefa no sistema do cliente")
    client_type_id: str = Field(..., description="ID do tipo de tarefa no sistema do cliente")
    desc_type: str = Field(..., description="Descrição do tipo de tarefa no sistema do cliente")
    type_modified_date: datetime = Field(..., description="Data/hora de última modificação do tipo da tarefa no sistema do cliente")    
    client_place_id: str = Field(..., description="ID do local da tarefa no sistema do cliente")
    trade_name: str = Field(..., description="Nome comercial do local da tarefa, se houver, no sistema do cliente")
    cnpj: Optional[str] = Field(None, description="CNPJ do local da tarefa, se houver, no sistema do cliente")
    place_modified_date: datetime = Field(..., description="Data/hora de última modificação do local da tarefa no sistema do cliente")
    client_address_id: str = Field(..., description="ID do endereço da tarefa no sistema do cliente")
    address: str = Field(..., description="Endereço completo da tarefa no sistema do cliente")  
    city: str = Field(..., description="Cidade do local da tarefa, se houver, no sistema do cliente")
    state: str = Field(..., description="Estado do local da tarefa, se houver, no sistema do cliente")
    zip_code: Optional[str] = Field(None, description="CEP do local da tarefa, se houver, no sistema do cliente")
    geocode_lat: Optional[str] = Field(None, description="Latitude do local da tarefa, se houver, no sistema do cliente")
    geocode_long: Optional[str] = Field(None, description="Longitude do local da tarefa, se houver, no sistema do cliente")
    address_modified_date: datetime = Field(..., description="Data/hora de última modificação do endereço da tarefa no sistema do cliente")
    plan_start_date: datetime = Field(..., description="Data/hora planejada de início da tarefa no sistema do cliente")
    plan_end_date: datetime = Field(..., description="Data/hora planejada de término da tarefa no sistema do cliente")
    actual_start_date: Optional[datetime] = Field(None, description="Data/hora real de início da tarefa, se houver, no sistema do cliente")
    actual_end_date: Optional[datetime] = Field(None, description="Data/hora real de término da tarefa, se houver, no sistema do cliente")
    real_time_service: Optional[int] = Field(None, description="Tempo de serviço em segundos da tarefa, se houver, no sistema do cliente")
    plan_time_service: Optional[int] = Field(None, description="Tempo de serviço planejado em segundos da tarefa, se houver, no sistema do cliente")    
    limit_start_date: Optional[datetime] = Field(None, description="Data/hora limite de início da tarefa, se houver, no sistema do cliente")
    limit_end_date: Optional[datetime] = Field(None, description="Data/hora limite de término da tarefa, se houver, no sistema do cliente")
    priority: Optional[str] = Field(None, description="Prioridade da tarefa, se houver, no sistema do cliente")
    time_setup: Optional[int] = Field(None, description="Tempo de configuração em segundos da tarefa, se houver, no sistema do cliente")
    time_overlap: Optional[int] = Field(None, description="Tempo de sobreposição em segundos da tarefa, se houver, no sistema do cliente")
    distance: Optional[int] = Field(None, description="Distância em metros da tarefa, se houver, no sistema do cliente")
    time_distance: Optional[int] = Field(None, description="Tempo de deslocamento em segundos da tarefa, se houver, no sistema do cliente")
    complements: Optional[Any] = None
    created_date: datetime = Field(..., description="Data/hora da criação da tarefa")
    modified_date: datetime = Field(..., description="Data/hora da ultima atualização da tarefa")

class ResourcesUpsertRequest(BaseModel):
    client_resource_id: str = Field(..., description="ID do recurso no sistema do cliente")
    resource_name: Optional[str] = Field(None, description="Nome do recurso, se houver, da tarefa no sistema do cliente")
    off_shift_flag: int = Field(default=0, description="Indica que o recurso trabalha fora do turno.")
    off_shift_start_time: str = Field(default='07:00:00', description="Indica a hora inicial limite para atuar fora do turno.")
    off_shift_end_time: str = Field(default='22:00:00', description="Indica a hora final limite para atuar fora do turno.")
    geocode_lat_actual: Optional[str] = Field('None', description="Latitude atual do recurso")
    geocode_long_actual: Optional[str] = Field(None, description="Longitude atual do recurso")
    geocode_lat_start: Optional[str] = Field(None, description="Latitude da partida do recurso, para efetuar o calculo do deslocamento do inicio da rota do recurso")
    geocode_long_start: Optional[str] = Field(None, description="Longitude da partida do recurso, para efetuar o calculo do deslocamento do inicio da rota do recurso")
    geocode_lat_end: Optional[str] = Field(None, description="Latitude de chegada do recurso, para efetuar o calculo do deslocamento do fim da rota do recurso")
    geocode_long_end: Optional[str] = Field(None, description="Longitude de chegada do recurso, para efetuar o calculo do deslocamento do fim da rota do recurso")
    client_status_id: str = Field(..., description="ID da situação (status) do recurso do cliente")
    desc_status: str = Field(..., description="Descrição da situação (status) do recurso do cliente")
    modified_date: datetime = Field(..., description="Data/hora da ultima atualização da tarefa")

class ResourceWindowsUpsertRequest(BaseModel):
    client_resource_id: str = Field(..., description="ID do recurso no sistema do cliente")
    client_resource_window_id: str = Field(..., description="ID da janela do recurso no sistema do cliente")
    week_day: int = Field(..., ge=1, le=7, description="Dia da semana. 1-domingo... 7-sábado")
    description: str = Field(..., description="Descrição da Janela (seg, ter, qua...).")
    start_time: str = Field(..., pattern=r"^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$", description="Indica a hora inicial limite para atuar fora do turno.")
    end_time: str = Field(..., pattern=r"^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$", description="Indica a hora final limite para atuar fora do turno.")
    modified_date: datetime = Field(..., description="Data/hora da ultima atualização da tarefa")

class UpsertResponse(BaseModel):
    id: int
    client_id: str
    action: str  # "INSERT" | "UPDATE"

    class Config:
        from_attributes = True

class UpsertError(BaseModel):
    type: str
    id: str
    message: str

class BatchResponse(BaseModel):
    processed: int
    inserted: int
    updated: int
    results: list[UpsertResponse]
    errors: list[UpsertError]
