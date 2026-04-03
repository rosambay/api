import gzip
import zlib
import uuid
import asyncio
import re
from datetime import datetime
from typing import Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from xml.sax.saxutils import escape
import httpx
from lxml import etree
from loguru import logger

from config import settings


class FSMTarget(str, Enum):
    """Tipos de endpoint suportados"""
    WCF = "WCF"
    REST = "REST"


class FSMError(Exception):
    """Exceção base para erros do FSM"""
    def __init__(self, message: str, code: str = None, details: str = None):
        self.message = message
        self.code = code
        self.details = details
        super().__init__(message)


@dataclass
class FSMConnectionConfig:
    """Configuração de conexão com o FSM"""
    host: str
    username: str
    password: str
    tenant: Optional[str] = None
    target: FSMTarget = FSMTarget.WCF
    encrypted: bool = True
    compressed: bool = True
    include_auth: bool = True
    timeout: int = 60


@dataclass
class FSMResponse:
    """Resposta do FSM"""
    success: bool
    status_code: int
    response_xml: str
    duration_ms: int
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    attachments: Optional[dict] = None


class FSMClient:
    """
    Cliente para comunicação com o FSM Server.
    """
    
    # Namespaces
    SOAP12_NS = "http://www.w3.org/2003/05/soap-envelope"
    WSA_NS = "http://www.w3.org/2005/08/addressing"
    TEMPURI_NS = "http://tempuri.org/"
    
    def __init__(self, config: FSMConnectionConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._service_url = self._build_service_url()
        
    def _build_service_url(self) -> str:
        """Constrói a URL do serviço - usa o host base"""
        host = self.config.host.rstrip("/")
        # Remove M5Service.svc se presente
        if host.endswith("/M5Service.svc"):
            host = host.rsplit("/M5Service.svc", 1)[0]
        return host + "/"
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Obtém ou cria cliente HTTP assíncrono"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.config.timeout),
                verify=True,
                follow_redirects=True,
            )
        return self._client
    
    def _get_auth_xml(self) -> str:
        """Retorna o bloco de autenticação XML"""
        if self.config.include_auth:
            return f'<authentication><logon_info><person_id>{self.config.username}</person_id><password>{self.config.password}</password></logon_info></authentication>'
        return ''
    
    def _create_soap_envelope(self, inner_xml: str, action: str = "ProcessXML") -> str:
        """
        Cria envelope SOAP 1.2 com WS-Addressing.
        O inner_xml deve já conter a autenticação se necessário.
        
        Args:
            inner_xml: XML da requisição
            action: "ProcessXML" ou "ServiceRequest"
        """
        message_id = f"urn:uuid:{uuid.uuid4()}"
        escaped_xml = escape(inner_xml)
        
        if action == "ServiceRequest":
            # ServiceRequest inclui attachments (vazio para download)
            body = f'''<ServiceRequest xmlns="{self.TEMPURI_NS}"><xmlRequest>{escaped_xml}</xmlRequest><attachments xmlns:b="http://schemas.microsoft.com/2003/10/Serialization/Arrays" xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:nil="true"/></ServiceRequest>'''
        else:
            body = f'''<ProcessXML xmlns="{self.TEMPURI_NS}"><xmlRequest>{escaped_xml}</xmlRequest></ProcessXML>'''
        
        return f'''<s:Envelope xmlns:s="{self.SOAP12_NS}" xmlns:a="{self.WSA_NS}"><s:Header><a:Action s:mustUnderstand="1">{self.TEMPURI_NS}IM5Service/{action}</a:Action><a:MessageID>{message_id}</a:MessageID><a:ReplyTo><a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address></a:ReplyTo><a:To s:mustUnderstand="1">{self._service_url}</a:To></s:Header><s:Body>{body}</s:Body></s:Envelope>'''
    
    def _decompress(self, data: bytes) -> bytes:
        """Descomprime dados gzip/deflate"""
        if not data:
            return data
        try:
            return gzip.decompress(data)
        except Exception:
            try:
                decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
                return decompressor.decompress(data[10:])
            except Exception:
                try:
                    return zlib.decompress(data, zlib.MAX_WBITS | 16)
                except Exception:
                    return data
    
    def _extract_xml_response(self, soap_response: str) -> str:
        """Extrai o XMLResponse do envelope SOAP de resposta"""
        try:
            # Procura por XMLResponse ou b:XMLResponse
            for tag in ['<b:XMLResponse>', '<XMLResponse>']:
                if tag in soap_response:
                    start = soap_response.find(tag) + len(tag)
                    end_tag = tag.replace('<', '</')
                    end = soap_response.find(end_tag)
                    if end > start:
                        xml_response = soap_response[start:end]
                        # Unescape
                        xml_response = xml_response.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&').replace('&#xD;', '\r').replace('&#xA;', '\n')
                        return xml_response
            return soap_response
        except Exception:
            return soap_response
    
    def _extract_attachments(self, soap_response: str) -> dict:
        """Extrai attachments da resposta SOAP (base64 encoded)"""
        import base64
        attachments = {}
        
        try:
            # Procura por Attachments no XML
            # Formato WCF: <b:Attachments>...<b:Value>base64data</b:Value>...</b:Attachments>
            # ou <Attachments>...<Value>...</Value>...</Attachments>
            
            for prefix in ['b:', '']:
                attachments_start_tag = f'<{prefix}Attachments'
                attachments_end_tag = f'</{prefix}Attachments>'
                
                if attachments_start_tag in soap_response:
                    start = soap_response.find(attachments_start_tag)
                    end = soap_response.find(attachments_end_tag)
                    
                    if start >= 0 and end > start:
                        attachments_xml = soap_response[start:end + len(attachments_end_tag)]
                        
                        # Extrair valores (KeyValueOfstringbase64Binary)
                        # <d3p1:KeyValueOfstringbase64Binary><d3p1:Key>filename</d3p1:Key><d3p1:Value>base64</d3p1:Value>
                        import re
                        
                        # Padrão para extrair key e value
                        key_pattern = re.compile(r'<[^>]*Key[^>]*>([^<]+)</[^>]*Key[^>]*>')
                        value_pattern = re.compile(r'<[^>]*Value[^>]*>([^<]+)</[^>]*Value[^>]*>')
                        
                        keys = key_pattern.findall(attachments_xml)
                        values = value_pattern.findall(attachments_xml)
                        
                        for i, (key, value) in enumerate(zip(keys, values)):
                            try:
                                decoded = base64.b64decode(value)
                                attachments[key] = decoded
                            except Exception as e:
                                logger.warning(f"Erro ao decodificar attachment {key}: {e}")
                        
                        # Se não encontrou pares key/value, tentar extrair valor direto
                        if not attachments and values:
                            for i, value in enumerate(values):
                                try:
                                    decoded = base64.b64decode(value)
                                    attachments[f"attachment_{i}"] = decoded
                                except Exception:
                                    pass
                        
                        if attachments:
                            break
        except Exception as e:
            logger.error(f"Erro ao extrair attachments: {e}")
        
        return attachments
    
    async def send_request(
        self,
        inner_xml: str,
        retry_count: int = 0
    ) -> FSMResponse:
        """
        Envia uma requisição XML para o FSM.
        O inner_xml deve ser o XML completo incluindo autenticação.
        """
        start_time = datetime.now()
        
        try:
            client = await self._get_client()
            
            # Cria envelope SOAP
            soap_xml = self._create_soap_envelope(inner_xml)
            
            # Comprime
            compressed = gzip.compress(soap_xml.encode("utf-8"))
            
            # Headers
            headers = {
                "Content-Type": "application/x-gzip",
                "Accept-Encoding": "gzip, deflate",
            }
            
            # Envia
            response = await client.post(
                self._service_url,
                content=compressed,
                headers=headers,
            )
            
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            # Processa resposta
            response_content = self._decompress(response.content)
            
            try:
                response_text = response_content.decode("utf-8")
            except Exception:
                response_text = response_content.decode("latin-1", errors="replace")
            
            if response.status_code >= 400:
                logger.error(f"HTTP Error {response.status_code}")
                return FSMResponse(
                    success=False,
                    status_code=response.status_code,
                    response_xml=response_text,
                    duration_ms=duration_ms,
                    error_message=f"HTTP Error: {response.status_code}",
                    error_code=str(response.status_code),
                )
            
            # Extrai resposta
            extracted = self._extract_xml_response(response_text)
            
            # Verifica erros
            error_info = self._check_error(extracted)
            
            return FSMResponse(
                success=error_info is None,
                status_code=response.status_code,
                response_xml=extracted,
                duration_ms=duration_ms,
                error_message=error_info[0] if error_info else None,
                error_code=error_info[1] if error_info else None,
            )
            
        except httpx.TimeoutException:
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            if retry_count < settings.FSM_MAX_RETRY:
                logger.warning(f"Timeout, retrying ({retry_count + 1}/{settings.FSM_MAX_RETRY})")
                await asyncio.sleep(2 ** retry_count)
                return await self.send_request(inner_xml, retry_count + 1)
            
            return FSMResponse(
                success=False,
                status_code=0,
                response_xml="",
                duration_ms=duration_ms,
                error_message=f"Timeout após {settings.FSM_MAX_RETRY} tentativas",
                error_code="TIMEOUT",
            )
            
        except Exception as e:
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            logger.error(f"Erro na requisição: {e}")
            
            return FSMResponse(
                success=False,
                status_code=0,
                response_xml="",
                duration_ms=duration_ms,
                error_message=str(e),
                error_code="CONNECTION_ERROR",
            )
    
    def _check_error(self, xml: str) -> Optional[Tuple[str, str]]:
        """Verifica se a resposta contém erro."""
        try:
            if not xml or not xml.strip():
                return None
            
            # Verifica por mensagens de erro comuns
            if 'type="Exception"' in xml or '<error>' in xml:
                try:
                    root = etree.fromstring(xml.encode())
                    
                    # Procura mensagem de erro
                    for path in ['.//system_error/message', './/interface_error/message', './/message']:
                        elem = root.find(path)
                        if elem is not None and elem.text:
                            severity = root.findtext('.//severity', 'ERROR')
                            return (elem.text, severity)
                except Exception:
                    pass

                # Fallback: extrai mensagem via regex
                import re
                match = re.search(r'<message>([^<]+)</message>', xml)
                if match:
                    return (match.group(1), 'ERROR')

            return None
        except Exception:
            return None
    
    async def test_connection(self) -> Tuple[bool, str]:
        """
        Testa a conexão com o FSM usando perform_exec_db_query com 'select 1'.
        """
        auth = self._get_auth_xml()
        test_xml = f'''<perform_exec_db_query>{auth}<parameters><sql_command>select 1</sql_command></parameters></perform_exec_db_query>'''
        
        response = await self.send_request(test_xml)
        
        if response.status_code == 200:
            if response.success:
                return (True, "Conexão bem sucedida")
            else:
                # Erro de negócio mas conectou
                return (False, response.error_message or "Erro na execução")
        else:
            return (False, response.error_message or f"Erro HTTP: {response.status_code}")
    
    async def execute_query(
        self,
        sql: str,
        raw_mode: bool = False,
        allow_non_select: bool = False
    ) -> FSMResponse:
        """
        Executa uma query SQL no FSM.
        
        Args:
            sql: O comando SQL a ser executado
            raw_mode: Se True, envia o SQL sem preprocessamento (para casos especiais)
            allow_non_select: Quando True, permite enviar EXEC/DML/DDL sem tentar converter para SELECT
        """
        auth = self._get_auth_xml()
        
        # Preprocessa o SQL se n?o estiver em modo raw
        if not raw_mode:
            sql = self._preprocess_sql(sql, enforce_select_only=not allow_non_select)
        
        # Usar CDATA para proteger SQL (igual ao execute_edit)
        if ']]>' in sql:
            sql_parts = sql.split(']]>')
            sql_cdata = '<![CDATA[' + ']]><![CDATA['.join(sql_parts) + ']]>'
        else:
            sql_cdata = f'<![CDATA[{sql}]]>'
        
        xml = f'''<perform_exec_db_query>{auth}<parameters><sql_command>{sql_cdata}</sql_command></parameters></perform_exec_db_query>'''
        
        # 🔍 LOG: Para EXEC statements, mostrar o XML sendo enviado ao FSM
        if sql.strip().upper().startswith('EXEC'):
            logger.info(f"[FSM Client] Enviando EXEC ao FSM")
            logger.info(f"[FSM Client] SQL (dentro de CDATA): {sql[:500]}")
            logger.debug(f"[FSM Client] XML completo: {xml[:1000]}")
        
        return await self.send_request(xml)
    

    async def execute_edit(self, sql: str, raw_mode: bool = False) -> FSMResponse:
        """
        Executa comandos DML/EXEC no FSM usando perform_exec_db_edit.
        
        CRÍTICO: Preserva quebras de linha dentro de strings SQL (XML formatado).
        """
        # DEBUG: Log para SQL com campos XML antes de enviar ao FSM
        xml_fields_pattern = r'(table_script|rule_note|value|xml_message|validation_xml|alternate_xml|process_xml)'
        has_xml_fields = re.search(xml_fields_pattern, sql, re.IGNORECASE)
        
        if has_xml_fields:
            logger.info(f"=== DEBUG: ENVIANDO SQL AO FSM (execute_edit) ===")
            logger.info(f"SQL length: {len(sql)}")
            logger.info(f"SQL has newlines: {chr(10) in sql}")
            logger.info(f"SQL has tabs: {chr(9) in sql}")
            
            # Procurar por campos XML no SQL
            for field_name in ['table_script', 'rule_note', 'value', 'xml_message', 'validation_xml', 'alternate_xml', 'process_xml']:
                # Padrão para encontrar o valor do campo dentro de VALUES
                pattern = rf"{field_name}.*?VALUES\s*\(.*?'(.*?)'"
                match = re.search(pattern, sql, re.DOTALL | re.IGNORECASE)
                if match:
                    field_value = match.group(1)
                    logger.info(f"=== Campo XML {field_name} no SQL ===")
                    logger.info(f"Length: {len(field_value)}")
                    logger.info(f"Has newlines: {chr(10) in field_value}")
                    logger.info(f"Has tabs: {chr(9) in field_value}")
                    logger.info(f"Has 4-space indent: {'    ' in field_value}")
                    logger.info(f"Has 2-space indent: {'  ' in field_value}")
                    logger.info(f"First 300 chars: {repr(field_value[:300])}")
                    logger.info(f"=== FIM Campo XML {field_name} ===")
            logger.info(f"=== FIM DEBUG ENVIANDO SQL ===")
        
        auth = self._get_auth_xml()
        
        if not raw_mode:
            # IMPORTANTE: _preprocess_sql agora preserva quebras de linha dentro de strings SQL
            sql_preprocessed = self._preprocess_sql(sql, enforce_select_only=False)
            
            # DEBUG: Log após preprocessamento
            if has_xml_fields:
                logger.info(f"=== DEBUG: SQL APÓS PREPROCESSAMENTO ===")
                logger.info(f"SQL preprocessed length: {len(sql_preprocessed)}")
                logger.info(f"SQL preprocessed has newlines: {chr(10) in sql_preprocessed}")
                logger.info(f"SQL preprocessed has tabs: {chr(9) in sql_preprocessed}")
                
                # Procurar por campos XML no SQL preprocessado
                for field_name in ['table_script', 'rule_note', 'value', 'xml_message', 'validation_xml', 'alternate_xml', 'process_xml']:
                    pattern = rf"{field_name}.*?VALUES\s*\(.*?'(.*?)'"
                    match = re.search(pattern, sql_preprocessed, re.DOTALL | re.IGNORECASE)
                    if match:
                        field_value = match.group(1)
                        logger.info(f"=== Campo XML {field_name} APÓS PREPROCESSAMENTO ===")
                        logger.info(f"Length: {len(field_value)}")
                        logger.info(f"Has newlines: {chr(10) in field_value}")
                        logger.info(f"Has tabs: {chr(9) in field_value}")
                        logger.info(f"Has 4-space indent: {'    ' in field_value}")
                        logger.info(f"Has 2-space indent: {'  ' in field_value}")
                        logger.info(f"First 300 chars: {repr(field_value[:300])}")
                        logger.info(f"=== FIM Campo XML {field_name} APÓS PREPROCESSAMENTO ===")
                logger.info(f"=== FIM DEBUG SQL PREPROCESSADO ===")
            
            sql = sql_preprocessed
        
        if ']]>' in sql:
            # Caso raro: SQL contém "]]>" - dividir em múltiplos blocos CDATA
            sql_parts = sql.split(']]>')
            sql_cdata = '<![CDATA[' + ']]><![CDATA['.join(sql_parts) + ']]>'
        else:
            sql_cdata = f'<![CDATA[{sql}]]>'
        
        # DEBUG: Log do SQL antes e depois do CDATA
        if has_xml_fields:
            logger.info(f"=== DEBUG: SQL ANTES DO CDATA ===")
            logger.info(f"SQL length: {len(sql)}")
            logger.info(f"SQL has newlines: {chr(10) in sql}")
            logger.info(f"SQL has tabs: {chr(9) in sql}")
            logger.info(f"SQL has XML comments: {'<!--' in sql}")
            
            # Procurar por campos XML no SQL antes do CDATA
            for field_name in ['table_script', 'rule_note', 'value', 'xml_message', 'validation_xml', 'alternate_xml', 'process_xml']:
                pattern = rf"{field_name}.*?VALUES\s*\(.*?'(.*?)'"
                match = re.search(pattern, sql, re.DOTALL | re.IGNORECASE)
                if match:
                    field_value = match.group(1)
                    logger.info(f"=== Campo XML {field_name} ANTES CDATA ===")
                    logger.info(f"Length: {len(field_value)}")
                    logger.info(f"Has newlines: {chr(10) in field_value}")
                    logger.info(f"Has tabs: {chr(9) in field_value}")
                    logger.info(f"Has XML comments: {'<!--' in field_value}")
                    logger.info(f"Has 4-space indent: {'    ' in field_value}")
                    logger.info(f"Has 2-space indent: {'  ' in field_value}")
                    logger.info(f"First 300 chars: {repr(field_value[:300])}")
                    logger.info(f"=== FIM Campo XML {field_name} ANTES CDATA ===")
            logger.info(f"=== FIM DEBUG SQL ANTES CDATA ===")
            
            logger.info(f"=== DEBUG: SQL APÓS CDATA ===")
            logger.info(f"SQL CDATA length: {len(sql_cdata)}")
            logger.info(f"SQL CDATA has newlines: {chr(10) in sql_cdata}")
            logger.info(f"SQL CDATA has tabs: {chr(9) in sql_cdata}")
            logger.info(f"=== FIM DEBUG SQL APÓS CDATA ===")
        
        xml = f'''<perform_exec_db_edit>{auth}<parameters><sql_command>{sql_cdata}</sql_command></parameters></perform_exec_db_edit>'''
        
        # 🔍 LOG: Para EXEC statements, mostrar o XML sendo enviado ao FSM
        if sql.strip().upper().startswith('EXEC'):
            logger.info(f"[FSM Client] Enviando EXEC ao FSM via perform_exec_db_edit")
            logger.info(f"[FSM Client] SQL (dentro de CDATA): {sql[:500]}")
            logger.debug(f"[FSM Client] XML completo: {xml[:1000]}")
        
        # DEBUG: Log do XML final antes de enviar
        if has_xml_fields:
            logger.info(f"=== DEBUG: XML FINAL ANTES DE ENVIAR AO FSM ===")
            logger.info(f"XML length: {len(xml)}")
            logger.info(f"XML has newlines: {chr(10) in xml}")
            logger.info(f"XML has tabs: {chr(9) in xml}")
            
            # Procurar por campos XML no XML final (pode estar escapado)
            for field_name in ['table_script', 'rule_note', 'value', 'xml_message', 'validation_xml', 'alternate_xml', 'process_xml']:
                # No XML, o SQL está dentro de <sql_command>, então procuramos diretamente
                pattern = rf"{field_name}.*?VALUES.*?'(.*?)'"
                match = re.search(pattern, xml, re.DOTALL | re.IGNORECASE)
                if match:
                    field_value = match.group(1)
                    logger.info(f"=== Campo XML {field_name} NO XML FINAL ===")
                    logger.info(f"Length: {len(field_value)}")
                    logger.info(f"Has newlines: {chr(10) in field_value}")
                    logger.info(f"Has tabs: {chr(9) in field_value}")
                    logger.info(f"Has 4-space indent: {'    ' in field_value}")
                    logger.info(f"Has 2-space indent: {'  ' in field_value}")
                    logger.info(f"First 300 chars: {repr(field_value[:300])}")
                    logger.info(f"=== FIM Campo XML {field_name} NO XML FINAL ===")
            logger.info(f"=== FIM DEBUG XML FINAL ===")
        
        return await self.send_request(xml)
    
    def _preprocess_sql(self, sql: str, enforce_select_only: bool = True) -> str:

        import re
        
        # Detectar se o SQL contém campos XML conhecidos
        xml_fields_pattern = r'(table_script|rule_note|value|xml_message|validation_xml|alternate_xml|process_xml)'
        has_xml_fields = re.search(xml_fields_pattern, sql, re.IGNORECASE)
        
        if has_xml_fields:
            lines = sql.split('\n')
            processed_lines = []
            in_string = False  # Estado preservado entre linhas - NÃO resetar!
            string_char = None  # Caractere de string (' ou ") preservado entre linhas - NÃO resetar!
            
            for line in lines:
                # Processa comentários de linha, mas cuidado com strings
                result = []
                i = 0
                
                while i < len(line):
                    char = line[i]
                    
                    # Detecta início/fim de string
                    if char in ("'", '"') and not in_string:
                        in_string = True
                        string_char = char
                        result.append(char)
                    elif char == string_char and in_string:
                        # Verifica se é escape ('')
                        if i + 1 < len(line) and line[i + 1] == string_char:
                            result.append(char)
                            result.append(char)
                            i += 1
                        else:
                            in_string = False
                            string_char = None
                            result.append(char)
                    elif not in_string and char == '-' and i + 1 < len(line) and line[i + 1] == '-':
                        # CRÍTICO: Verificar se não é parte de comentário XML (<!--)
                        # Comentário XML começa com <!-- e NÃO deve ser removido
                        # Comentário SQL começa com -- (sem < antes)
                        # IMPORTANTE: Se estamos dentro de uma string SQL (in_string == True),
                        # NUNCA remover -- como comentário, pois pode ser parte de comentário XML
                        if i > 0 and line[i - 1] == '<':
                            # É parte de comentário XML (<!--), preservar
                            result.append(char)
                        else:
                            # É comentário SQL (--), ignorar o resto da linha
                            break
                    elif in_string and char == '-' and i + 1 < len(line) and line[i + 1] == '-':
                        result.append(char)
                    else:
                        result.append(char)
                    
                    i += 1
                
                processed_line = ''.join(result)
                processed_lines.append(processed_line)
            
            sql = '\n'.join(processed_lines)
            sql = sql.strip()
        else:
            # Para SQL sem campos XML, fazer processamento normal
            # (código original para compatibilidade)
            lines = sql.split('\n')
            processed_lines = []
            
            for line in lines:
                result = []
                in_string = False
                string_char = None
                i = 0
                
                while i < len(line):
                    char = line[i]
                    
                    if char in ("'", '"') and not in_string:
                        in_string = True
                        string_char = char
                        result.append(char)
                    elif char == string_char and in_string:
                        if i + 1 < len(line) and line[i + 1] == string_char:
                            result.append(char)
                            result.append(char)
                            i += 1
                        else:
                            in_string = False
                            string_char = None
                            result.append(char)
                    elif not in_string and char == '-' and i + 1 < len(line) and line[i + 1] == '-':
                        break
                    else:
                        result.append(char)
                    
                    i += 1
                
                processed_line = ''.join(result).rstrip()
                if processed_line:
                    processed_lines.append(processed_line)
            
            sql = '\n'.join(processed_lines)
            
            # Normalizar espaços apenas para SQL sem campos XML
            sql = re.sub(r'[ \t]{2,}', ' ', sql)
            sql = sql.strip()
        
        if enforce_select_only:
            # Converte EXEC comuns para SELECT equivalentes (FSM s? aceita SELECT)
            sql = self._convert_exec_to_select(sql)
        
        return sql
    
    def _convert_exec_to_select(self, sql: str) -> str:
        """
        Converte comandos EXEC comuns para SELECT equivalentes.
        FSM perform_exec_db_query só aceita SELECT, então convertemos
        procedures comuns para consultas equivalentes.
        """
        import re
        
        sql_upper = sql.upper().strip()
        
        # sp_helptext 'object_name' -> SELECT definition from sys.sql_modules
        match = re.match(r"^EXEC(?:UTE)?\s+SP_HELPTEXT\s+['\"]?([^'\";\s]+)['\"]?", sql, re.IGNORECASE)
        if match:
            object_name = match.group(1)
            return f"SELECT definition AS [Text] FROM sys.sql_modules WHERE object_id = OBJECT_ID('{object_name}')"
        
        # sp_help 'object_name' -> SELECT column info
        match = re.match(r"^EXEC(?:UTE)?\s+SP_HELP\s+['\"]?([^'\";\s]+)['\"]?", sql, re.IGNORECASE)
        if match:
            object_name = match.group(1)
            return f"""SELECT 
                c.name AS Column_name,
                t.name AS Type,
                c.max_length AS Length,
                c.precision AS Prec,
                c.scale AS Scale,
                CASE c.is_nullable WHEN 1 THEN 'yes' ELSE 'no' END AS Nullable
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE c.object_id = OBJECT_ID('{object_name}')
            ORDER BY c.column_id"""
        
        # sp_columns 'table_name' -> SELECT column info
        match = re.match(r"^EXEC(?:UTE)?\s+SP_COLUMNS\s+['\"]?([^'\";\s]+)['\"]?", sql, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            return f"""SELECT 
                c.name AS COLUMN_NAME,
                t.name AS TYPE_NAME,
                c.max_length AS LENGTH,
                c.precision AS PRECISION,
                c.scale AS SCALE,
                CASE c.is_nullable WHEN 1 THEN 1 ELSE 0 END AS NULLABLE
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE c.object_id = OBJECT_ID('{table_name}')
            ORDER BY c.column_id"""
        
        # sp_tables -> SELECT tables
        if re.match(r"^EXEC(?:UTE)?\s+SP_TABLES\s*$", sql, re.IGNORECASE):
            return """SELECT 
                s.name AS TABLE_OWNER,
                t.name AS TABLE_NAME,
                CASE t.type WHEN 'U' THEN 'TABLE' WHEN 'V' THEN 'VIEW' END AS TABLE_TYPE
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            UNION ALL
            SELECT 
                s.name AS TABLE_OWNER,
                v.name AS TABLE_NAME,
                'VIEW' AS TABLE_TYPE
            FROM sys.views v
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            ORDER BY TABLE_NAME"""
        
        # sp_who / sp_who2 -> SELECT session info
        if re.match(r"^EXEC(?:UTE)?\s+SP_WHO2?\s*$", sql, re.IGNORECASE):
            return """SELECT 
                s.session_id AS SPID,
                s.login_name AS LoginName,
                s.host_name AS HostName,
                DB_NAME(s.database_id) AS DBName,
                s.status AS Status,
                s.program_name AS ProgramName
            FROM sys.dm_exec_sessions s
            WHERE s.session_id > 50
            ORDER BY s.session_id"""
        
        # sp_depends 'object_name' -> SELECT dependencies
        match = re.match(r"^EXEC(?:UTE)?\s+SP_DEPENDS\s+['\"]?([^'\";\s]+)['\"]?", sql, re.IGNORECASE)
        if match:
            object_name = match.group(1)
            return f"""SELECT DISTINCT
                OBJECT_NAME(d.referencing_id) AS referencing_object,
                OBJECT_NAME(d.referenced_id) AS referenced_object,
                d.referenced_entity_name
            FROM sys.sql_expression_dependencies d
            WHERE d.referenced_id = OBJECT_ID('{object_name}')
               OR d.referencing_id = OBJECT_ID('{object_name}')"""
        
        # Se começa com EXEC mas não é um dos suportados, tenta retornar erro informativo
        if sql_upper.startswith('EXEC'):
            logger.warning(f"EXEC command not supported by FSM perform_exec_db_query: {sql[:100]}")
        
        return sql
    
    async def process_xml(self, xml: str) -> FSMResponse:
        """
        Processa XML de perform.
        Adiciona autenticação ao XML se necessário.
        """
        auth = self._get_auth_xml()
        
        # Adiciona autenticação ao XML se ainda não tiver
        if auth and '<authentication>' not in xml:
            xml = self._inject_auth(xml, auth)
        
        return await self.send_request(xml)
    
    def _inject_auth(self, xml: str, auth: str) -> str:
        """
        Injeta autenticação no XML do perform.
        A autenticação deve ficar logo após a tag de abertura do perform.
        """
        xml = xml.strip()
        
        # Remove declaração XML se existir
        if xml.startswith('<?xml'):
            end_decl = xml.find('?>') + 2
            xml = xml[end_decl:].strip()
        
        # Verifica se é self-closing tag (ex: <perform_xyz />)
        if xml.rstrip().endswith('/>'):
            # Converte self-closing para tag normal com auth
            # <perform_xyz /> -> <perform_xyz>auth</perform_xyz>
            close_pos = xml.rfind('/>')
            tag_start = xml.find('<') + 1
            space_pos = xml.find(' ', tag_start)
            gt_pos = xml.find('>', tag_start)
            
            if space_pos > 0 and space_pos < gt_pos:
                tag_name = xml[tag_start:space_pos]
            else:
                tag_name = xml[tag_start:close_pos].strip()
            
            # Remove /> e adiciona auth + closing tag
            return xml[:close_pos].rstrip() + '>' + auth + f'</{tag_name}>'
        
        # Tag normal: encontra a primeira tag e injeta auth logo após
        first_gt = xml.find('>')
        if first_gt > 0:
            return xml[:first_gt + 1] + auth + xml[first_gt + 1:]
        return xml
    
    async def hierarchy_select(self, table: str, max_rows: int = 100, where: str = None) -> FSMResponse:
        """Executa uma hierarchy_select."""
        auth = self._get_auth_xml()
        where_clause = f"<where>{where}</where>" if where else ""
        xml = f'''<hierarchy_select max_rows="{max_rows}">{auth}<primary_table>{table}</primary_table>{where_clause}</hierarchy_select>'''
        return await self.send_request(xml)
    
    # Aliases para compatibilidade
    async def send_fsm_import(self, fsm_import_xml: str) -> FSMResponse:
        """Envia XML de perform para o FSM."""
        return await self.process_xml(fsm_import_xml)
    
    async def send_query(self, query_xml: str) -> FSMResponse:
        return await self.send_request(query_xml)
    
    async def service_request(self, xml: str) -> FSMResponse:
        """
        Envia uma requisição ServiceRequest (para operações com attachments como get_server_log).
        """
        start_time = datetime.now()
        
        try:
            client = await self._get_client()
            
            # Adiciona autenticação se necessário
            auth = self._get_auth_xml()
            if auth and '<authentication>' not in xml:
                xml = self._inject_auth(xml, auth)
            
            # Cria envelope SOAP com action ServiceRequest
            soap_xml = self._create_soap_envelope(xml, action="ServiceRequest")
            
            # Comprime
            compressed = gzip.compress(soap_xml.encode("utf-8"))
            
            # Headers
            headers = {
                "Content-Type": "application/x-gzip",
                "Accept-Encoding": "gzip, deflate",
            }
            
            
            # Envia
            response = await client.post(
                self._service_url,
                content=compressed,
                headers=headers,
            )
            
            duration = int((datetime.now() - start_time).total_seconds() * 1000)
            
            # Processa resposta
            response_data = response.content
            if response_data:
                response_data = self._decompress(response_data)
            
            soap_response = response_data.decode("utf-8", errors="replace") if response_data else ""
            
            # Extrai XMLResponse e Attachments
            xml_response = self._extract_xml_response(soap_response)
            attachments = self._extract_attachments(soap_response)
            
            
            # Verifica erros
            success = response.status_code == 200
            error_message = None
            
            if not success or "<fault" in xml_response.lower() or "<error" in xml_response.lower():
                success = False
                # Tenta extrair mensagem de erro
                import re
                error_match = re.search(r'<(?:error_)?message[^>]*>([^<]+)</(?:error_)?message>', xml_response, re.IGNORECASE)
                if error_match:
                    error_message = error_match.group(1)
                else:
                    fault_match = re.search(r'<faultstring[^>]*>([^<]+)</faultstring>', xml_response, re.IGNORECASE)
                    if fault_match:
                        error_message = fault_match.group(1)
            
            return FSMResponse(
                success=success,
                status_code=response.status_code,
                response_xml=xml_response,
                duration_ms=duration,
                error_message=error_message,
                attachments=attachments
            )
            
        except Exception as e:
            duration = int((datetime.now() - start_time).total_seconds() * 1000)
            logger.error(f"Erro em ServiceRequest: {e}")
            return FSMResponse(
                success=False,
                status_code=0,
                response_xml="",
                duration_ms=duration,
                error_message=str(e),
                attachments={}
            )
    
    async def close(self):
        """Fecha o cliente HTTP"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
