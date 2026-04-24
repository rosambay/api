from typing import List, Dict, Any
import re
import xml.etree.ElementTree as ET
import html
from datetime import datetime
from loguru import logger
from fsm_client import FSMClient, FSMConnectionConfig, FSMTarget
from config import settings

_SESSION_RE = re.compile(r'^[A-Za-z0-9_\-]{1,128}$')
_TOKEN_RE   = re.compile(r'^[A-Za-z0-9_\-\.]{1,256}$')

def _validate_session(value: str, label: str = "session") -> str:
    """Valida session_id/token contra um padrão seguro e retorna o valor."""
    if not value or not _SESSION_RE.match(value):
        raise ValueError(f"Valor inválido para '{label}': contém caracteres não permitidos.")
    return value

def _validate_datetime(value: str, label: str = "dateTime") -> str:
    """Valida e re-formata um datetime ISO para evitar injeção SQL."""
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        raise ValueError(f"Valor inválido para '{label}': formato de data esperado ISO 8601.")

def get_element_full_text(element) -> str:
    """
    Extrai o conteúdo completo de um elemento XML, incluindo:
    - Texto simples
    - Conteúdo XML/JSON embutido
    - Elementos filhos serializados
    
    Isso é necessário para tabelas como INTEGRATION_LOG_DETAIL que contêm
    mensagens XML completas nas colunas.
    
    CRÍTICO: Preserva formatação original do XML (indentação, espaços entre tags, etc.)
    para evitar que XML formatado seja normalizado durante o parse.
    """
    # Se não tem filhos, retorna apenas o texto
    if len(element) == 0:
        return element.text or ""
    
    # Se tem filhos, serializa todo o conteúdo interno
    # Isso preserva XML/JSON embutido
    try:
        # CRÍTICO: ET.tostring pode normalizar formatação (remover espaços entre tags)
        # Para preservar formatação original, tentar usar método que preserva espaços
        # Pega o texto inicial (antes do primeiro filho)
        text_parts = []
        if element.text:
            text_parts.append(element.text)
        
        # Serializa cada filho
        for child in element:
            # CRÍTICO: ET.tostring com method='xml' pode normalizar formatação
            # Tentar preservar formatação usando pretty_print=False (padrão)
            # Mas isso ainda pode normalizar espaços entre tags
            # Para XML que contém formatação importante, precisamos preservar o original
            child_str = ET.tostring(child, encoding='unicode', method='xml')
            text_parts.append(child_str)
            # Pega o texto após o elemento (tail)
            if child.tail:
                text_parts.append(child.tail)
        
        return ''.join(text_parts)
    except Exception:
        # Fallback: apenas texto
        return element.text or ""


def get_tag_name(tag: str) -> str:
    """Remove namespace de uma tag XML e retorna apenas o nome."""
    if not tag:
        return ""
    if '}' in tag:
        return tag.split('}', 1)[1]
    return tag


def parse_xml_dataset(xml_response: str) -> tuple[List[str], List[Dict[str, Any]]]:
    """
    Parse XML response do FSM (formato NewDataSet) para lista de dicionários.
    O FSM retorna o XML dentro de <response> com HTML encoding.
    
    IMPORTANTE: Suporta colunas que contêm XML/JSON (como INTEGRATION_LOG_DETAIL).
    """
    columns = []
    rows = []
    
    try:
        # LOG: XML recebido
        #logger.debug(f"[parse_xml_dataset] XML recebido - length: {len(xml_response)} chars")
        #logger.debug(f"[parse_xml_dataset] Primeiros 1000 chars: {xml_response[:1000]}")
        
        # Limpa o XML
        xml_response = xml_response.strip()
        if not xml_response:
            logger.warning(f"[parse_xml_dataset] XML vazio!")
            return columns, rows
        
        # Primeiro, parse o XML externo (perform_exec_db_query_result)
        root = ET.fromstring(xml_response)
        #logger.debug(f"[parse_xml_dataset] Root tag: {root.tag}")
        
        # Procura pela tag <response> que contém o XML encodado
        # Usar findall e pegar o primeiro para evitar erro "Sequence contains more than one matching element"
        response_elems = root.findall(".//response")
        response_elem = response_elems[0] if response_elems else None
        
        # LOG: Verificar se existe <response>
        if response_elem is None:
            logger.warning(f"[parse_xml_dataset] Tag <response> não encontrada!")
            return columns, rows
        
        if not response_elem.text:
            logger.warning(f"[parse_xml_dataset] Tag <response> está vazia!")
            return columns, rows
            
        #logger.debug(f"[parse_xml_dataset] Tag <response> encontrada com {len(response_elem.text)} chars")
        
        if response_elem is not None and response_elem.text:
            # Decodifica HTML entities
            inner_xml = html.unescape(response_elem.text)
            inner_xml = inner_xml.strip()
            
            # LOG: Inner XML decodificado
            #logger.debug(f"[parse_xml_dataset] Inner XML decodificado - length: {len(inner_xml)} chars")
            #logger.debug(f"[parse_xml_dataset] Inner XML primeiros 1500 chars: {inner_xml[:1500]}")
            
            if inner_xml.startswith('\ufeff'):
                inner_xml = inner_xml.lstrip('\ufeff')
            # Alguns performs (como exec_db_edit) retornam "-1" antes do XML real.
            if inner_xml.startswith('-1'):
                remaining = inner_xml[2:].lstrip()
                if remaining.startswith('<'):
                    inner_xml = remaining
                else:
                    # Apenas contador de linhas afetadas, sem dataset.
                    logger.info(f"[parse_xml_dataset] Retornou apenas contador de linhas (-1), sem dataset")
                    return columns, rows
            
            schema_cols = re.findall(r'<xs:element name="([^"]+)"', inner_xml)
            for col in schema_cols:
                clean_col = col.split('}', 1)[-1]
                if clean_col not in ["NewDataSet", "execute_sql_result"] and clean_col not in columns:
                    columns.append(clean_col)
            
            # Se não encontrou colunas no schema via regex, tentar parsear com ET.fromstring
            # (mas só se o XML estiver bem formado - se falhar, continuar com regex)
            if not columns:
                try:
                    inner_root = ET.fromstring(inner_xml)
                    schema = inner_root.find(".//{http://www.w3.org/2001/XMLSchema}schema")
                    if schema is not None:
                        for element in schema.findall(".//{http://www.w3.org/2001/XMLSchema}element[@name]"):
                            name = element.get("name")
                            if name:
                                clean_name = get_tag_name(name)
                                if clean_name and clean_name not in ["NewDataSet", "execute_sql_result"] and clean_name not in columns:
                                    columns.append(clean_name)
                except ET.ParseError:
                    # XML malformado - continuar com regex apenas
                    # Isso é comum quando campos XML contêm comentários incompletos
                    pass
            
            result_pattern = r'<(?:\w+:)?execute_sql_result>(.*?)</(?:\w+:)?execute_sql_result>'
            results = re.findall(result_pattern, inner_xml, re.DOTALL)
            
            # 🔍 LOG: Quantos resultados encontrados
            logger.info(f"[parse_xml_dataset] Regex encontrou {len(results)} <execute_sql_result> tags")
            if len(results) == 0:
                logger.warning(f"[parse_xml_dataset] Nenhum <execute_sql_result> encontrado!")
                #logger.debug(f"[parse_xml_dataset] Inner XML estrutura: {inner_xml[:2000]}")
            
            for result_content in results:
                row = {}
                
                # Identificar campos XML conhecidos que podem conter tags aninhadas
                xml_fields = {'value', 'table_script', 'rule_note', 'xml_message', 'validation_xml', 'alternate_xml', 'process_xml'}
                
                # Extrair campos XML primeiro e protegê-los
                xml_field_contents = {}
                for xml_field in xml_fields:
                    # Procurar por campo XML de primeiro nível
                    xml_field_pattern = rf'<{re.escape(xml_field)}>(.*?)</{re.escape(xml_field)}>'
                    xml_field_match = re.search(xml_field_pattern, result_content, re.DOTALL)
                    if xml_field_match:
                        # Extrair e proteger o conteúdo do campo XML
                        xml_field_contents[xml_field] = xml_field_match.group(0)  # Tag completa
                        # Substituir temporariamente por placeholder para evitar matching interno
                        placeholder = f'__XML_FIELD_{xml_field}_{id(xml_field_contents)}__'
                        result_content = result_content.replace(xml_field_contents[xml_field], placeholder)
                
                # Para cada coluna conhecida do schema, extrai o valor usando regex
                # Isso preserva formatação original (indentação, espaços entre tags, etc.)
                for col in columns:
                    search_content = result_content
                    if col in xml_field_contents:
                        # Restaurar o campo XML antes de fazer matching
                        placeholder = f'__XML_FIELD_{col}_{id(xml_field_contents)}__'
                        if placeholder in search_content:
                            search_content = search_content.replace(placeholder, xml_field_contents[col])
                    
                    # CRÍTICO: Procurar por CDATA primeiro (mais comum e preserva melhor)
                    # Padrão: <col><![CDATA[conteúdo]]></col>
                    # IMPORTANTE: NÃO usar \s* antes/depois do CDATA para preservar linhas vazias
                    cdata_pattern = rf'<{re.escape(col)}><!\[CDATA\[(.*?)\]\]></{re.escape(col)}>'
                    cdata_match = re.search(cdata_pattern, search_content, re.DOTALL)
                    
                    if cdata_match:
                        # Conteúdo está em CDATA - extrair diretamente
                        extracted_value = cdata_match.group(1)
                    else:
                        # Não está em CDATA - usar padrão normal
                        # CRÍTICO: Procurar apenas tags de PRIMEIRO NÍVEL (não dentro de campos XML)
                        # Usar regex não-greedy para pegar apenas o primeiro nível
                        # e evitar pegar tags dentro de campos XML
                        # CRÍTICO: Usar re.DOTALL para capturar quebras de linha dentro de XML
                        col_pattern = rf'<{re.escape(col)}>(.*?)</{re.escape(col)}>'
                        match = re.search(col_pattern, search_content, re.DOTALL)
                        if match:
                            extracted_value = match.group(1)
                        else:
                            # Tag vazia ou não encontrada
                            extracted_value = None
                    
                    if extracted_value is not None:
                        is_placeholder = False
                        for xml_field in xml_fields:
                            placeholder = f'__XML_FIELD_{xml_field}_{id(xml_field_contents)}__'
                            if extracted_value == placeholder or placeholder in extracted_value:
                                # Valor foi extraído de dentro de um campo XML - IGNORAR
                                is_placeholder = True
                                logger.warning(f"parse_xml_dataset: Ignorando campo '{col}' com valor extraído de dentro do campo XML '{xml_field}' (não existe no banco)")
                                break
                        
                        if is_placeholder:
                            # Não incluir este campo - foi extraído incorretamente de dentro de um campo XML
                            continue
                        
                        # Se o campo é um campo XML protegido, restaurar o conteúdo original
                        if col in xml_field_contents:
                            # Este é o próprio campo XML - extrair o valor real (sem as tags externas)
                            xml_field_match = re.search(rf'<{re.escape(col)}>(.*?)</{re.escape(col)}>', xml_field_contents[col], re.DOTALL)
                            if xml_field_match:
                                extracted_value = xml_field_match.group(1)
                            else:
                                extracted_value = xml_field_contents[col]
                        
                        # DEBUG: Log para table_script
                        if col.lower() == 'table_script' and extracted_value:
                            logger.info(f"=== DEBUG: table_script EXTRAÍDO PELO REGEX ===")
                            logger.info(f"Length: {len(extracted_value)}")
                            logger.info(f"Has newlines: {chr(10) in extracted_value}")
                            logger.info(f"Has carriage return: {chr(13) in extracted_value}")
                            logger.info(f"Has tabs: {chr(9) in extracted_value}")
                            logger.info(f"Has 4-space indent: {'    ' in extracted_value}")
                            logger.info(f"First 300 chars: {repr(extracted_value[:300])}")
                            logger.info(f"=== FIM table_script EXTRAÍDO ===")
                        
                        row[col] = extracted_value
                    else:
                        # Verifica se é tag vazia ou auto-fechada
                        if f'<{col}/>' in result_content or f'<{col} />' in result_content:
                            row[col] = ""
                
                if row:
                    rows.append(row)
            
            # LOG: Resultado do regex parsing
            logger.info(f"[parse_xml_dataset] Após regex parsing - columns={len(columns)}, rows={len(rows)}")
            # if len(rows) > 0:
            #     logger.debug(f"[parse_xml_dataset] Primeira row: {list(rows[0].keys())}")
            
            # Se encontrou resultados com regex, retornar (mais robusto e preserva formatação)
            if rows:
                logger.info(f"[parse_xml_dataset] Retornando {len(rows)} rows com {len(columns)} columns via regex")
                return columns, rows
            
            # LOG: Fallback para ET.fromstring
            logger.warning(f"[parse_xml_dataset] Regex não encontrou rows, tentando fallback com ET.fromstring")
            
            # Fallback: se regex não encontrou resultados, tentar ET.fromstring
            # (pode normalizar, mas é melhor que nada)
            try:
                inner_root = ET.fromstring(inner_xml)
                for result in inner_root.iter():
                    if get_tag_name(result.tag) != "execute_sql_result":
                        continue
                    row = {}
                    for child in result:
                        tag = get_tag_name(child.tag)
                        if len(child) == 0:
                            # Sem filhos - usar text diretamente (preserva formatação)
                            text = child.text or ""
                        else:
                            # Com filhos - ET.tostring pode normalizar, mas é necessário para XML aninhado
                            # Tentar preservar o máximo possível da formatação original
                            text = get_element_full_text(child)
                        row[tag] = text
                        if tag not in columns:
                            columns.append(tag)
                    if row:
                        rows.append(row)
            except ET.ParseError as inner_error:
                # Se falhar o parse interno, tenta extrair com regex
                snippet = inner_xml[:500].replace('\n', '\\n') if 'inner_xml' in locals() else ''
                length_info = len(inner_xml) if 'inner_xml' in locals() else 0
                logger.warning(f"Inner XML parse failed, trying regex: {inner_error}. Length: {length_info}. Snippet: {snippet}")
                
                # Extrai colunas do schema se possível
                schema_cols = re.findall(r'<xs:element name="([^"]+)"', inner_xml)
                for col in schema_cols:
                    clean_col = col.split('}', 1)[-1]
                    if clean_col not in ["NewDataSet", "execute_sql_result"] and clean_col not in columns:
                        columns.append(clean_col)
                
                # Extrai resultados com regex mais robusto
                result_pattern = r'<(?:\w+:)?execute_sql_result>(.*?)</(?:\w+:)?execute_sql_result>'
                results = re.findall(result_pattern, inner_xml, re.DOTALL)
                
                for result_content in results:
                    row = {}
                    
                    # Identificar campos XML conhecidos que podem conter tags aninhadas
                    xml_fields = {'value', 'table_script', 'rule_note', 'xml_message', 'validation_xml', 'alternate_xml', 'process_xml'}
                    
                    # Extrair campos XML primeiro e protegê-los
                    xml_field_contents = {}
                    for xml_field in xml_fields:
                        # Procurar por campo XML de primeiro nível
                        xml_field_pattern = rf'<{re.escape(xml_field)}>(.*?)</{re.escape(xml_field)}>'
                        xml_field_match = re.search(xml_field_pattern, result_content, re.DOTALL)
                        if xml_field_match:
                            # Extrair e proteger o conteúdo do campo XML
                            xml_field_contents[xml_field] = xml_field_match.group(0)  # Tag completa
                            # Substituir temporariamente por placeholder para evitar matching interno
                            placeholder = f'__XML_FIELD_{xml_field}_{id(xml_field_contents)}__'
                            result_content = result_content.replace(xml_field_contents[xml_field], placeholder)
                    
                    # Para cada coluna conhecida do schema, extrai o valor
                    # IMPORTANTE: Não procura por tags adicionais, pois campos XML podem conter
                    # tags internas que não são colunas (ex: <request>, <update_request> dentro de XML)
                    for col in columns:
                        # CRÍTICO: Procurar por CDATA primeiro (mais comum e preserva melhor)
                        # Padrão: <col><![CDATA[conteúdo]]></col>
                        # IMPORTANTE: NÃO usar \s* antes/depois do CDATA para preservar linhas vazias exatamente como estão
                        cdata_pattern = rf'<{re.escape(col)}><!\[CDATA\[(.*?)\]\]></{re.escape(col)}>'
                        cdata_match = re.search(cdata_pattern, result_content, re.DOTALL)
                        
                        if cdata_match:
                            # Conteúdo está em CDATA - extrair diretamente
                            extracted_value = cdata_match.group(1)
                        else:
                            # Não está em CDATA - usar padrão normal
                            # CRÍTICO: Procurar apenas tags de PRIMEIRO NÍVEL (não dentro de campos XML)
                            # Usar regex não-greedy para pegar apenas o primeiro nível
                            # e evitar pegar tags dentro de campos XML
                            col_pattern = rf'<{re.escape(col)}>(.*?)</{re.escape(col)}>'
                            match = re.search(col_pattern, result_content, re.DOTALL)
                            if match:
                                extracted_value = match.group(1)
                            else:
                                # Tag vazia ou não encontrada
                                extracted_value = None
                        
                        if extracted_value is not None:
                            is_placeholder = False
                            for xml_field in xml_fields:
                                placeholder = f'__XML_FIELD_{xml_field}_{id(xml_field_contents)}__'
                                if extracted_value == placeholder or placeholder in extracted_value:
                                    # Valor foi extraído de dentro de um campo XML - IGNORAR
                                    is_placeholder = True
                                    logger.warning(f"parse_xml_dataset (FALLBACK): Ignorando campo '{col}' com valor extraído de dentro do campo XML '{xml_field}' (não existe no banco)")
                                    break
                            
                            if is_placeholder:
                                # Não incluir este campo - foi extraído incorretamente de dentro de um campo XML
                                continue
                            
                            # Se o campo é um campo XML protegido, o valor já foi extraído corretamente acima
                            # (restauramos o campo XML antes de fazer matching)
                            # Não precisamos fazer nada adicional aqui
                            
                            # DEBUG: Log para table_script (fallback regex)
                            if col.lower() == 'table_script' and extracted_value:
                                logger.info(f"=== DEBUG: table_script EXTRAÍDO PELO REGEX (FALLBACK) ===")
                                logger.info(f"Length: {len(extracted_value)}")
                                logger.info(f"Has newlines: {chr(10) in extracted_value}")
                                logger.info(f"Has carriage return: {chr(13) in extracted_value}")
                                logger.info(f"Has tabs: {chr(9) in extracted_value}")
                                logger.info(f"Has 4-space indent: {'    ' in extracted_value}")
                                logger.info(f"First 300 chars: {repr(extracted_value[:300])}")
                                logger.info(f"=== FIM table_script EXTRAÍDO (FALLBACK) ===")
                            
                            # Extrair o conteúdo, mas não processar tags internas
                            row[col] = extracted_value
                        else:
                            # Verifica se é tag vazia ou auto-fechada
                            if f'<{col}/>' in result_content or f'<{col} />' in result_content:
                                row[col] = ""
                    
                    # Restaurar campos XML no result_content para próxima iteração (se necessário)
                    for xml_field, xml_content in xml_field_contents.items():
                        placeholder = f'__XML_FIELD_{xml_field}_{id(xml_field_contents)}__'
                        if placeholder in result_content:
                            result_content = result_content.replace(placeholder, xml_content)
                    
                    if row:
                        rows.append(row)
                
                # LOG: Resultado do fallback regex
                logger.info(f"[parse_xml_dataset] Após fallback regex - columns={len(columns)}, rows={len(rows)}")
                return columns, rows
        
        # Fallback: tenta parse direto (para outros formatos)
        logger.warning(f"[parse_xml_dataset] Usando fallback final: parse direto do root")
        for result in root.iter():
            if get_tag_name(result.tag) != "execute_sql_result":
                continue
            row = {}
            for child in result:
                tag = get_tag_name(child.tag)
                text = get_element_full_text(child)
                row[tag] = text
                if tag not in columns:
                    columns.append(tag)
            if row:
                rows.append(row)
        
        # LOG: Resultado final
        logger.info(f"[parse_xml_dataset] Final result - columns={len(columns)}, rows={len(rows)}")
        return columns, rows
        
    except ET.ParseError as e:
        logger.error(f"[parse_xml_dataset] Parse error: {e}")
        return columns, rows
    except Exception as e:
        logger.error(f"[parse_xml_dataset] Unexpected error: {e}")
        import traceback
        logger.error(f"[parse_xml_dataset] Traceback: {traceback.format_exc()}")
        return columns, rows


def parse_hierarchy_response(xml_response: str) -> tuple[List[str], List[Dict[str, Any]]]:
    """
    Parse XML response de hierarchy_select.
    
    CRÍTICO: Preserva formatação XML original (indentação, espaços entre tags, etc.)
    para campos XML aninhados como rule_note em process_rule_def.
    """
    columns = []
    rows = []
    
    try:
        xml_response = xml_response.strip()
        if not xml_response:
            return columns, rows
        
        # CRÍTICO: Para preservar formatação XML original, usar regex para extrair valores
        # ao invés de ET.fromstring + child.text que pode perder formatação
        # Extrair rows com regex (preserva formatação original)
        # Suporta namespaces: <row> ou <ns:row>
        row_pattern = r'<(?:\w+:)?row>(.*?)</(?:\w+:)?row>'
        row_contents = re.findall(row_pattern, xml_response, re.DOTALL)
        
        for row_content in row_contents:
            row = {}
            # Extrair cada coluna usando parser que conta tags aninhadas
            # CRÍTICO: Preserva formatação XML original (indentação, quebras de linha, etc.)
            
            def extract_tag_content(content: str, start_pos: int) -> tuple[str, int, str]:
                """
                Extrai conteúdo de uma tag XML, contando tags aninhadas.
                Retorna: (tag_name, end_position, tag_value)
                """
                # Procurar tag de abertura
                tag_start = content.find('<', start_pos)
                if tag_start == -1:
                    return None, len(content), None
                
                # Pular comentários XML
                if content[tag_start:tag_start+4] == '<!--':
                    comment_end = content.find('-->', tag_start + 4)
                    if comment_end == -1:
                        return None, len(content), None
                    return extract_tag_content(content, comment_end + 3)
                
                # Encontrar fim da tag de abertura
                tag_end = content.find('>', tag_start)
                if tag_end == -1:
                    return None, len(content), None
                
                tag_full = content[tag_start + 1:tag_end].strip()
                
                # Verificar se é tag auto-fechada
                if tag_full.endswith('/'):
                    tag_name = tag_full[:-1].split()[0]
                    if ':' in tag_name:
                        tag_name = tag_name.split(':', 1)[1]
                    return get_tag_name(tag_name), tag_end + 1, ""
                
                # Extrair nome da tag
                tag_parts = tag_full.split()
                tag_name_raw = tag_parts[0]
                if ':' in tag_name_raw:
                    tag_name_raw = tag_name_raw.split(':', 1)[1]
                tag_name = get_tag_name(tag_name_raw)
                
                # Buscar tag de fechamento, contando tags aninhadas
                content_start = tag_end + 1
                depth = 1
                pos = content_start
                
                while depth > 0 and pos < len(content):
                    next_tag = content.find('<', pos)
                    if next_tag == -1:
                        break
                    
                    # Pular comentários
                    if content[next_tag:next_tag+4] == '<!--':
                        comment_end = content.find('-->', next_tag + 4)
                        if comment_end == -1:
                            break
                        pos = comment_end + 3
                        continue
                    
                    tag_close = content.find('>', next_tag)
                    if tag_close == -1:
                        break
                    
                    tag_inner = content[next_tag + 1:tag_close].strip()
                    
                    # Pular tags auto-fechadas
                    if tag_inner.endswith('/'):
                        pos = tag_close + 1
                        continue
                    
                    # Verificar se é abertura ou fechamento da mesma tag
                    if tag_inner.startswith('/'):
                        closing_name = tag_inner[1:].split()[0]
                        if ':' in closing_name:
                            closing_name = closing_name.split(':', 1)[1]
                        if closing_name == tag_name:
                            depth -= 1
                            if depth == 0:
                                # Encontrou fechamento correto
                                value = content[content_start:next_tag]
                                return tag_name, tag_close + 1, value
                    else:
                        opening_name = tag_inner.split()[0]
                        if ':' in opening_name:
                            opening_name = opening_name.split(':', 1)[1]
                        if opening_name == tag_name:
                            depth += 1
                    
                    pos = tag_close + 1
                
                # Não encontrou fechamento
                return tag_name, tag_end + 1, None
            
            # Extrair todas as tags de primeiro nível
            pos = 0
            while pos < len(row_content):
                tag_name, next_pos, value = extract_tag_content(row_content, pos)
                if tag_name is None:
                    break
                if value is not None:
                    row[tag_name] = value
                    if tag_name not in columns:
                        columns.append(tag_name)
                    
                    # DEBUG: Log para campos XML comuns que precisam preservar formatação
                    xml_fields = ['rule_note', 'value', 'xml_message', 'validation_xml', 'alternate_xml', 'process_xml']
                    if tag_name.lower() in xml_fields and value and value.strip().startswith('<'):
                        logger.debug(f"=== DEBUG: Campo XML {tag_name} EXTRAÍDO PELO PARSER ===")
                        logger.debug(f"Length: {len(value)}")
                        logger.debug(f"Has newlines: {chr(10) in value}")
                        logger.debug(f"Has tabs: {chr(9) in value}")
                        logger.debug(f"Has 4-space indent: {'    ' in value}")
                        logger.debug(f"Has 2-space indent: {'  ' in value}")
                        logger.debug(f"First 300 chars: {repr(value[:300])}")
                        logger.debug(f"=== FIM Campo XML {tag_name} EXTRAÍDO ===")
                
                pos = next_pos
            
            if row:
                rows.append(row)
        
        # Se regex não encontrou resultados, usar fallback com ET.fromstring
        if not rows:
            root = ET.fromstring(xml_response)
            
            # Procura por row elements
            for row_elem in root.findall(".//row"):
                row = {}
                for child in row_elem:
                    tag = get_tag_name(child.tag)
                    # CRÍTICO: Para campos XML, tentar preservar formatação original
                    if len(child) == 0:
                        # Sem filhos - usar text diretamente (preserva formatação)
                        text = child.text or ""
                    else:
                        # Com filhos - usar get_element_full_text que tenta preservar formatação
                        text = get_element_full_text(child)
                    row[tag] = text
                    if tag not in columns:
                        columns.append(tag)
                if row:
                    rows.append(row)
        
        return columns, rows
        
    except ET.ParseError:
        return columns, rows

async def getUserSession(session):
    try:
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        
        # config = FSMConnectionConfig(
        #     host='https://MTLfsmTST.avcweb.com.br:443/FSMServerTST/',
        #     username='hasaus',
        #     password='mfrio123',
        #     target=FSMTarget.WCF
        # )
        client = FSMClient(config)

        safe_session = _validate_session(session)
        app_params_response = await client.execute_query(f"""
            SELECT session_id, user_id, token
              FROM dbo.c_user_sessions_maps
              WHERE session_id = '{safe_session}'
            """)
        await client.close()

        if app_params_response.success:
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
       
        return (result_rows)
        
    except Exception as e:
        logger.error(f"Erro na conexão: {e}")    

async def setUserSession(session,token):
    try:
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        
        # config = FSMConnectionConfig(
        #     host='https://MTLfsmTST.avcweb.com.br:443/FSMServerTST/',
        #     username='hasaus',
        #     password='mfrio123',
        #     target=FSMTarget.WCF
        # )
        client = FSMClient(config)

        safe_session = _validate_session(session)
        safe_token = _validate_session(token, label="token")
        app_params_response = await client.execute_edit(f"""
            UPDATE dbo.c_user_sessions_maps
               set token = '{safe_token}'
              WHERE session_id = '{safe_session}'
            """)
        await client.close()
        return
        
    except Exception as e:
        logger.error(f"Erro na conexão: {e}")    

async def dropUserSession(session):
    try:
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        
        # config = FSMConnectionConfig(
        #     host='https://MTLfsmTST.avcweb.com.br:443/FSMServerTST/',
        #     username='hasaus',
        #     password='mfrio123',
        #     target=FSMTarget.WCF
        # )
        client = FSMClient(config)

        safe_session = _validate_session(session)
        app_params_response = await client.execute_edit(f"""
            delete from dbo.c_user_sessions_maps
              WHERE session_id = '{safe_session}'
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[SQL Query Tool] Template exclusão executado com sucesso")
            
        return
        
    except Exception as e:
        logger.error(f"Erro na conexão: {e}")    

async def getJobsMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        
        client = FSMClient(config)

# Recuperando o valor

        app_params_response = await client.execute_query(f"""
            
                SELECT TOP 10
                    CONCAT(t.request_id,'|',t.task_id) AS client_job_id,
                    t.team_id client_team_id,
                    t.desc_team,
                    CONVERT(VARCHAR(19), CAST(t.team_modified_dttm AS DATETIME2), 120) team_modified_date,
                    t.person_id AS client_resource_id,
                    concat(p.first_name,COALESCE(p.middle_name,' '),p.last_name) AS resource_name,
                    CONVERT(VARCHAR(19), CAST(p.modified_dttm AS DATETIME2), 120) AS resource_modified_date,                                         
                    t.task_status AS client_status_id, 
                    case when mmd.message_text is null then t.desc_task_status else mmd.message_text end desc_status,
                    t.item_style_id client_style_id, 
                    CONVERT(VARCHAR(19), CAST(t.task_status_modified_dttm AS DATETIME2), 120) status_modified_date,
                    t.task_type AS client_type_id,
                    t.desc_task_type AS desc_type,
                    CONVERT(VARCHAR(19), CAST(t.task_type_modified_dttm AS DATETIME2), 120) type_modified_date,
                    t.place_id client_place_id,
                    t.trade_name,
                    t.cnpj,
                    CONVERT(VARCHAR(19), CAST(t.place_modified_dttm AS DATETIME2), 120) place_modified_date,
                    t.address_id client_address_id,
                    t.address,
                    t.city,
                    t.state_prov state, 
                    t.zippost zip_code,
                    t.geocode_lat, 
                    t.geocode_long,
                    CONVERT(VARCHAR(19), CAST(t.address_modified_dttm AS DATETIME2), 120) address_modified_date,
                    CONVERT(VARCHAR(19), CAST(t.plan_start_dttm AS DATETIME2), 120) plan_start_date,
                    CONVERT(VARCHAR(19), CAST(t.plan_end_dttm AS DATETIME2), 120) plan_end_date,
                    CONVERT(VARCHAR(19), CAST(t.actual_start_dttm AS DATETIME2), 120) actual_start_date,
                    CONVERT(VARCHAR(19), CAST(t.actual_end_dttm AS DATETIME2), 120) actual_end_date,
                    COALESCE(t.work_duration,0) * 60 AS real_time_service,
                    COALESCE(t.plan_task_dur_min,0) * 60 AS plan_time_service,
                    CONVERT(VARCHAR(19), CAST(t.created_dttm AS DATETIME2), 120) limit_start_date,
                    t.sla limit_end_date,
                    t.contr_type as priority,
                    CONVERT(VARCHAR(19), CAST(t.created_dttm AS DATETIME2), 120) created_date, 
                    CONVERT(VARCHAR(19), CAST(t.modified_dttm AS DATETIME2), 120) modified_date

                FROM dbo.c_task_routes_vw t WITH (NOEXPAND)
                    LEFT JOIN dbo.c_person_vw p WITH (NOEXPAND) ON t.person_id = p.person_id
                    LEFT JOIN metrix_message_def mmd ON t.desc_message_id = mmd.message_id AND locale_code = 'PT-BR' AND mmd.message_type = 'CODE'
			    WHERE t.modified_dttm >= CAST('{dateTime}' AS datetime)
                 ORDER BY t.modified_dttm ASC 
            
            """)
        # WHERE t.modified_dttm >= CAST('{dateTime}' AS datetime)
        await client.close()

        if app_params_response.success:
                logger.info(f"[getJobsMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        return (result_rows)
        
    except Exception as e:
        logger.error(f"[getJobsMatrix] Erro ao testar conexão: {e}")

async def getStyleMetrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        client = FSMClient(config)
        app_params_response = await client.execute_query(f"""
            select  item_style_id client_style_id, 
                    COALESCE(background, '#FFFFFF') AS background,
                    COALESCE(foreground, '#000000') AS foreground,
                    CONVERT(VARCHAR(19), CAST(modified_dttm AS DATETIME2), 120) AS modified_date
              from METRIX_ITEM_STYLE_VIEW
            where (modified_dttm >= CAST('{dateTime}' AS datetime))
            order by modified_dttm ASC
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[getStyleMetrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
        
    except Exception as e:
        logger.error(f"[getStyleMetrix] Erro ao testar conexão: {e}")

async def getResourcesMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)

            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        client = FSMClient(config)
        app_params_response = await client.execute_query(f"""
            select 
                p.person_id, 
                concat(p.first_name,COALESCE(p.middle_name,' '),p.last_name) AS name,
                p.geocode_lat, 
                p.geocode_long, 
                pg.geocode_lat_from,
                pg.geocode_long_from, 
                pg.geocode_lat_at, 
                pg.geocode_long_at,
                CASE WHEN p.work_status = 'OFF SHIFT' THEN 1 ELSE 0 END as work_status,
                p.modified_dttm, 
                max(p.modified_dttm)  OVER (PARTITION BY 1)  last_snap
              from C_PERSON_VW p WITH (NOEXPAND)
              LEFT JOIN c_person_geocode_vw pg ON p.person_id = pg.person_id
            where p.modified_dttm >= CAST('{dateTime}' AS datetime)
              and p.person_type = 'TECNICO'
            order by p.modified_dttm desc
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[SQL Query Tool] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
    except Exception as e:
        logger.error(f"Erro ao testar conexão: {e}")

async def getAdressMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        client = FSMClient(config)
        app_params_response = await client.execute_query(f"""
          SELECT a.*, 
                  MAX(modified_dttm) OVER (PARTITION BY 1) AS last_snap 
            FROM (                                             
                SELECT *
                from C_ADDRESS_VW a WITH (NOEXPAND)
                where a.modified_dttm >= CAST('{dateTime}' AS datetime)
            ) AS a
            order by a.modified_dttm desc                                                         
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[getAdressMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
    except Exception as e:
        logger.error(f"[getAdressMatrix] Erro ao testar conexão: {e}")

async def getPlaceMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        client = FSMClient(config)
        app_params_response = await client.execute_query(f"""
          SELECT a.*, 
                  MAX(modified_dttm) OVER (PARTITION BY 1) AS last_snap 
            FROM (                                             
                SELECT *
                from C_PLACE_VW a WITH (NOEXPAND)
                where a.modified_dttm >= CAST('{dateTime}' AS datetime)
            ) AS a
            order by a.modified_dttm desc                                                         
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[getPlaceMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
    except Exception as e:
        logger.error(f"[getPlaceMatrix] Erro ao testar conexão: {e}")

async def getLogInOutMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        client = FSMClient(config)
        app_params_response = await client.execute_query(f"""
            select a.*, max(modified_dttm)  OVER (PARTITION BY 1)  last_snap
              from (select person_id, logged_in, logged_out, modified_dttm,
                    ROW_NUMBER() OVER(
                        PARTITION BY person_id 
                        ORDER BY modified_dttm DESC
                    ) AS linha_num
                      from C_LOG_HISTORY_VW WITH (NOEXPAND)
                      where modified_dttm >= CAST('{dateTime}' AS datetime) ) as a
            WHERE linha_num = 1
              order by modified_dttm desc
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[getLogInOutMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)    
        
    except Exception as e:
        logger.error(f"[getLogInOutMatrix] Erro ao testar conexão: {e}")

async def getGeoPosMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        client = FSMClient(config)
        app_params_response = await client.execute_query(f"""
                                                         
            SELECT 
                modified_by, 
                geocode_lat, 
                geocode_long, 
                modified_dttm,
                max(modified_dttm)  OVER (PARTITION BY 1)  last_snap
            FROM (SELECT 
                    modified_by, 
                    geocode_lat, 
                    geocode_long, 
                    modified_dttm,
                    ROW_NUMBER() OVER(
                        PARTITION BY modified_by 
                        ORDER BY modified_dttm DESC
                    ) AS linha_num
                FROM geoposition
                WHERE modified_dttm >=  CAST('{dateTime}' AS datetime)
                ) as a
            WHERE linha_num = 1
              order by modified_dttm desc
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[getGeoPosMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
        
    except Exception as e:
        logger.error(f"[getGeoPosMatrix] Erro ao testar conexão: {e}")

async def getTeamMemberMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        
        client = FSMClient(config)

# Recuperando o valor

        app_params_response = await client.execute_query(f"""
            SELECT a.*, 
                  MAX(a.modified_dttm) OVER (PARTITION BY 1) AS last_snap 
             FROM dbo.c_team_member_vw a WITH (NOEXPAND)
            --WHERE a.modified_dttm >=  CAST('{dateTime}' AS datetime)
            ORDER BY a.modified_dttm DESC
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[getTeamMemberMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
        
    except Exception as e:
        logger.error(f"[getTeamMemberMatrix] Erro ao testar conexão: {e}")

async def getJobTypeMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        client = FSMClient(config)
        app_params_response = await client.execute_query(f"""
                                                        
            select code_value, description, modified_dttm, 
              MAX(modified_dttm) OVER (PARTITION BY 1) AS last_snap 
              from GLOBAL_CODE_TABLE
              where modified_dttm >= CAST('{dateTime}' AS datetime)
              and CODE_NAME = 'TASK_TYPE'
                AND ACTIVE = 'Y'
              order by modified_dttm desc
            """)

        await client.close()

        if app_params_response.success:
                logger.info(f"[getJobTypeMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
    except Exception as e:
        logger.error(f"[getJobTypeMatrix] Erro ao testar conexão: {e}")

async def getJobStatusMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        client = FSMClient(config)
        app_params_response = await client.execute_query(f"""
            SELECT 
              t.task_status, 
              CASE 
                  WHEN mmd.message_text IS NULL THEN t.description 
                  ELSE mmd.message_text 
              END AS description,
              t.item_style_id, 
              t.modified_dttm,
              MAX(t.modified_dttm) OVER (PARTITION BY 1) AS last_snap 
          FROM task_status t
          LEFT JOIN metrix_message_def mmd 
              ON t.desc_message_id = mmd.message_id 
              AND mmd.locale_code = 'PT-BR' 
              AND mmd.message_type = 'CODE'
          WHERE t.modified_dttm >= CAST('{dateTime}' AS datetime)
            AND t.active = 'Y'
          ORDER BY 
              t.modified_dttm DESC
            """)

        await client.close()

        if app_params_response.success:
                logger.info(f"[getJobStatusMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
    except Exception as e:
        logger.error(f"[getJobStatusMatrix] Erro ao testar conexão: {e}")

async def getTeamMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        
        client = FSMClient(config)

# Recuperando o valor

        app_params_response = await client.execute_query(f"""
            SELECT 
              t.team_id, 
              t.description, 
              a.geocode_lat,
              a.geocode_long,
              t.modified_dttm,
              MAX(t.modified_dttm) OVER (PARTITION BY 1) AS last_snap 
             FROM dbo.team t
              join dbo.place p on p.place_id = t.place_id
              left join dbo.place_address pa ON p.place_id = pa.place_id and pa.address_type = 'DEFAULT'
              LEFT join dbo.address a on pa.address_id  = a.address_id
             where t.modified_dttm >= CAST('{dateTime}' AS datetime)                                            
            ORDER BY t.modified_dttm DESC
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[getTeamMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
        
    except Exception as e:
        logger.error(f"[getTeamMatrix] Erro ao testar conexão: {e}")

async def getResourceWindowMatrix(dateTime: str = None, client_uid: str = None):
    try:
        dateTime = _validate_datetime(dateTime)
            # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        
        client = FSMClient(config)

# Recuperando o valor

        app_params_response = await client.execute_query(f"""
            SELECT a.*, 
                  MAX(a.modified_dttm) OVER (PARTITION BY 1) AS last_snap 
             FROM dbo.C_WORK_TIME_VW a WITH (NOEXPAND)
            WHERE a.modified_dttm >=  CAST('{dateTime}' AS datetime)
            ORDER BY a.modified_dttm DESC
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[getResourceWindowMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
        
    except Exception as e:
        logger.error(f"[getResourceWindowMatrix] Erro ao testar conexão: {e}")

async def getPriorityMatrix(client_uid: str = None):
    try:
        # Cria cliente FSM
        config = FSMConnectionConfig(
            host=settings.url_fsm,
            username=settings.usuario,
            password=settings.senha,
            target=FSMTarget.WCF
        )
        
        client = FSMClient(config)

        app_params_response = await client.execute_query(f"""
            SELECT 
              contr_type, 
              ranking 
             FROM dbo.C_PRIORITY_VW
            """)
        await client.close()

        if app_params_response.success:
                logger.info(f"[getPriorityMatrix] Template executado com sucesso")
                result_cols, result_rows = parse_xml_dataset(app_params_response.response_xml)
        
        return (result_rows)
        
    except Exception as e:
        logger.error(f"[getPriorityMatrix] Erro ao testar conexão: {e}")
