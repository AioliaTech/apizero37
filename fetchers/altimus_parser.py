"""
Parser específico para Altimus (altimus.com.br)
"""

from .base_parser import BaseParser
from typing import Dict, List, Any
import re
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger(__name__)

class AltimusParser(BaseParser):
    """Parser para dados do Altimus"""
    
    def can_parse(self, data: Any, url: str) -> bool:
        """Verifica se pode processar dados do Altimus"""
        return "altimus.com.br" in url.lower()
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        """Processa dados do Altimus (JSON ou XML)"""
        # Detecta se é XML ou JSON
        if isinstance(data, str):
            # Se for string, tenta fazer parse como XML
            try:
                logger.info("Tentando fazer parse do XML...")
                veiculos_data = self._parse_xml(data)
                logger.info(f"XML parseado com sucesso. {len(veiculos_data)} veículos encontrados.")
            except Exception as e:
                logger.error(f"Erro ao fazer parse do XML: {e}", exc_info=True)
                return []
        else:
            # Se for dict, processa como JSON
            veiculos_data = data.get("veiculos", [])
            if isinstance(veiculos_data, dict):
                veiculos_data = [veiculos_data]
            logger.info(f"JSON processado. {len(veiculos_data)} veículos encontrados.")
        
        return self._process_vehicles(veiculos_data)
    
    def _parse_xml(self, xml_string: str) -> List[Dict]:
        """Converte XML para estrutura dict compatível com o parse JSON"""
        # Remove possível BOM e espaços
        xml_string = xml_string.strip()
        if xml_string.startswith('\ufeff'):
            xml_string = xml_string[1:]
        
        logger.info(f"Primeiros 200 caracteres do XML: {xml_string[:200]}")
        
        root = ET.fromstring(xml_string)
        logger.info(f"Root tag: {root.tag}")
        
        veiculos = []
        
        for veiculo_element in root.findall('Veiculo'):
            veiculo = {}
            
            # Mapeamento dos campos XML para estrutura JSON
            veiculo['id'] = self._get_xml_text(veiculo_element, 'Codigo')
            veiculo['tipo'] = self._get_xml_text(veiculo_element, 'Tipo')
            veiculo['marca'] = self._get_xml_text(veiculo_element, 'Marca')
            veiculo['modelo'] = self._get_xml_text(veiculo_element, 'Modelo')
            veiculo['versao'] = self._get_xml_text(veiculo_element, 'ModeloVersao')
            veiculo['anoFabricacao'] = self._get_xml_text(veiculo_element, 'AnoFabr')
            veiculo['anoModelo'] = self._get_xml_text(veiculo_element, 'AnoModelo')
            veiculo['ano'] = self._get_xml_text(veiculo_element, 'AnoModelo')
            veiculo['combustivel'] = self._get_xml_text(veiculo_element, 'Combustivel')
            veiculo['cambio'] = self._get_xml_text(veiculo_element, 'Cambio')
            veiculo['portas'] = self._get_xml_text(veiculo_element, 'Portas')
            veiculo['cor'] = self._get_xml_text(veiculo_element, 'Cor')
            veiculo['km'] = self._get_xml_text(veiculo_element, 'Km')
            veiculo['preco'] = self._get_xml_text(veiculo_element, 'Preco')
            veiculo['valorVenda'] = self._get_xml_text(veiculo_element, 'Preco')
            
            # Opcionais - concatena campos relevantes
            opcionais_parts = []
            equipamentos = self._get_xml_text(veiculo_element, 'Equipamentos')
            if equipamentos:
                opcionais_parts.append(equipamentos)
            
            # Adiciona campos opcionais se existirem
            if self._get_xml_text(veiculo_element, 'Ar_condicionado') == 'sim':
                opcionais_parts.append('Ar condicionado')
            if self._get_xml_text(veiculo_element, 'Vidros_eletricos') == 'sim':
                opcionais_parts.append('Vidros elétricos')
            if self._get_xml_text(veiculo_element, 'Travas_eletricas') == 'sim':
                opcionais_parts.append('Travas elétricas')
            if self._get_xml_text(veiculo_element, 'Desembacador_traseiro') == 'sim':
                opcionais_parts.append('Desembaçador traseiro')
            if self._get_xml_text(veiculo_element, 'Direcao_hidraulica') == 'sim':
                opcionais_parts.append('Direção hidráulica')
            
            veiculo['opcionais'] = ', '.join(opcionais_parts) if opcionais_parts else ''
            
            # Fotos - split por ponto e vírgula
            fotos_text = self._get_xml_text(veiculo_element, 'Fotos')
            if fotos_text:
                veiculo['fotos'] = [f.strip() for f in fotos_text.split(';') if f.strip()]
            else:
                veiculo['fotos'] = []
            
            logger.info(f"Veículo parseado: {veiculo.get('marca')} {veiculo.get('modelo')}")
            veiculos.append(veiculo)
        
        logger.info(f"Total de veículos parseados do XML: {len(veiculos)}")
        return veiculos
    
    def _get_xml_text(self, element: ET.Element, tag: str) -> str:
        """Extrai texto de um elemento XML de forma segura"""
        child = element.find(tag)
        return child.text.strip() if child is not None and child.text else None
    
    def _process_vehicles(self, veiculos: List[Dict]) -> List[Dict]:
        """Processa lista de veículos (comum para JSON e XML)"""
        parsed_vehicles = []
        
        for v in veiculos:
            modelo_veiculo = v.get("modelo")
            versao_veiculo = v.get("versao")
            opcionais_veiculo = self._parse_opcionais(v.get("opcionais"))
            combustivel_veiculo = v.get("combustivel")
            
            # Determina se é moto ou carro - CORREÇÃO PARA EVITAR ERRO DE None
            tipo_veiculo = v.get("tipo", "")
            tipo_veiculo_lower = tipo_veiculo.lower() if tipo_veiculo else ""
            is_moto = "moto" in tipo_veiculo_lower or "motocicleta" in tipo_veiculo_lower
            
            if is_moto:
                cilindrada_final, categoria_final = self.inferir_cilindrada_e_categoria_moto(
                    modelo_veiculo, versao_veiculo
                )
            else:
                categoria_final = self.definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)
                cilindrada_final = None
            
            # Determina o tipo final do veículo
            tipo_final = self._determine_tipo(tipo_veiculo, is_moto)
            
            # NOVA REGRA: Se tipo for 'moto' ou 'eletrico' e combustível for 'Elétrico', categoria = "Scooter Eletrica"
            if (tipo_final in ['moto', 'eletrico'] and 
                combustivel_veiculo and 
                str(combustivel_veiculo).lower() == 'elétrico'):
                categoria_final = "Scooter Eletrica"
            
            parsed = self.normalize_vehicle({
                "id": v.get("id"),
                "tipo": tipo_final,
                "titulo": None,
                "versao": versao_veiculo,
                "marca": v.get("marca"),
                "modelo": modelo_veiculo,
                "ano": v.get("anoModelo") or v.get("ano"),
                "ano_fabricacao": v.get("anoFabricacao") or v.get("ano_fabricacao"),
                "km": v.get("km"),
                "cor": v.get("cor"),
                "combustivel": combustivel_veiculo,
                "cambio": self._normalize_cambio(v.get("cambio")),
                "motor": self._extract_motor_from_version(versao_veiculo),
                "portas": v.get("portas"),
                "categoria": categoria_final,
                "cilindrada": cilindrada_final,
                "preco": self.converter_preco(v.get("valorVenda") or v.get("preco")),
                "opcionais": opcionais_veiculo,
                "fotos": v.get("fotos", [])
            })
            parsed_vehicles.append(parsed)
        
        logger.info(f"Total de veículos processados: {len(parsed_vehicles)}")
        return parsed_vehicles
    
    def _parse_opcionais(self, opcionais: Any) -> str:
        """Processa os opcionais do Altimus"""
        if isinstance(opcionais, list):
            return ", ".join(str(item) for item in opcionais if item)
        return str(opcionais) if opcionais else ""
    
    def _determine_tipo(self, tipo_original: str, is_moto: bool) -> str:
        """Determina o tipo final do veículo"""
        if not tipo_original:
            return "carro" if not is_moto else "moto"
        
        # Normaliza tipos do XML
        tipo_lower = tipo_original.lower()
        if "motos" in tipo_lower or "moto" in tipo_lower:
            return "moto"
        elif "carros" in tipo_lower or "carro" in tipo_lower:
            return "carro"
        elif tipo_original in ["Bicicleta", "Patinete Elétrico"]:
            return "eletrico"
        elif is_moto:
            return "moto"
        elif tipo_original == "Carro/Camioneta":
            return "carro"
        else:
            return tipo_lower
    
    def _normalize_cambio(self, cambio: str) -> str:
        """Normaliza informações de câmbio"""
        if not cambio:
            return cambio
        
        cambio_str = str(cambio).lower()
        if "manual" in cambio_str:
            return "manual"
        elif "automático" in cambio_str or "automatico" in cambio_str:
            return "automatico"
        else:
            return cambio
    
    def _extract_motor_from_version(self, versao: str) -> str:
        """Extrai informações do motor da versão"""
        if not versao:
            return None
        
        # Busca padrão de cilindrada (ex: 1.4, 2.0, 1.6)
        motor_match = re.search(r'\b(\d+\.\d+)\b', str(versao))
        return motor_match.group(1) if motor_match else None
