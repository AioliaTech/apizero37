"""
Parser para empreendimentos imobiliários
"""

from .base_parser import BaseParser
from typing import Dict, List, Any

class EmpreendimentosParser(BaseParser):
    """Parser para dados de empreendimentos imobiliários"""

    def can_parse(self, data: Any, url: str) -> bool:
        """Verifica se pode processar dados de empreendimentos"""
        return "empreendimentos" in url.lower()

    def parse(self, data: Any, url: str) -> List[Dict]:
        """Processa os dados de empreendimentos e retorna lista normalizada"""
        if not isinstance(data, dict) or "empreendimentos" not in data:
            return []

        empreendimentos = data["empreendimentos"]
        if not isinstance(empreendimentos, list):
            return []

        result = []
        for emp in empreendimentos:
            if not isinstance(emp, dict):
                continue

            normalized = {
                "id": emp.get("id"),
                "cliente_id": emp.get("cliente_id"),
                "id_cv": emp.get("id_cv"),
                "empreendimento": emp.get("empreendimento"),
                "endereco": emp.get("endereco"),
                "bairro": emp.get("bairro"),
                "cidade": emp.get("cidade"),
                "pontos_referencia": emp.get("pontos_referencia"),
                "tipo": emp.get("tipo"),
                "data_entrega": emp.get("data_entrega"),
                "segmento": emp.get("segmento"),
                "metragem": emp.get("metragem"),
                "andares": emp.get("andares"),
                "apartamentos_por_andar": emp.get("apartamentos_por_andar"),
                "quartos": emp.get("quartos"),
                "descricao": emp.get("descricao"),
                "valor": emp.get("valor"),
                "fotos": self.normalize_fotos(emp.get("fotos", [])),
                "ativo": emp.get("ativo"),
                "destaque": emp.get("destaque"),
                "created_at": emp.get("created_at"),
                "updated_at": emp.get("updated_at"),
                "book_url": emp.get("book_url"),
                "cliente": data.get("cliente")
            }
            result.append(normalized)

        return result