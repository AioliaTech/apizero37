"""
Parser específico para Zero37 - Peças e produtos de refrigeração
"""

from .base_parser import BaseParser
from typing import Dict, List, Any


class Zero37Parser(BaseParser):
    """Parser para dados da Zero37 (peças de refrigeração)"""
    
    def can_parse(self, data: Any, url: str) -> bool:
        """Verifica se pode processar dados da Zero37"""
        # Verifica pela URL
        if "zero37" in url.lower():
            return True
        
        # Verifica pela estrutura do JSON (array com campos específicos)
        if isinstance(data, list) and len(data) > 0:
            first_item = data[0] if data else {}
            if isinstance(first_item, dict):
                # Verifica se tem os campos característicos da Zero37
                has_zero37_fields = all(
                    key in first_item 
                    for key in ["id", "nome", "preco", "codigo_interno", "estoque", "foto"]
                )
                if has_zero37_fields:
                    return True
        
        return False
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        """Processa dados da Zero37"""
        if not isinstance(data, list):
            data = [data]
        
        parsed_items = []
        
        for item in data:
            # Pula itens sem nome (dados inválidos)
            nome = item.get("nome", "")
            if not nome or not nome.strip():
                continue
            
            # Pula itens com estoque negativo (não disponíveis)
            estoque = item.get("estoque", 0)
            if isinstance(estoque, (int, float)) and estoque < 0:
                continue
            
            # Processa a foto - adiciona .jpg se existir URL
            foto = item.get("foto")
            fotos = self._process_foto(foto)
            
            # Converte preço para float
            preco = self.converter_preco(item.get("preco", 0))
            
            # Pula itens com preço zero
            if preco <= 0:
                continue
            
            # Extrai código interno
            codigo_interno = item.get("codigo_interno", "")
            
            parsed = self.normalize_vehicle({
                "id": item.get("id"),
                "tipo": "peca_refrigeracao",
                "titulo": nome.strip(),
                "versao": None,
                "marca": None,
                "modelo": None,
                "observacao": None,
                "ano": None,
                "ano_fabricacao": None,
                "km": None,
                "cor": None,
                "combustivel": None,
                "cambio": None,
                "motor": None,
                "portas": None,
                "categoria": "Peça de Refrigeração",
                "cilindrada": None,
                "preco": preco,
                "opcionais": self._build_opcionais(item),
                "localizacao": None,
                "fotos": fotos,
                "codigo_interno": codigo_interno if codigo_interno and str(codigo_interno).strip() else None
            })
            parsed_items.append(parsed)
        
        return parsed_items
    
    def _process_foto(self, foto: Any) -> List[str]:
        """Processa a foto - adiciona extensão .jpg quando existir URL"""
        if not foto:
            return []
        
        if isinstance(foto, str):
            foto = foto.strip()
            if foto:
                # Adiciona .jpg no final da URL se não tiver extensão
                if not foto.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                    foto = foto + ".jpg"
                return [foto]
        
        return []
    
    def _build_opcionais(self, item: Dict) -> str:
        """Constrói o campo opcionais com informações adicionais"""
        parts = []
        
        # Código interno
        codigo_interno = item.get("codigo_interno", "")
        if codigo_interno and str(codigo_interno).strip():
            parts.append(f"Código: {codigo_interno}")
        
        # Estoque
        estoque = item.get("estoque", 0)
        if isinstance(estoque, (int, float)) and estoque >= 0:
            parts.append(f"Estoque: {int(estoque)}")
        
        return " | ".join(parts)
