"""
Sistema unificado de processamento de XMLs com parsers modularizados
"""
import asyncio
import requests
import xmltodict
from typing import List, Dict
from config_loader import get_xml_configs, print_config_summary
from parsers.revendamais_parser import RevendamaisParser


async def processar_xml(config: Dict[str, str], parser: RevendamaisParser) -> List[Dict]:
    """
    Processa um único XML
    
    Args:
        config: Dict com 'url', 'localizacao', 'nome_env'
        parser: Instância do parser
    
    Returns:
        Lista de veículos parseados
    """
    try:
        localizacao_display = config['localizacao'] if config['localizacao'] else '(sem localização)'
        print(f"\n🔄 Processando: {config['nome_env']}")
        print(f"   Localização: {localizacao_display}")
        
        # Baixa o XML
        response = requests.get(config['url'], timeout=30)
        response.raise_for_status()
        
        # Parse XML
        data = xmltodict.parse(response.content)
        
        # Verifica se o parser pode processar
        if not parser.can_parse(data, config['url']):
            print(f"   ⚠️  Parser não suporta este formato")
            return []
        
        # Processa com localização
        vehicles = parser.parse(
            data=data,
            url=config['url'],
            localizacao=config['localizacao']
        )
        
        print(f"   ✅ {len(vehicles)} veículos encontrados")
        
        return vehicles
        
    except requests.exceptions.Timeout:
        print(f"   ❌ Timeout ao baixar XML")
        return []
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Erro ao baixar XML: {e}")
        return []
    except Exception as e:
        print(f"   ❌ Erro ao processar: {e}")
        import traceback
        traceback.print_exc()
        return []


async def processar_todos_xmls() -> List[Dict]:
    """
    Processa todos os XMLs configurados nas variáveis de ambiente
    
    Returns:
        Lista com todos os veículos de todos os XMLs
    """
    print("[INFO] Sistema unificado iniciado com parsers modularizados")
    
    # Carrega configurações
    configs = get_xml_configs()
    
    if not configs:
        print("[AVISO] Nenhuma variável de ambiente 'XML_URL' foi encontrada.")
        print("Atualização concluída: 0 veículos carregados")
        return []
    
    # Mostra resumo
    print_config_summary(configs)
    
    # Inicializa parser
    parser = RevendamaisParser()
    
    # Processa todos os XMLs
    print("🚀 Iniciando processamento dos XMLs...")
    all_vehicles = []
    
    for i, config in enumerate(configs, 1):
        print(f"\n[{i}/{len(configs)}] ", end="")
        vehicles = await processar_xml(config, parser)
        all_vehicles.extend(vehicles)
    
    # Resumo final
    print("\n" + "=" * 80)
    print("RESUMO FINAL")
    print("=" * 80)
    print(f"📊 Total de veículos carregados: {len(all_vehicles)}")
    
    # Agrupa por localização
    por_localizacao = {}
    for vehicle in all_vehicles:
        loc = vehicle.get('localizacao') or '(sem localização)'
        por_localizacao[loc] = por_localizacao.get(loc, 0) + 1
    
    if por_localizacao:
        print("\n📍 Veículos por localização:")
        for loc, count in sorted(por_localizacao.items()):
            print(f"   • {loc}: {count} veículos")
    
    print("=" * 80)
    print(f"✅ Atualização concluída: {len(all_vehicles)} veículos carregados\n")
    
    return all_vehicles


def main():
    """Ponto de entrada do sistema"""
    try:
        vehicles = asyncio.run(processar_todos_xmls())
        
        # Aqui você pode:
        # - Salvar no banco de dados
        # - Enviar para uma API
        # - Gerar um relatório
        # etc.
        
        return vehicles
    except KeyboardInterrupt:
        print("\n⚠️  Processamento interrompido pelo usuário")
        return []
    except Exception as e:
        print(f"\n❌ Erro fatal no sistema: {e}")
        import traceback
        traceback.print_exc()
        return []


if __name__ == "__main__":
    main()
