"""
Script de limpeza de dados e cache
Remove arquivos temporários, logs antigos e cache.
"""

import os
import shutil
from pathlib import Path

def clean_project():
    """Limpa arquivos temporários do projeto."""
    print("=" * 70)
    print("🧹 LIMPEZA DO PROJETO")
    print("=" * 70)
    
    # Diretório base do projeto
    base_dir = Path(__file__).parent.parent
    
    # Padrões a remover
    patterns_to_remove = [
        "**/__pycache__",
        "**/*.pyc",
        "**/*.pyo",
        "**/*.pyd",
        "**/.pytest_cache",
        "**/.coverage",
        "**/htmlcov",
        "**/*.log",
    ]
    
    removed_count = 0
    
    for pattern in patterns_to_remove:
        for item in base_dir.glob(pattern):
            try:
                if item.is_file():
                    item.unlink()
                    print(f"  ✅ Removido: {item.relative_to(base_dir)}")
                    removed_count += 1
                elif item.is_dir():
                    shutil.rmtree(item)
                    print(f"  ✅ Removida pasta: {item.relative_to(base_dir)}")
                    removed_count += 1
            except Exception as e:
                print(f"  ⚠️ Não foi possível remover {item}: {e}")
    
    print("\n" + "=" * 70)
    print(f"✅ Limpeza concluída! {removed_count} items removidos.")
    print("=" * 70)

if __name__ == "__main__":
    clean_project()
