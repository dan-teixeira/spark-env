from pathlib import Path

def get_project_base_path():
    return Path(__file__).parents[1].resolve()