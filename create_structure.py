import os

# Define project structure
structure = {
    "my_data_pipeline": {
        "app": {
            "__init__.py": "",
            "config.py": "",
            "logger.py": "",
            "clients": {
                "__init__.py": "",
                "http_client.py": "",
                "secrets_manager.py": ""
            },
            "connectors": {
                "__init__.py": "",
                "snowflake_client.py": ""
            },
            "etl": {
                "__init__.py": "",
                "pipeline.py": ""
            },
            "lambda_handler.py": "",
            "utils.py": ""
        },
        "infra": {
            "step_function_definition.json": ""
        },
        "tests": {
            "test_pipeline.py": ""
        },
        ".github": {
            "workflows": {
                "ci-cd.yml": ""
            }
        },
        "requirements.txt": "",
        "README.md": ""
    }
}

def create_structure(base_path, structure_dict):
    for name, content in structure_dict.items():
        path = os.path.join(base_path, name)
        if isinstance(content, dict):  # It's a folder
            os.makedirs(path, exist_ok=True)
            create_structure(path, content)
        else:  # It's a file
            os.makedirs(base_path, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)

if __name__ == "__main__":
    create_structure(".", structure)
    print("✅ Project structure created successfully!")
