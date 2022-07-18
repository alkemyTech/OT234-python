from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import yaml, os

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('template_dags_UNLP_UAIn.jinja2')

for file_name in os.listdir(file_dir):
    if file_name.endswith('.yaml'):
        with open(f"{file_dir}/{file_name}", 'r') as config_file:
            config = yaml.safe_load(config_file)
            with open(f"{Path(__file__).parent.absolute().parent}/dag_{config['dag_id']}.py", 'w') as f:
                f.write(template.render(config))
