# University Dag generator
# Creates each university dag from yaml config files

from jinja2 import Environment, FileSystemLoader
import yaml
import os


file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
dag_template = env.get_template('dag_template_OT234_UPal-UNJu.jinja2')

for file_name in os.listdir(file_dir):
    if file_name.endswith('.yaml'):
        with open(f"{file_dir}/{file_name}", 'r') as config_file:
            config = yaml.safe_load(config_file)
            with open(f"dags/{config['dag_id']}.py", 'w') as f:
                f.write(dag_template.render(config))