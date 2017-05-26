from flask import Flask, Blueprint, render_template, abort
from jinja2 import TemplateNotFound

from jobless.web.jobs_api import jobs_blueprint


app = Flask(__name__, template_folder='static')
app.register_blueprint(jobs_blueprint, url_prefix='/jobs')


@app.route('/')
def hello_world():
    return 'Hello, World!'

