from flask import Flask, Blueprint, render_template, abort

from jobless.web.jobs_api import jobs_blueprint
from jobless.web.completed_jobs_api import completed_jobs_blueprint

app = Flask(__name__, template_folder='static')
app.register_blueprint(jobs_blueprint, url_prefix='/jobs')
app.register_blueprint(completed_jobs_blueprint, url_prefix='/completed_jobs')


@app.route('/')
def hello_world():
    return 'Hello, World!'
