from flask import Flask

from jobless.web.jobs_api import jobs_blueprint


app = Flask(__name__)
app.register_blueprint(jobs_blueprint, url_prefix='/jobs')


@app.route('/')
def hello_world():
    return 'Hello, World!'

