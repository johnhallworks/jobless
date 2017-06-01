from flask import (Blueprint,
                   jsonify)

from jobless.brokers import load_completed_jobs_log


completed_jobs_blueprint = Blueprint('completed_jobs', 'completed_jobs')
completed_jobs_log = load_completed_jobs_log()


@completed_jobs_blueprint.route('/<job_id>', methods=['GET'])
def get_completed_jobs(job_id):
    with completed_jobs_log.session_scope() as session:
        completed_jobs = completed_jobs_log.get_completed_jobs(session, job_id)
        completed_jobs = [completed_job.to_dict() for completed_job in completed_jobs]
        return jsonify(completed_jobs)
