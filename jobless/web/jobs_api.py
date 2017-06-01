import json
from datetime import timedelta

from flask import (request,
                   jsonify,
                   Response,
                   Blueprint)

from jobless.brokers import load_jobs_repo
from jobless.models.job import Job, Status
from jobless.jobs_service.jobs_repos.exceptions import JobNotFoundException


jobs_blueprint = Blueprint('jobs', 'jobs')
jobs_repo = load_jobs_repo()


def body_to_job(job_json):
    job = Job(
        time_to_process=job_json['time_to_process'],
        command=job_json['command'],
        schedule=job_json.get('schedule', None),
        status=Status.READY.value,
        args=job_json.get('args', {}),
        on_success=job_json.get('on_success'),
        on_failure=job_json.get('on_failure')
    )
    return job


@jobs_blueprint.route('', methods=['POST'])
def create_job():
    try:
        job_json = request.get_json(silent=True)
        job = body_to_job(job_json)
        with jobs_repo.session_scope() as session:
            jobs_repo.insert(session, job)
        return jsonify(job.to_dict())
    except Exception as ex:
        print(ex)
        response = json.dumps({"error": str(ex)})
        status = 500
        headers = {"content-type": "application/json"}
        return response, status, headers


@jobs_blueprint.route('/<job_id>', methods=['GET'])
def retrieve_job(job_id):
    try:
        with jobs_repo.session_scope() as session:
            job = jobs_repo.get_job(session, job_id)
            if job:
                return jsonify(job.to_dict())
    except JobNotFoundException as ex:
        response = json.dumps({"error": str(ex)})
        status = 404
        headers = {"content-type": "application/json"}
        return response, status, headers


@jobs_blueprint.route('', methods=['GET'])
def retrieve_jobs():
    try:
        days = request.args.get('days', 0)
        hours = request.args.get('hours', 0)
        minutes = request.args.get('minutes', 0)
        seconds = request.args.get('seconds', 0)
        window = timedelta(days=int(days), hours=int(hours), minutes=int(minutes), seconds=int(seconds))

        with jobs_repo.session_scope() as session:
            jobs = jobs_repo.get_window(session, window)
            jobs = [job.to_dict() for job in jobs]
            return jsonify(jobs)
    except Exception as ex:
        response = json.dumps({"error": str(ex)})
        status = 400
        headers = {"content-type": "application/json"}
        return response, status, headers


@jobs_blueprint.route('/<job_id>', methods=['PUT'])
def update_job(job_id):
    try:
        job_json = request.get_json(silent=True)
        job = body_to_job(job_json)
        job.id = job_id
        with jobs_repo.session_scope() as session:
            jobs_repo.update(session, job)
        return Response(status=204)
    except JobNotFoundException:
        return Response(status=404)
    except Exception as ex:
        print(ex)
        return Response(status=500)


@jobs_blueprint.route('/<job_id>', methods=['DELETE'])
def delete_job(job_id):
    try:
        with jobs_repo.session_scope() as session:
            jobs_repo.delete(session, job_id)
        return Response(status=204)
    except JobNotFoundException:
        return Response(status=404)
    except Exception as ex:
        print(ex)
        return Response(status=500)
