
if __name__ == "__main__":
    from jobless.scheduler import Scheduler
    from jobless.brokers import load_jobs_service
    scheduler = Scheduler(load_jobs_service())
    scheduler.start()
