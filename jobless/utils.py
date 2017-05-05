from sqlalchemy import create_engine


def create_tables(base, mysql_uri):
    engine = create_engine(mysql_uri, echo=True)
    base.metadata.create_all(engine)
