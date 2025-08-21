import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def get_rds_connection():
    """
    Gets AWS RDS connection using Airflow connections
    """
    logger.info("Getting Sqlalchemy engine")
    engine = PostgresHook(
        postgres_conn_id="RDS_POSTGRES_CONN"
    ).get_sqlalchemy_engine()
    logger.info("Sqlalchemy engine gotten")
    return engine


def create_table_on_rds():
    """
    Creates Table if does not exist
    """
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS public.trending_repos (
        repo_id BIGINT NOT NULL,
        repo_name TEXT NOT NULL,
        repo_full_name TEXT,
        description TEXT,
        primary_language TEXT,
        no_of_stars BIGINT,
        no_of_forks BIGINT,
        no_of_watchers BIGINT,
        no_of_open_issues BIGINT,
        repo_created_at TIMESTAMPTZ,
        repo_updated_at TIMESTAMPTZ,
        repo_pushed_at TIMESTAMPTZ,
        default_branch_name TEXT,
        ssh_url TEXT NOT NULL,
        clone_url TEXT NOT NULL,
        homepage TEXT,
        size_of_repo BIGINT NOT NULL,
        license TEXT,
        query_date DATE NOT NULL,
        PRIMARY KEY(repo_id, query_date)
        );
        """
    )

    engine = get_rds_connection()

    try:
        logger.info("creating table 'public.trending_repos'")
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(create_table_sql)
        logger.info("Table 'public.trending_repos' created Successfully")
    except Exception as e:
        logger.error(f"Table Creation failed: {e}")
        raise


def insert_into_rds(db_engine: Engine, data: list[dict]) -> None:
    """
    Inserts trending repositories data into RDS postgres database
    """
    total_records = len(data)
    logger.info(
        f"Inserting {total_records} record into 'public.trending_repos' on RDS"
    )

    insert_sql = text(
        """
        INSERT INTO public.trending_repos (
            repo_id, repo_name, repo_full_name,
            description, primary_language, no_of_stars,
            no_of_forks, no_of_watchers, no_of_open_issues,
            repo_created_at, repo_updated_at, repo_pushed_at,
            default_branch_name, ssh_url, clone_url,
            homepage, size_of_repo, license, query_date
        ) VALUES (
            :repo_id, :repo_name, :repo_full_name,
            :description, :primary_language, :no_of_stars,
            :no_of_forks, :no_of_watchers, :no_of_open_issues,
            :repo_created_at, :repo_updated_at, :repo_pushed_at,
            :default_branch_name, :ssh_url, :clone_url,
            :homepage, :size_of_repo, :license, :query_date
        )"""
    )

    try:
        with db_engine.connect() as conn:
            with conn.begin():
                conn.execute(insert_sql, data)
        logger.info("Insert Operation Completed Successfully")
    except Exception as e:
        logger.error(f"Insert operation failed: {e}")
        raise
