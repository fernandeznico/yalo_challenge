"""
Get a different Chuck Norris Joke every day

# Data Base Configuration

    CREATE SCHEMA yalo;

    CREATE TABLE yalo.chuck_norris_jokes(
        id				VARCHAR(22)		NOT NULL								,
        value			TEXT			NOT NULL								,
        source			VARCHAR(20)		NOT NULL								,
        created_at		TIMESTAMP		NOT NULL	DEFAULT CURRENT_TIMESTAMP	,

        PRIMARY KEY (id)
    );

    SELECT * FROM yalo.chuck_norris_jokes WHERE value LIKE '%''%' LIMIT 1;

    SELECT EXTRACT(hour FROM created_at), source, min(created_at), max(created_at), count(*)
    FROM yalo.chuck_norris_jokes
    GROUP BY 1, 2
    ORDER BY 1, 2
    ;

"""
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from pendulum import today

from utils.logging import log


default_args = {
    "owner": "DE_TEAM",
    # "depends_on_past": True,
    # "email": "airflow@example.com",
    # "email_on_failure": True,
    # "email_on_retry": False,
    # "retries": 3,
    # "retry_delay": timedelta(minutes=5),
    # "retry_exponential_backoff": False,
    # "max_retry_delay": timedelta(hours=1),
    # "start_date": datetime(2024, 1, 1),
    # "end_date": datetime(2024, 12, 31),
    # "schedule_interval": "@daily",
    # "catchup": False,
    # "sla": timedelta(hours=2),
    # "execution_timeout": timedelta(minutes=30),
    # "queue": "default",
    # "priority_weight": 1,
    # "wait_for_downstream": True,
    # "trigger_rule": "all_success",
    # "pool": "default_pool"
}


with DAG(
    dag_id=Path(__file__).stem,
    default_args=default_args,
    start_date=today("America/Mexico_City").add(days=-1),
    schedule_interval=None,  # "0 10 * * *",
    tags=["API", "DAILY", "PYTHON"],
) as dag:
    @task(task_id="load_a_new_joke")
    def load_a_new_joke():
        import requests

        from utils.postgres import execute_query
        from utils.str import fmt_left

        url = "https://matchilling-chuck-norris-jokes-v1.p.rapidapi.com/jokes/random"

        headers = {
            "x-rapidapi-key": "77ad4cc72emshb09e37268a68141p106138jsn793b5e66df75",
            "x-rapidapi-host": "matchilling-chuck-norris-jokes-v1.p.rapidapi.com",
            "accept": "application/json"
        }

        def get_a_random_joke():
            response = requests.get(url, headers=headers)
            if not response.ok:
                log.error(response.json())
                #   429 # {'message': 'You have exceeded the rate limit per hour for your plan, BASIC, by the API provider'}
                #   ~927 requests per hour
                raise Exception("API failed")
            d = response.json()
            return d["id"], d["value"]

        def does_this_id_exists():
            query = f"SELECT 1 FROM yalo.chuck_norris_jokes WHERE id = '{quote_escaped_joke_id}' LIMIT 1"
            if execute_query(query=query):
                return True
            return False

        for _ in range(1):
            joke_id, joke_value = get_a_random_joke()
            quote_escaped_joke_id = joke_id.replace("'", "''")
            if not does_this_id_exists():
                log.info(f"Joke id `{quote_escaped_joke_id}` is new")
                quote_escaped_joke_value = joke_value.replace("'", "''")
                execute_query(
                    query=fmt_left(
                        f"""
                        INSERT INTO yalo.chuck_norris_jokes(id, value, source)
                        VALUES ('{quote_escaped_joke_id}', '{quote_escaped_joke_value}', 'AIRFLOW')
                        """
                    ),
                    fetch_one=False,
                    commit=True,
                )
                log.info(f"Joke value `{quote_escaped_joke_value}` added")
                return
            log.warning(f"Joke id `{quote_escaped_joke_id}` already exists")
        raise Exception("After tryings 1000 times, a new joke was not found")

    load_a_new_joke()
