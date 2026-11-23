"""STEP 7c - Our first DAG: a "hello world" with 2 tasks.

    [say_hello] ──► [greet]

It touches no data. It only shows: how a DAG is written, how the order of the tasks
is declared, how a small value travels from one task to the next (XCom), and where to
read the logs.
"""

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="hello_lakehouse",  # the name you will see in the web UI
    schedule=None,  # None = it only runs when YOU press the button
    start_date=datetime(2026, 11, 1),  # from when this DAG "exists"
    catchup=False,  # don't run the past days automatically (explained in step 7f)
    tags=["learning"],  # a label to filter DAGs in the UI
)
def hello_lakehouse():
    @task
    def say_hello() -> str:
        print("Hola desde Airflow!")
        return "Júlia"  # the returned value is stored as an XCom

    @task
    def greet(name: str) -> None:
        print(f"Hola, {name}! Este nombre me ha llegado de la tarea anterior (XCom).")

    # Passing the result of say_hello to greet declares the ORDER: say_hello -> greet.
    greet(say_hello())


hello_lakehouse()  # creates the DAG so Airflow can find it
