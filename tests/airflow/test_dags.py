from airflow.models import DagBag


def test_dags_load():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("bank_pipeline")

    assert dagbag.import_errors == {}, f"DAG import failures: {dagbag.import_errors}"
    assert dag is not None
    assert len(dag.tasks) > 0


def test_dag_tasks():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)

    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"


def test_dag_dependencies():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)

    for dag_id, dag in dag_bag.dags.items():
        for task in dag.tasks:
            assert task.task_id is not None
