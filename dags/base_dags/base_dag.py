from typing import Dict, Optional, Union, Any

from airflow import DAG


class BASE_DAG:
    """
    A Base Dag is A wrapper of airflow.DAG which can dynamically
    creates multiple dags for you.
    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.

    :param dag_id: The id of the DAG; must consist exclusively of alphanumeric
        characters, dashes, dots and underscores (all ASCII).
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators.
    :param schedule_interval: Defines how often that DAG runs, this
        timedelta object gets added to your latest task instance's
        execution_date to figure out the next schedule
    :param catchup: Perform scheduler catchup

    **Look up for more parameters**: `Airflow DAG documentation
        <https://airflow.apache.org/docs/apache-airflow/1.10.12/concepts.html>`_
    """

    def __init__(
            self,
            dag_id: str,
            default_args: Optional[Dict],
            schedule_interval: Union[Any],
            catchup: bool,
    ) -> None:
        self.dag_id = dag_id
        self.default_args = default_args
        self.schedule_interval = schedule_interval
        self.catchup = catchup

    def Create_Dag(self, **kwargs) -> DAG:
        """
        A Create Dag is a method which provides basic dag creation

        :param **kwargs <dict>: Inherit all the dag parameters
        """
        ## DAG INIT
        dag = DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
            catchup=self.catchup,
            **kwargs,
        )
        return dag
