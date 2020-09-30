from airflow.hooks.http_hook import HttpHook
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults


class RunMetricsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 warehouse_id,
                 table_name,
                 metric_ids=None,
                 *args,
                 **kwargs):
        super(RunMetricsOperator, self).__init__(*args, **kwargs)
        self.warehouse_id = warehouse_id
        self.table_name = table_name
        self.metric_ids = metric_ids

    def execute(self, context):
        hook = HttpHook(http_conn_id='toro_connection', method='GET')
        result = hook.run("api/v1/metrics?warehouseIds={warehouse_id}&tableName={table_name}"
                          .format(warehouse_id=self.warehouse_id,
                                  table_name=self.table_name),
                          headers={"Accept": "application/json"})
        metrics = result.json()
        for m in metrics:
            print("Running metric: ", m)
            hook.run("statistics/runOne/{id}".format(id=m['id']))
