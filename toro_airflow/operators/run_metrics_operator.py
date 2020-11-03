import logging

from airflow.hooks.http_hook import HttpHook
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults


class RunMetricsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 connection_id,
                 warehouse_id,
                 schema_name,
                 table_name,
                 metric_ids=None,
                 *args,
                 **kwargs):
        super(RunMetricsOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.warehouse_id = warehouse_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.metric_ids = metric_ids

    def execute(self, context):
        table = self._get_table_for_name(self.schema_name, self.table_name)
        if table is None or table.get("id") is None:
            raise Exception("Could not find table: ", self.schema_name, self.table_name)
        table_id = table.get("id")
        hook = self.get_hook('GET')
        result = hook.run("api/v1/metrics?warehouseIds={warehouse_id}&tableIds={table_id}"
                          .format(warehouse_id=self.warehouse_id,
                                  table_id=table_id),
                          headers={"Accept": "application/json"})
        metrics = result.json()
        num_failing_metrics = 0
        for m in metrics:
            logging.debug("Running metric: %s", m)
            metric_result = hook.run("statistics/runOne/{id}".format(id=m['id'])).json()
            for mr in metric_result:
                if not mr['statusOk']:
                    logging.error("Metric is not OK: %s", m)
                    logging.error("Metric result: %s", mr)
                    num_failing_metrics += 1
        if num_failing_metrics > 0:
            error_message = "There are {num_failing} failing metrics; see logs for more details"
            raise ValueError(error_message.format(num_failing=num_failing_metrics))

    def get_hook(self, method) -> HttpHook:
        return HttpHook(http_conn_id=self.connection_id, method=method)

    def _get_table_for_name(self, schema_name, table_name):
        hook = self.get_hook('GET')
        result = hook.run("dataset/tables/{warehouse_id}/{schema_name}"
                          .format(warehouse_id=self.warehouse_id,
                                  schema_name=schema_name),
                          headers={"Accept": "application/json"})
        tables = result.json()
        for t in tables:
            if t['datasetName'].lower() == table_name.lower():
                return t
        return None
