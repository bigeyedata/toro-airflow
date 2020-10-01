import json
from functools import reduce

from airflow.hooks.http_hook import HttpHook
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults


class UpsertFreshnessMetricOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 warehouse_id,
                 schema_name,
                 table_name,
                 column_name,
                 hours_between_update,
                 hours_delay_at_update,
                 notifications=[],
                 *args,
                 **kwargs):
        super(UpsertFreshnessMetricOperator, self).__init__(*args, **kwargs)
        self.warehouse_id = warehouse_id
        self.table_name = table_name
        self.column_name = column_name
        self.schema_name = schema_name
        self.hours_between_update = hours_between_update
        self.hours_delay_at_update = hours_delay_at_update
        self.notifications = notifications

    def execute(self, context):
        table = self._get_table_for_name()
        if table is None or table.get("id") is None:
            raise Exception("Could not find table: ", self.schema_name, self.table_name)
        existing_metric = self._get_existing_freshness_metric(table)
        metric = self._get_metric_object(existing_metric, table)
        print(metric)
        hook = HttpHook(http_conn_id='toro_connection', method='POST')
        result = hook.run("api/v1/metrics",
                          headers={"Content-Type": "application/json", "Accept": "application/json"},
                          data=json.dumps(metric))
        print("Url: ", result.url)
        print("Status code: ", result.status_code)
        print("Result: ", result.json())

    def _get_metric_object(self, existing_metric, table):
        metric = {
            "scheduleFrequency": {
                "intervalType": "HOURS_TIME_INTERVAL_TYPE",
                "intervalValue": 72
            },
            "thresholds": [
                {
                    "constantThreshold": {
                        "bound": {
                            "boundType": "UPPER_BOUND_SIMPLE_BOUND_TYPE",
                            "value": self.hours_between_update + self.hours_delay_at_update
                        }
                    }
                },
                {
                    "constantThreshold": {
                        "bound": {
                            "boundType": "LOWER_BOUND_SIMPLE_BOUND_TYPE",
                            "value": self.hours_delay_at_update
                        }
                    }
                }
            ],
            "warehouseId": self.warehouse_id,
            "datasetId": table.get("id"),
            "metricType": {
                "predefinedMetric": {
                    "metricName": self._get_metric_name_for_field()
                }
            },
            "parameters": [
                {
                    "key": "arg1",
                    "columnName": self.column_name
                }
            ],
            "lookback": {
                "intervalType": "DAYS_TIME_INTERVAL_TYPE",
                "intervalValue": 14
            },
            "notificationChannels": self._get_notification_channels()
        }
        if existing_metric is None:
            return metric
        else:
            existing_metric["thresholds"] = metric["thresholds"]
            existing_metric["notificationChannels"] = metric.get("notificationChannels", [])
            return existing_metric

    def _get_existing_freshness_metric(self, table):
        hook = HttpHook(http_conn_id='toro_connection', method='GET')
        result = hook.run("api/v1/metrics?warehouseIds={warehouse_id}&tableIds={table_id}"
                          .format(warehouse_id=self.warehouse_id,
                                  table_id=table.get("id")),
                          headers={"Accept": "application/json"})
        metrics = result.json()
        for m in metrics:
            if self._is_latency_metric(m):
                return m
        return None

    def _is_latency_metric(self, metric):
        keys = ["metricType", "predefinedMetric", "metricName"]
        result = reduce(lambda val, key: val.get(key) if val else None, keys, metric)
        return result is not None and result.startswith("HOURS_SINCE_MAX")

    def _get_table_for_name(self):
        hook = HttpHook(http_conn_id='toro_connection', method='GET')
        result = hook.run("dataset/tables/{warehouse_id}/{schema_name}"
                          .format(warehouse_id=self.warehouse_id,
                                  schema_name=self.schema_name),
                          headers={"Accept": "application/json"})
        tables = result.json()
        for t in tables:
            if t['datasetName'].lower() == self.table_name.lower():
                return t
        return None

    def _get_notification_channels(self):
        channels = []
        for n in self.notifications:
            if n.startswith('#') or n.startswith('@'):
                channels.append({"slackChannel": n})
            elif n.contains('@') and n.contains('.'):
                channels.append({"email": n})
        return channels

    def _get_metric_name_for_field(self, table):
        for f in table.get("fields"):
            if f.get("fieldName").lower() == self.column_name.lower():
                if f.get("type") == "TIMESTAMP_LIKE":
                    return "HOURS_SINCE_MAX_TIMESTAMP"
                elif f.get("type") == "DATE_LIKE":
                    return "HOURS_SINCE_MAX_DATE"