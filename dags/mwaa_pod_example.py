from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, NodegroupStates
from airflow.providers.amazon.aws.operators.eks import (
    EKSCreateClusterOperator,
    EKSCreateNodegroupOperator,
    EKSDeleteClusterOperator,
    EKSDeleteNodegroupOperator,
    EKSPodOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EKSClusterStateSensor, EKSNodegroupStateSensor
from airflow.utils.dates import days_ago

default_args = {
    "owner": "aws",
    "depends_on_past": False,
    "start_date": datetime(2019, 2, 20),
    "provide_context": True,
}

dag = DAG("kubernetes_pod_example", default_args=default_args, schedule_interval=None)

# use a kube_config stored in s3 dags folder for now
kube_config_path = "/opt/airflow/dags/repo/dags/kube_config.yaml"

# podRun = KubernetesPodOperator(
#     namespace="mwaa",
#     image="ubuntu:18.04",
#     cmds=["bash"],
#     arguments=["-c", "ls"],
#     labels={"foo": "bar"},
#     name="mwaa-pod-test",
#     task_id="pod-task",
#     get_logs=True,
#     dag=dag,
#     is_delete_operator_pod=False,
#     config_file=kube_config_path,
#     in_cluster=False,
#     cluster_context="mwaa", # Must match kubeconfig context
# )
with DAG(
    dag_id='example_eks_with_nodegroups_dag',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['example'],
) as dag:
    start_pod = EKSPodOperator(
        task_id="run_pod",
        cluster_name="managed-airflow-mwaa",
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "ls"],
        labels={"demo": "hello_world"},
        get_logs=True,
        # Delete the pod when it reaches its final state, or the execution is interrupted.
        #is_delete_operator_pod=True,
    )

this_will_skip = BashOperator(
    task_id='this_will_skip',
    bash_command='aws;aws --region=us-west-2 eks get-token --cluster-name=managed-airflow-mwaa;echo "hello world"; exit 99;',
    dag=dag,
)