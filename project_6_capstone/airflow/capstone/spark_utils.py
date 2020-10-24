from capstone.constants import DAGVariables
from typing import List
from airflow.models import Variable


def get_emr_add_steps_spec(
        step_name: str,
        spark_job_file: str, script_arguments: List[str] = [],
        spark_packages: str = None):

    spark_packages_param = []
    if spark_packages:
        # for some reason, setting up the SparkSession with the spark.jars.packages doesn't actually load that package.
        # The Spark session may have already been created maybe.
        spark_packages_param = ['--packages', spark_packages]

    # Request schema https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.add_job_flow_steps
    # https://stackoverflow.com/questions/36706512/how-do-you-automate-pyspark-jobs-on-emr-using-boto3-or-otherwise
    spec = [
        {
            'Name': step_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                ] + spark_packages_param + [
                    '--py-files', 's3://{}/spark/lib.zip'.format(Variable.get(DAGVariables.SCRIPT_BUCKET_NAME_VARIABLE)),
                    's3://{}/spark/{}'.format(Variable.get(DAGVariables.SCRIPT_BUCKET_NAME_VARIABLE), spark_job_file),
                ] + script_arguments,
            },
        }
    ]
    return spec
