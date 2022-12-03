# mlops_integrating_mlflow_with_orchestration



The first folder mlflow contains how to make use of mlflow:
   * To track the multiple runs of experiments
   * log the metrics , params
   * Log the models and artifacts
   * Automatically choose the best model and push that to model registry and eventually to staging , production etc.
   * [Refer this blog for concrete info](https://medium.com/@kaanboke/step-by-step-mlflow-implementations-a9872dd32d9b) and [mlflow](https://www.mlflow.org/docs/latest/index.html)

The second folder deals with orchestration:
 * retries, distributed execution, scheduling, caching made easy with prefect
 * To monitor and also schedule MLOps pipelines.
 * Automatic scheduling of training the model with different data points
 * Deployment

Check the below model for visual understanding how it can be used in mlops

![alt text](orchestration_with_prefect2.0b5/Flow_Diagram_AM_W3.png)

[Good starting point to know about prefect](https://medium.com/@kaanboke/step-by-step-prefect-implementations-lets-orchestrate-the-workflows-9b3d09053c19)

More info about converting python scipts to prefect flows and tasks, creating storage ( local , cloud etc), creating deployments and atlast invoking agents to execute the scheduled flow at [github page](https://gist.github.com/gowtham07/d6fb11f6308e59248eec3a83f19faca4)
