# analytics-databricks-analyst-notebooks
A repository where the analysts on the analytics team can create and manage databricks notebooks

# Topaz

## Set up airflow
This section describes how we got the airflow instance running on EKS. First, ensure you have a working kubectl and helm setup on your computer and can reach the EKS cluster (left as an exercise to the reader).

Create a file `values-overrides.yaml` as follows:
```yaml
dags:
  persistence:
    enabled: false
  gitSync:
    enabled: true
    repo: git@github.com:budbee/analytics-databricks-analyst-notebooks.git
    branch: main
    subPath: ""
    wait: 60
    sshKeySecret: airflow-ssh-secret
extraSecrets:
  airflow-ssh-secret:
    data: |
      gitSshKey: ''
```

For the field `gitSshKey`, do the following;
```bash
ssh-keygen -t ed25519 -b 4096 -C "your.email@budbee.com"
base64 <your-private-key-file-generated-by-the-above-command> -w 0
```
**Use the ed25519 algorithm to generate this key**. Paste the output of the second command into the yaml file. Then put the public part of the key as a deploy key on the git repo.

After the SSH keys are in place, Run
```bash
helm upgrade --install topaz-airflow apache-airflow/airflow -f values-overrides.yaml
```

🎉

For further reading, consult
* Airflow Helm chart: https://airflow.apache.org/docs/helm-chart/stable/manage-dags-files.html
* Airflow override input: https://airflow.apache.org/docs/helm-chart/stable/parameters-ref.html

