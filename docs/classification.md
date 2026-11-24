# Distributed Epsilon classification

`bin/submit classify` runs a binary classification comparison on the selected Epsilon training data using Spark MLlib. It uses all configured workers that pass the live preflight check; offline nodes are skipped.

## Run

```bash
bin/start-cluster
bin/stage epsilon
bin/submit classify 2>&1 | tee results/epsilon-classification.log
```

The selected input is read from `data/epsilon.format`. The classifier supports the repository's LIBSVM, TSV, and Parquet formats. Each node reads its own staged local file, so staging is required before training.

## Method

- Maps the binary labels `-1` and `1` to Spark's expected `0` and `1`.
- Uses a seeded 80/20 random split of the available 400,000-row training set (`CLASSIFY_SEED=42`, `TRAIN_FRACTION=0.8`).
- Trains Logistic Regression and Linear SVC as distributed Spark MLlib estimators. Both are linear classifiers suitable for a high-dimensional, binary task.
- Uses 10 optimization iterations by default. Set `CLASSIFY_MAX_ITER` to change this, and `CLASSIFY_EXECUTOR_MEMORY` if the workers have more or less available memory.
- Uses one disk-backed copy of parsed vectors, 32 MiB input partitions, and small cache batches to control heap usage on the workers.
- Reports per-model accuracy, precision, recall, F1, ROC AUC, test-row count, fit/evaluation time, and the true-negative/false-positive/false-negative/true-positive matrix.
- Saves coefficient, intercept, threshold, label mapping, and training settings as `results/models/epsilon/logistic-regression.json` and `results/models/epsilon/linear-svc.json`. These local outputs are ignored by Git.

Example with more optimization iterations and a timestamped log:

```bash
set -o pipefail
CLASSIFY_MAX_ITER=20 bin/submit classify 2>&1 | tee "results/epsilon-classification-$(date +%Y%m%d-%H%M%S).log"
```

The split is reproducible for the same input and partition layout. The metrics are an internal holdout estimate from the available training split; they are not a final benchmark against a separate official Epsilon test set. Compare both models on the same split, and use a separate test set for final model selection if one becomes available.

While training, view stages and executors at `http://<MASTER_LAN_IP>:4040`; the cluster dashboard is at `http://<MASTER_LAN_IP>:8090`.
