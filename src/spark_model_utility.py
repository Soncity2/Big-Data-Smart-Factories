from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import year, month, dayofmonth, hour, unix_timestamp, lag, expr, mean, log1p, col
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler, PCA
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator, ClusteringEvaluator

from xgboost.spark import SparkXGBRegressor

import matplotlib.pyplot as plt
import seaborn as sns

import logging

logger = logging.getLogger("DataProcessor")

class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_data(self, file_path: str) -> DataFrame:
        """
        Load CSV data with header and inferred schema.
        """
        return self.spark.read.csv(file_path, header=True, inferSchema=True)

    def extract_time_features(self, df: DataFrame, timestamp_col: str = "ts") -> DataFrame:
        """
        Extracts year, month, day, hour, and UNIX timestamp from a timestamp column.
        """
        return df.withColumn("year", year(col(timestamp_col)))\
                 .withColumn("month", month(col(timestamp_col)))\
                 .withColumn("day", dayofmonth(col(timestamp_col)))\
                 .withColumn("hour", hour(col(timestamp_col)))\
                 .withColumn("ts_unix", unix_timestamp(col(timestamp_col)))

    def add_lag_features(
        self,
        df: DataFrame,
        asset_col: str,
        time_col: str,
        target_cols: list,
        lag_values: list = [1, 2, 3]
    ) -> DataFrame:
        """
        Adds lagged features for each target column, using a window partitioned by asset and ordered by time.
        Fills missing values with the column's mean.
        """
        window_spec = Window.partitionBy(asset_col).orderBy(time_col)

        for target in target_cols:
            for lag_num in lag_values:
                col_name = f"{target}_lag{lag_num}"
                df = df.withColumn(col_name, lag(col(target), lag_num).over(window_spec))
                mean_value = df.select(mean(col_name)).collect()[0][0]
                df = df.fillna({col_name: mean_value if mean_value is not None else 0.0})

        return df

    def log_transform_column(self, df: DataFrame, input_col: str, output_col: str = None) -> DataFrame:
        """
        Applies log1p (log(1 + x)) transformation to a column.
        """
        if output_col is None:
            output_col = f"{input_col}_log"
        return df.withColumn(output_col, log1p(col(input_col)))



def assemblerAndPipeline(df, config):
    # Ensure all features exist in the dataset
    existing_features = [col for col in config["features"] if col in df.columns]

    if not existing_features:
        raise ValueError(f"No valid features found in dataset. Available columns: {df.columns}")

    assembler = VectorAssembler(inputCols=existing_features, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)

    stages = [assembler, scaler]

    # Optional PCA
    if config.get("use_pca", False):
        pca_components = config.get("pca_components", 5)
        pca = PCA(k=pca_components, inputCol="scaled_features", outputCol="pca_features")
        stages.append(pca)
        feature_input = "pca_features"
    else:
        feature_input = "scaled_features"

    # Clustering only
    if config["model"] == KMeans:
        model_instance = config["model"](**config["params"], featuresCol=feature_input)
        stages.append(model_instance)
        pipeline = Pipeline(stages=stages)

    # Pseudo-labeling workflow
    elif config.get("pseudo_labeling", False):
        # Step 1: Cluster
        pseudo_cluster = config["pseudo_clustering"](**config["params"],
                                                             featuresCol=feature_input, predictionCol="pseudo_labels")
        stages.append(pseudo_cluster)
        clustering_pipeline = Pipeline(stages=stages)
        df = clustering_pipeline.fit(df).transform(df)

        if "pseudo_labels" not in df.columns:
            raise ValueError("Pseudo labels were not generated. Check KMeans step and input features.")

        # Step 2: Regressor on pseudo labels (assumes SparkXGBRegressor)
        model_instance = SparkXGBRegressor(features_col=feature_input, label_col="pseudo_labels")
        pipeline = Pipeline(stages=[model_instance])  # This only applies the model at the end

    # Standard supervised model
    else:
        model_instance = config["model"](featuresCol=feature_input, labelCol=config["target"])
        stages.append(model_instance)
        pipeline = Pipeline(stages=stages)

    return pipeline, df

# Function to train and save a model
def train_and_save_model(df, model_name, config):
    pipeline, df_new = assemblerAndPipeline(df, config)

    # Sort data by timestamp
    df_new = df_new.orderBy("ts_unix")

     # Debug: Print dataset before splitting
    print(f"✅ Columns before train-test split for {model_name}: {df_new.columns}")

    # Ensure `pseudo_labels` exists before splitting
    if config.get("pseudo_labeling", False) and "pseudo_labels" not in df_new.columns:
        raise ValueError(f"❌ Missing 'pseudo_labels' before splitting for {model_name}")

    train_size = int(df.count() * 0.85)
    train = df_new.limit(train_size)

    # Debug: Print columns in the training dataset
    print(f"✅ Columns in training set for {model_name}: {train.columns}")

    # Ensure `pseudo_labels` is still present before fitting
    if config.get("pseudo_labeling", False) and "pseudo_labels" not in train.columns:
        raise ValueError(f"❌ 'pseudo_labels' disappeared in training set for {model_name}")

    model = pipeline.fit(train)
    model.write().overwrite().save(f"models/{model_name}")
    print(f"Model '{model_name}' trained and saved successfully!")
    

class ModelEvaluator:
    def __init__(self, spark):
        self.spark = spark

    def load_model(self, model_name):
        model_path = f"models/{model_name}"
        return PipelineModel.load(model_path)

    def get_test_data(self, df, time_col="ts_unix", test_fraction=0.15):
        test_size = int(df.count() * test_fraction)
        sorted_df = df.orderBy(time_col)
        train_size = df.count() - test_size
        return sorted_df.subtract(sorted_df.limit(train_size))

    def evaluate_regression(self, predictions, target_col="power_avg"):
        evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)

        evaluator.setMetricName("mae")
        mae = evaluator.evaluate(predictions)

        evaluator.setMetricName("r2")
        r2 = evaluator.evaluate(predictions)

        print(f"📊 RMSE: {rmse:.4f}, MAE: {mae:.4f}, R²: {r2:.4f}")

        pdf = predictions.select("prediction", target_col).toPandas()
        plt.figure(figsize=(8, 6))
        sns.scatterplot(x=pdf[target_col], y=pdf["prediction"], alpha=0.5)
        plt.plot([pdf[target_col].min(), pdf[target_col].max()],
                 [pdf[target_col].min(), pdf[target_col].max()], 'r', linestyle="--")
        plt.xlabel("Actual Values")
        plt.ylabel("Predicted Values")
        plt.title(f"Regression Model Evaluation: {target_col}")
        plt.show()

    def evaluate_classification(self, predictions, target_col="status_index"):
        evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)

        evaluator.setMetricName("f1")
        f1_score = evaluator.evaluate(predictions)

        print(f"📊 Accuracy: {accuracy:.4f}, F1-score: {f1_score:.4f}")

        pdf = predictions.select("prediction", target_col).toPandas()
        plt.figure(figsize=(8, 6))
        sns.histplot(pdf["prediction"], color='blue', label='Predictions', kde=True, bins=30)
        sns.histplot(pdf[target_col], color='red', label='Actual', kde=True, bins=30)
        plt.xlabel("Class")
        plt.ylabel("Count")
        plt.title(f"Classification Model Evaluation: {target_col}")
        plt.legend()
        plt.show()

    def evaluate_kmeans(self, predictions, feature_col="features", cluster_col="prediction"):
        evaluator = ClusteringEvaluator(featuresCol=feature_col, predictionCol=cluster_col, metricName="silhouette")
        silhouette_score = evaluator.evaluate(predictions)

        print(f"📊 Silhouette Score: {silhouette_score:.4f}")

        pdf = predictions.select(cluster_col).toPandas()
        plt.figure(figsize=(8, 6))
        sns.countplot(x=pdf[cluster_col], palette="viridis")
        plt.xlabel("Cluster")
        plt.ylabel("Count")
        plt.title("Cluster Distribution")
        plt.show()

    def evaluate_xgboost_unsupervised(self, predictions, target_col="pseudo_labels"):
        evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)

        evaluator.setMetricName("mae")
        mae = evaluator.evaluate(predictions)

        evaluator.setMetricName("r2")
        r2 = evaluator.evaluate(predictions)

        print(f"📊 XGBoost Unsupervised Evaluation:\nRMSE: {rmse:.4f}, MAE: {mae:.4f}, R²: {r2:.4f}")

        pdf = predictions.select("prediction", target_col).toPandas()
        plt.figure(figsize=(8, 6))
        sns.scatterplot(x=pdf[target_col], y=pdf["prediction"], alpha=0.5)
        plt.plot([pdf[target_col].min(), pdf[target_col].max()],
                 [pdf[target_col].min(), pdf[target_col].max()], 'r', linestyle="--")
        plt.xlabel("Actual Pseudo Labels")
        plt.ylabel("Predicted Pseudo Labels")
        plt.title("XGBoost Pseudo-Label Regression Performance")
        plt.show()

    def assemble4KMeans(self, df, config):
        # Validate input features
        existing_features = [col for col in config["features"] if col in df.columns]
        if not existing_features:
            raise ValueError(f"No valid features found in dataset. Available columns: {df.columns}")

        # Assemble features
        assembler = VectorAssembler(inputCols=existing_features, outputCol="features")
        df_assembled = assembler.transform(df)

        # Scale
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
        df_scaled = scaler.fit(df_assembled).transform(df_assembled)

        # Optional PCA
        if config.get("use_pca", False):
            k = config.get("pca_components", 5)
            pca = PCA(k=k, inputCol="scaled_features", outputCol="pca_features")
            df_scaled = pca.fit(df_scaled).transform(df_scaled)
            print("PCA applied — resulting columns:", df_scaled.columns)

        return df_scaled

    def pipeline2KMeans(self, pipeline_model):
        kmeans_model = None
        for stage in pipeline_model.stages:
            if isinstance(stage, KMeansModel):
                kmeans_model = stage
                break

        if kmeans_model:
            print("✅ Number of clusters:", kmeans_model.getK())
            return kmeans_model
        else:
            print("⚠️ KMeans model not found in the pipeline!")
            
    def convert_PCA_Columns(df, name):
        # Convert Spark DataFrame to Pandas
        pdf = df.select("pca_features", "prediction").toPandas()

        # Split vector into individual dimensions
        pca_array = pdf["pca_features"].apply(lambda x: x.toArray() if hasattr(x, "toArray") else x)
        pdf[["pca_x", "pca_y", "pca_z"]] = pd.DataFrame(pca_array.tolist(), index=pdf.index)

        fig = plt.figure(figsize=(8, 6))
        ax = fig.add_subplot(111, projection='3d')
        # Get unique clusters
        clusters = sorted(pdf["prediction"].unique())

        # Plot each cluster separately to label them
        for cluster_id in clusters:
            subset = pdf[pdf["prediction"] == cluster_id]
            ax.scatter(subset["pca_x"], subset["pca_y"], subset["pca_z"],
                       label=f"Cluster {cluster_id}", alpha=0.6)

        ax.set_xlabel("PCA 1")
        ax.set_ylabel("PCA 2")
        ax.set_zlabel("PCA 3")
        ax.set_title(f"3D PCA Cluster Plot - {name}")
        ax.legend()
        plt.show()
