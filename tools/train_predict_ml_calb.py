import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, median_absolute_error, r2_score
from sklearn.model_selection import GridSearchCV, train_test_split
from xgboost import XGBRegressor


"""
Train and Predict Machine Learning Manning's Calibration Coefficients.

This script:
- Loads ML training data (prepared by aggregate_ml_calb_data.py).
- Computes log10-transformed calibration target (log_calb) if needed.
- Fits an XGBoost Regressor with optimal hyperparameters or GridSearchCV.
- Evaluates regression performance (R2, RMSE, MAE, MedAE, CV scores).
- Generates predictions on reaches across CONUS, inverse-transforms
   predictions back to linear scale (10 ** pred), and exports results to Parquet.
- Saves the trained model and feature importances.
"""

logger = logging.getLogger("ML_calb")

# environmental, catchment, and rating curve features
DEFAULT_FEATURES: List[str] = [
    'SLOPE',
    'areasqkm',
    'LengthKm',
    'TotDASqKM',
    'StreamOrde',
    'slope',
    'ArbolateSu',
    'Sinuosity',
    'silt_mean_0_5_r250',
    'WtDepWs',
    'LAI',
    'BFIWs',
    'kffactcat',
    'permcat',
    'D50_mm_',
    'Q_10',
    'a',
    'b',
    'density',
    'specific_q',
    'bf_area',
    'bf_perimeter',
    'owp_tw_bf',
    'owp_y_bf',
    'wtod',
]

# Baseline tuned hyperparameters
DEFAULT_HYPERPARAMS: Dict = {
    'n_estimators': 711,
    'learning_rate': 0.012297214840537831,
    'max_depth': 9,
    'subsample': 0.5300665990757903,
    'colsample_bytree': 0.7318827417847731,
    'min_child_weight': 9,
    'tree_method': 'hist',
    'random_state': 42,
    'n_jobs': -1,
}

# Hyperparameter search space for grid search
PARAM_GRID: Dict = {
    'n_estimators': [500, 711, 872],
    'learning_rate': [0.01, 0.0123, 0.0265],
    'max_depth': [8, 9, 10],
    'subsample': [0.53, 0.75, 0.96],
    'colsample_bytree': [0.54, 0.73, 0.85],
    'min_child_weight': [1, 5, 9],
}

# Output reach identifier columns to retain in final prediction parquet
OUTPUT_IDENTIFIER_COLS: List[str] = [
    'huc8',
    'calb_coef_final',
    'HydroID',
    'feature_id',
    'branch_id',
    'obs_source',
    'submitter',
]


def setup_logger(output_dir: str) -> str:
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"ml_train_predict_{timestamp}.log")

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    root_logger = logging.getLogger("ML_calb")
    root_logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    if not any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in root_logger.handlers
    ):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    return log_file


def load_dataset(file_path: str) -> pd.DataFrame:
    """
    Load dataset from Parquet or CSV file and sanitize column names for XGBoost.

    Parameters
    ----------
    file_path : str
        Path to file (.parquet or .csv).

    Returns
    -------
    pd.DataFrame
        Loaded DataFrame with sanitized column names.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Dataset not found at '{file_path}'.")

    logger.debug(f"Loading dataset from: {file_path}")

    df = pd.read_parquet(file_path)

    # Sanitize column names ('D50[mm]' to 'D50_mm_')
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)

    logger.debug(f"Loaded {len(df):,} rows and {len(df.columns)} columns.")
    return df


def prepare_training_data(
    df: pd.DataFrame,
    feature_names: List[str],
    target_col: str = "log_calb",
    min_stream_order: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.Series, List[str], pd.DataFrame]:
    """
    Clean and prepare features and target variable for training.

    Parameters
    ----------
    df : pd.DataFrame
        Raw training data.
    feature_names : List[str]
        List of desired feature names.
    target_col : str, optional
        Target column name (default: 'log_calb').
    min_stream_order : Optional[int], optional
        Minimum stream order threshold to filter by.

    Returns
    -------
    Tuple[pd.DataFrame, pd.Series, List[str], pd.DataFrame]
        Prepared X DataFrame, y Series, available feature list, and filtered DataFrame.
    """
    data = df.copy()

    # Apply stream order filter if specified
    if min_stream_order is not None and "StreamOrde" in data.columns:
        initial_count = len(data)
        data = data[data["StreamOrde"] >= min_stream_order].copy()
        logger.debug(
            f"Filtered by StreamOrde >= {min_stream_order}: "
            f"{len(data):,} / {initial_count:,} records retained."
        )

    # Prepare target variable
    if target_col in data.columns:
        y_raw = data[target_col]
    elif "calb_coef_final" in data.columns:
        logger.debug("Computing log10-transformed target 'log_calb' from 'calb_coef_final'...")
        valid_mask = data["calb_coef_final"] > 0
        data = data[valid_mask].copy()
        data["log_calb"] = np.log10(data["calb_coef_final"])
        y_raw = data["log_calb"]
    else:
        raise ValueError(
            f"Target column '{target_col}' or 'calb_coef_final' not found in dataset columns: "
            f"{list(data.columns)}"
        )

    # Verify feature availability
    available_features = [f for f in feature_names if f in data.columns]
    missing_features = set(feature_names) - set(available_features)

    if missing_features:
        logger.warning(
            f"{len(missing_features)} feature(s) not found in dataset and will be omitted: "
            f"{sorted(missing_features)}"
        )

    if not available_features:
        raise ValueError("None of the specified features were found in the dataset.")

    X = data[available_features].copy()

    # Remove records with NaN target
    valid_target_mask = ~data[target_col].isna()
    X = X[valid_target_mask]
    y = data.loc[valid_target_mask, target_col].astype(float)
    data = data[valid_target_mask]

    logger.info(f"Prepared training dataset: {X.shape[0]:,} samples across {X.shape[1]} features.")
    return X, y, available_features, data


def train_and_evaluate_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: Optional[pd.DataFrame] = None,
    y_test: Optional[pd.Series] = None,
    tune_hyperparameters: bool = False,
    cv_folds: int = 5,
    random_state: int = 42,
    n_jobs: int = -1,
) -> Tuple[XGBRegressor, Dict[str, float]]:
    """
    Train XGBoost regressor, optionally run GridSearchCV, and evaluate performance.

    Parameters
    ----------
    X_train : pd.DataFrame
        Training feature matrix.
    y_train : pd.Series
        Training target values.
    X_test : Optional[pd.DataFrame], optional
        Test feature matrix.
    y_test : Optional[pd.Series], optional
        Test target values.
    tune_hyperparameters : bool, optional
        Whether to perform hyperparameter grid search (default: False).
    cv_folds : int, optional
        Number of cross-validation folds (default: 5).
    random_state : int, optional
        Random seed (default: 42).
    n_jobs : int, optional
        Number of parallel jobs (default: -1).

    Returns
    -------
    Tuple[XGBRegressor, Dict[str, float]]
        Trained XGBoost model and dictionary of performance metrics.
    """
    metrics: Dict[str, float] = {}

    if tune_hyperparameters:
        logger.info(f"Starting GridSearchCV with {cv_folds}-fold cross validation...")
        base_xgb = XGBRegressor(tree_method='hist', random_state=random_state)
        grid_search = GridSearchCV(
            estimator=base_xgb, param_grid=PARAM_GRID, scoring='r2', cv=cv_folds, verbose=1, n_jobs=n_jobs
        )
        grid_search.fit(X_train, y_train)
        best_model: XGBRegressor = grid_search.best_estimator_
        logger.info(f"GridSearchCV Best Parameters: {grid_search.best_params_}")
        logger.info(f"GridSearchCV Best Mean CV R2: {grid_search.best_score_:.4f}")
        metrics["cv_best_r2"] = float(grid_search.best_score_)
    else:
        logger.info("Fitting XGBoost model with default tuned parameters...")
        params = DEFAULT_HYPERPARAMS.copy()
        params["random_state"] = random_state
        params["n_jobs"] = n_jobs
        best_model = XGBRegressor(enable_categorical=True, **params)
        best_model.fit(X_train, y_train)

    # Training set metrics
    train_preds = best_model.predict(X_train)
    metrics["train_r2"] = float(r2_score(y_train, train_preds))
    metrics["train_rmse"] = float(np.sqrt(mean_squared_error(y_train, train_preds)))
    metrics["train_mae"] = float(mean_absolute_error(y_train, train_preds))
    metrics["train_medae"] = float(median_absolute_error(y_train, train_preds))

    logger.debug("=" * 60)
    logger.debug("TRAIN SET PERFORMANCE:")
    logger.debug(f"  R2 Score:     {metrics['train_r2']:.4f}")
    logger.debug(f"  RMSE:         {metrics['train_rmse']:.4f}")
    logger.debug(f"  MAE:          {metrics['train_mae']:.4f}")
    logger.debug(f"  Median AE:    {metrics['train_medae']:.4f}")

    # Test set metrics
    if X_test is not None and y_test is not None and len(X_test) > 0:
        test_preds = best_model.predict(X_test)
        metrics["test_r2"] = float(r2_score(y_test, test_preds))
        metrics["test_rmse"] = float(np.sqrt(mean_squared_error(y_test, test_preds)))
        metrics["test_mae"] = float(mean_absolute_error(y_test, test_preds))
        metrics["test_medae"] = float(median_absolute_error(y_test, test_preds))

        # Real-scale metrics
        y_test_linear = 10.0**y_test
        test_preds_linear = 10.0**test_preds
        metrics["test_linear_rmse"] = float(np.sqrt(mean_squared_error(y_test_linear, test_preds_linear)))
        metrics["test_linear_mae"] = float(mean_absolute_error(y_test_linear, test_preds_linear))

        logger.debug("-" * 60)
        logger.debug("TEST SET PERFORMANCE (log-scale):")
        logger.debug(f"  R2 Score:     {metrics['test_r2']:.4f}")
        logger.debug(f"  RMSE:         {metrics['test_rmse']:.4f}")
        logger.debug(f"  MAE:          {metrics['test_mae']:.4f}")
        logger.debug(f"  Median AE:    {metrics['test_medae']:.4f}")
        logger.debug("TEST SET PERFORMANCE (linear calb_coef scale):")
        logger.debug(f"  Linear RMSE:  {metrics['test_linear_rmse']:.4f}")
        logger.debug(f"  Linear MAE:   {metrics['test_linear_mae']:.4f}")

    logger.debug("=" * 60)
    return best_model, metrics


def save_model_artifacts(
    model: XGBRegressor, features: List[str], output_dir: str, metrics: Optional[Dict[str, float]] = None
) -> None:
    """
    Save trained model, feature importance table, and performance metrics.

    Parameters
    ----------
    model : XGBRegressor
        Trained model.
    features : List[str]
        List of features used.
    output_dir : str
        Directory to save artifacts.
    metrics : Optional[Dict[str, float]], optional
        Performance metrics dictionary.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Save model in standard XGBoost JSON format
    model_json_path = os.path.join(output_dir, "xgboost_calb_model.json")
    model.save_model(model_json_path)
    logger.debug(f"Saved trained model to: {model_json_path}")

    # Extract and save feature importances
    importances = model.feature_importances_
    fi_df = (
        pd.DataFrame({"feature": features, "importance": importances})
        .sort_values(by="importance", ascending=False)
        .reset_index(drop=True)
    )

    fi_path = os.path.join(output_dir, "feature_importance.csv")
    fi_df.to_csv(fi_path, index=False)
    logger.debug(f"Saved feature importances to: {fi_path}")

    logger.debug("Top 10 Most Important Features:")
    for idx, row in fi_df.head(10).iterrows():
        logger.debug(f"  {row['feature']:<25} : {row['importance']:.4f}")

    # Save metrics summary
    if metrics:
        metrics_df = pd.DataFrame([metrics])
        metrics_path = os.path.join(output_dir, "evaluation_metrics.csv")
        metrics_df.to_csv(metrics_path, index=False)
        logger.debug(f"Saved evaluation metrics to: {metrics_path}")


def predict_all_reaches(
    model: XGBRegressor, predict_df: pd.DataFrame, features: List[str], output_path: str
) -> pd.DataFrame:
    """
    Apply trained model to reach dataset and export cleaned prediction table.

    Parameters
    ----------
    model : XGBRegressor
        Trained XGBoost model.
    predict_df : pd.DataFrame
        DataFrame of reaches to predict on.
    features : List[str]
        Features list used during model training.
    output_path : str
        Destination path for final Parquet file.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with predictions.
    """
    logger.debug(f"Generating predictions for {len(predict_df):,} reaches...")

    df_out = predict_df.copy()

    X_pred = df_out[features].copy()

    # Predict in log scale and convert back to linear calibration coefficient
    log_predictions = model.predict(X_pred)
    df_out["prediction_calb"] = 10.0**log_predictions

    # Retain strictly required identifier columns + prediction_calb
    keep_cols = [col for col in OUTPUT_IDENTIFIER_COLS if col in df_out.columns]
    keep_cols.append("prediction_calb")

    df_clean = df_out[keep_cols].copy()

    # Ensure output directory exists and export
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    df_clean.to_parquet(output_path, index=False)
    logger.info(f"Saved reach predictions ({len(df_clean):,} records) to: {output_path}")

    return df_clean


def run_pipeline(
    train_file: str,
    predict_file: Optional[str] = None,
    output_dir: str = "./ml_model_output",
    output_pred_filename: str = "predictions_all_reaches.parquet",
    min_stream_order: Optional[int] = None,
    test_size: float = 0.0,
    tune_hyperparameters: bool = False,
    cv_folds: int = 5,
    random_state: int = 42,
    n_jobs: int = -1,
) -> bool:
    """
    Run full training, evaluation, and inference workflow.

    Parameters
    ----------
    train_file : str
        Path to training dataset (.parquet or .csv).
    predict_file : Optional[str], optional
        Path to prediction dataset (.parquet or .csv). Defaults to train_file if None.
    output_dir : str, optional
        Output directory for outputs and logs.
    output_pred_filename : str, optional
        Name of final predictions parquet file.
    min_stream_order : Optional[int], optional
        Filter records by minimum stream order.
    test_size : float, optional
        Proportion of dataset to include in test split (default: 0.0 for full-data training).
    tune_hyperparameters : bool, optional
        Whether to run grid search (default: False).
    cv_folds : int, optional
        Cross validation folds (default: 5).
    random_state : int, optional
        Random seed (default: 42).
    n_jobs : int, optional
        Parallel worker threads (default: -1).

    Returns
    -------
    bool
        True if pipeline succeeded, False otherwise.
    """
    start_time = time.time()
    os.makedirs(output_dir, exist_ok=True)
    log_file = setup_logger(output_dir)

    logger.debug("=" * 80)
    logger.debug("STARTING ML CALIBRATION COEFFICIENT TRAINING & PREDICTION PIPELINE")
    logger.debug(f"Min Stream Order:     {min_stream_order}")
    logger.debug(f"Test Size:            {test_size}")
    logger.debug(f"Tune Hyperparams:     {tune_hyperparameters}")
    logger.debug(f"Random State:         {random_state}")
    logger.debug("=" * 80)

    try:
        # Load and Prepare Training Data
        train_raw_df = load_dataset(train_file)
        X, y, selected_features, processed_train_df = prepare_training_data(
            df=train_raw_df,
            feature_names=DEFAULT_FEATURES,
            target_col="log_calb",
            min_stream_order=min_stream_order,
        )

        # Split Train/Test
        if test_size > 0:
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=random_state
            )
            logger.debug(
                f"Split data into Train ({len(X_train):,} samples) and Test ({len(X_test):,} samples)."
            )
        else:
            X_train, y_train = X, y
            X_test, y_test = None, None
            logger.debug(f"Training on all {len(X_train):,} samples (test_size=0).")

        # Train Model & Evaluate
        model, metrics = train_and_evaluate_model(
            X_train=X_train,
            y_train=y_train,
            X_test=X_test,
            y_test=y_test,
            tune_hyperparameters=tune_hyperparameters,
            cv_folds=cv_folds,
            random_state=random_state,
            n_jobs=n_jobs,
        )

        # Save Model Artifacts & Feature Importances
        save_model_artifacts(model=model, features=selected_features, output_dir=output_dir, metrics=metrics)

        # Generate Predictions
        pred_source_file = predict_file if predict_file and os.path.isfile(predict_file) else None
        if pred_source_file:
            logger.debug(f"Loading dedicated prediction dataset from: {pred_source_file}")
            predict_df = load_dataset(pred_source_file)
        else:
            logger.debug("Using full input dataset for prediction generation.")
            predict_df = train_raw_df

        output_pred_path = os.path.join(output_dir, output_pred_filename)
        predict_all_reaches(
            model=model, predict_df=predict_df, features=selected_features, output_path=output_pred_path
        )

        elapsed = time.time() - start_time
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {int(elapsed // 60)}m {int(elapsed % 60)}s!")
        logger.info(f"Predictions: {output_pred_path}")
        return True

    except Exception as e:
        logger.exception(f"Pipeline failed with error: {e}")
        return False


def main() -> None:
    """
    Run ML training/prediction pipeline.
    """
    parser = argparse.ArgumentParser(
        description="Train XGBoost model and predict Manning's calibration coefficients."
    )
    parser.add_argument(
        "-t",
        "--train-data",
        dest="train_data",
        type=str,
        required=True,
        help="Path to training dataset (.parquet).",
    )
    parser.add_argument(
        "-p",
        "--predict-data",
        dest="predict_data",
        type=str,
        required=True,
        help="Path to uncalibrated prediction dataset (.parquet).",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        dest="output_dir",
        type=str,
        required=True,
        help="Directory to save model artifacts, feature importances, and predictions.",
    )
    parser.add_argument(
        "--output-filename",
        dest="output_filename",
        type=str,
        required=False,
        default="predictions_all_reaches.parquet",
        help="Name of final predictions parquet file (default: predictions_all_reaches.parquet).",
    )
    parser.add_argument(
        "--min-stream-order",
        dest="min_stream_order",
        type=int,
        required=False,
        default=None,
        help="Filter training reaches by minimum stream order (e.g. 1).",
    )
    parser.add_argument(
        "--test-size",
        dest="test_size",
        type=float,
        required=False,
        default=0.0,
        help="Fraction of data reserved for test set evaluation (default: 0.0 for full-data training).",
    )
    parser.add_argument(
        "--tune",
        dest="tune",
        action="store_true",
        required=False,
        help="Run GridSearchCV hyperparameter tuning instead of using default tuned parameters.",
    )
    parser.add_argument(
        "--cv-folds",
        dest="cv_folds",
        type=int,
        required=False,
        default=5,
        help="Number of cross-validation folds (default: 5).",
    )
    parser.add_argument(
        "--random-state",
        dest="random_state",
        type=int,
        required=False,
        default=42,
        help="Random seed for data split and XGBoost model (default: 42).",
    )
    parser.add_argument(
        "--n-jobs",
        dest="n_jobs",
        type=int,
        required=False,
        default=-1,
        help="Number of parallel worker jobs (default: -1 for all cores).",
    )

    args = parser.parse_args()

    success = run_pipeline(
        train_file=args.train_data,
        predict_file=args.predict_data,
        output_dir=args.output_dir,
        output_pred_filename=args.output_filename,
        min_stream_order=args.min_stream_order,
        test_size=args.test_size,
        tune_hyperparameters=args.tune,
        cv_folds=args.cv_folds,
        random_state=args.random_state,
        n_jobs=args.n_jobs,
    )

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
