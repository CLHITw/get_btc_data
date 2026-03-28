# kmeans_predict_module.py
import pandas as pd
import joblib
from pathlib import Path

def predict_new_clusters(btc_file, output_file=None, model_folder="./"):
    TRAINING_FEATURES = [
        'rsi_norm', 'boll_width_z', 'macd_z', 'macd_hist_z', 'rel_macd_hist_z',
        'volatility_pct_z', 'volume_pct', 'volume_log_z', 'num_trades_z',
        'taker_buy_base_ratio', 'taker_buy_quote_ratio',
        'dist_ma7_pct_z', 'dist_ma50_pct_z', 'dist_ma99_pct_z', 'dist_ma200_pct_z',
        'atr_pct_z', 'fginorm'
    ]

    try:
        df_btc = pd.read_excel(btc_file)
        if df_btc.empty:
            return None

        date_col = None
        for col in ['date', 'Date', 'datetime', 'Datetime', 'timestamp', 'time']:
            if col in df_btc.columns:
                date_col = col
                break
        if date_col is None:
            return None

        if 'close' not in df_btc.columns:
            return None

        df_btc[date_col] = pd.to_datetime(df_btc[date_col], errors='coerce')

        missing_features = [col for col in TRAINING_FEATURES if col not in df_btc.columns]
        if missing_features:
            return None

        df_valid = df_btc.dropna(subset=TRAINING_FEATURES).copy()
        if df_valid.empty:
            return None

        X = df_valid[TRAINING_FEATURES]
        if X.isna().any().any():
            X = X.fillna(0)

        model_folder_path = Path(model_folder)
        predictions = {
            date_col: df_valid[date_col],
            'close': df_valid['close']
        }

        for k in range(10, 16):
            possible_filenames = [
                f"kmeans_model_{k}.joblib",
                f"kmeans_k{k}.joblib",
                f"kmeans_{k}.joblib",
            ]
            model_file = None
            for filename in possible_filenames:
                test_path = model_folder_path / filename
                if test_path.exists():
                    model_file = test_path
                    break

            if model_file is None:
                continue

            try:
                kmeans_model = joblib.load(model_file)
                clusters = kmeans_model.predict(X)
                predictions[f'k{k}'] = clusters
            except:
                continue

        df_clusters = pd.DataFrame(predictions)

        if df_clusters.shape[1] <= 2:
            return None

        if date_col != 'date':
            df_clusters = df_clusters.rename(columns={date_col: 'date'})

        k_cols = [f'k{k}' for k in range(10, 16) if f'k{k}' in df_clusters.columns]
        ordered_cols = ['date', 'close'] + k_cols
        df_clusters = df_clusters[[col for col in ordered_cols if col in df_clusters.columns]]

        df_clusters = df_clusters.sort_values('date').reset_index(drop=True)

        if output_file is not None:
            df_clusters.to_excel(output_file, index=False)

        return df_clusters

    except:
        return None


def show_prediction_summary(cluster_file):
    try:
        df = pd.read_excel(cluster_file)
        date_col = next((col for col in ['date', 'Date', 'datetime', 'Datetime'] if col in df.columns), None)
        display_cols = [c for c in ['date', 'close'] + [f'k{k}' for k in range(10,16)] if c in df.columns]
        if display_cols:
            pass
    except:
        pass


if __name__ == "__main__":
    pass