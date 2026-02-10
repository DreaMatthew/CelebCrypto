import pandas as pd
import numpy as np
import json
import re
from datetime import datetime, timedelta
import joblib
import os
from sklearn.model_selection import TimeSeriesSplit

# ================= Configuration Area =================
# Anonymized paths: replace these with your actual local paths
POLICY_PATH = 'impact_features_step1.jsonl'
PRED_PATH = 'BTC_Final_Predictions_2025.csv'
MODEL_SAVE_PATH = 'hourly_robust_stats.pkl'

WHITELIST_EVENTS = [
    'Nonfarm Payrolls', 'CPI', 'Retail Sales', 'ISM', 'Jobless Claims', 
    'Fed Interest Rate', 'GDP', 'FOMC', 'Manufacturing PMI'
]

# ================= Utility Functions =================

def clean_event_name(name):
    name = re.sub(r'\(.*?\)', '', name)
    return name.strip()

def align_policy_time(ts_str):
    try:
        dt = datetime.strptime(ts_str.split('_')[0] if '_' in ts_str else ts_str, "%A, %B %d, %Y %H:%M")
    except:
        dt = pd.to_datetime(ts_str)
    return dt.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

def prepare_dataset_v7():
    print(">>> Building dataset and sorting by time...")
    df_pred = pd.read_csv(PRED_PATH)
    df_pred = df_pred[df_pred['model'] == 'PatchTST'].copy()
    df_pred['forecast_origin_t'] = pd.to_datetime(df_pred['forecast_origin_t'])
    df_pred_indexed = df_pred.set_index('forecast_origin_t')
    
    samples = []
    with open(POLICY_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            item = json.loads(line)
            cleaned_event = clean_event_name(item['event_meta']['event_name'])
            if not any(core in cleaned_event for core in WHITELIST_EVENTS):
                continue
            t_origin = align_policy_time(item['timestamp'])
            if t_origin in df_pred_indexed.index:
                row = df_pred_indexed.loc[t_origin]
                y_vector = [(row[f't+{i}_true'] / row[f't+{i}_pred'] - 1) * 100 for i in range(1, 25)]
                samples.append({
                    'timestamp': t_origin,
                    'event_group': cleaned_event,
                    'direction': item['qualitative_logic']['direction'],
                    'y_vector': np.array(y_vector)
                })
    return pd.DataFrame(samples).sort_values('timestamp').reset_index(drop=True)

# ================= Core: Hourly Cross-Validation Optimization =================

def get_priors(df):
    p = df.groupby(['event_group', 'direction'])['y_vector'].apply(lambda x: np.mean(np.stack(x), axis=0)).to_dict()
    gp = df.groupby('direction')['y_vector'].apply(lambda x: np.mean(np.stack(x), axis=0)).to_dict()
    return p, gp

def predict_res(row, p, gp):
    key = (row['event_group'], row['direction'])
    return p.get(key, gp.get(row['direction'], np.zeros(24)))

def find_hourly_factors_no_leakage(df_train):
    """
    Find optimal coefficients for each of the 24 hours 
    using time-series cross-validation within the training set.
    """
    tscv = TimeSeriesSplit(n_splits=5)
    # Stores the optimal coefficient for each hour in each fold: shape (n_splits, 24)
    fold_hourly_factors = []
    
    for train_idx, val_idx in tscv.split(df_train):
        sub_train, sub_val = df_train.iloc[train_idx], df_train.iloc[val_idx]
        p, gp = get_priors(sub_train)
        
        y_val = np.stack(sub_val['y_vector'].values)
        p_val = np.stack([predict_res(r, p, gp) for _, r in sub_val.iterrows()])
        
        current_fold_f = []
        # Independent optimization for each hour
        for h in range(24):
            f_best_h, min_mae_h = 0, np.abs(y_val[:, h]).mean()
            # Search for best intensity factor on the validation set for this specific hour
            for f in np.linspace(0, 1.5, 76): 
                curr_mae = np.abs(y_val[:, h] - p_val[:, h] * f).mean()
                if curr_mae < min_mae_h:
                    min_mae_h, f_best_h = curr_mae, f
            current_fold_f.append(f_best_h)
        fold_hourly_factors.append(current_fold_f)
    
    # Average results across all folds to get the final 24-hour coefficient vector
    final_hourly_fs = np.mean(fold_hourly_factors, axis=0)
    print(f">>> Hourly optimization complete. t+1 factor: {final_hourly_fs[0]:.4f}, t+24 factor: {final_hourly_fs[-1]:.4f}")
    return final_hourly_fs

# ================= Statistics and Execution =================

def main():
    df = prepare_dataset_v7()
    if df.empty: return

    # 1. Strict 7:3 split
    split_idx = int(len(df) * 0.7)
    df_train, df_test = df.iloc[:split_idx], df.iloc[split_idx:]
    print(f"Train samples: {len(df_train)} | Test samples: {len(df_test)}")

    # 2. Calculate base priors based only on training set
    final_priors, final_global_priors = get_priors(df_train)

    # 3. Find 24 optimal coefficients based only on training set (hourly optimization)
    robust_hourly_fs = find_hourly_factors_no_leakage(df_train)

    # 4. Evaluate on the held-out test set
    y_test_true = np.stack(df_test['y_vector'].values)
    preds_test_raw = np.stack([predict_res(row, final_priors, final_global_priors) for _, row in df_test.iterrows()])
    
    # Apply hourly factors: y_test_pred = prior * factor_vector
    y_test_pred = preds_test_raw * robust_hourly_fs

    # 5. Print Report
    print("\n" + "="*85)
    print(f"[V7 Zero Leakage - Hourly Optimization] Test Set Evaluation Details")
    print("-" * 85)
    print(f"{'Hour':<6} | {'Best Factor':<12} | {'Original MAE':<12} | {'Refined MAE':<12} | {'Improvement %'}")
    print("-" * 85)

    for h in range(24):
        oh = np.abs(y_test_true[:, h]).mean()
        ch = np.abs(y_test_true[:, h] - y_test_pred[:, h]).mean()
        imp = (oh - ch) / oh * 100 if oh != 0 else 0
        status = "[Circuit Breaker]" if robust_hourly_fs[h] < 0.01 else ""
        print(f"t+{h+1:<2}   | {robust_hourly_fs[h]:<12.4f} | {oh:10.4f}% | {ch:10.4f}% | {imp:8.2f}% {status}")

    total_oh = np.abs(y_test_true).mean()
    total_ch = np.abs(y_test_true - y_test_pred).mean()
    total_imp = (total_oh - total_ch) / total_oh * 100
    
    print("-" * 85)
    print(f"{'Total':<6} | {'-':<12} | {total_oh:10.4f}% | {total_ch:10.4f}% | {total_imp:8.2f}%")
    print("="*85)

    joblib.dump({
        'priors': final_priors,
        'global_priors': final_global_priors,
        'best_hourly_factors': robust_hourly_fs
    }, MODEL_SAVE_PATH)

if __name__ == "__main__":
    main()