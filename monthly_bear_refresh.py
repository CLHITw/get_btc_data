from __future__ import annotations

import json
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd


DATA_FILE = Path("/root/Desktop/btc/get_data/get_btc_data/btc.xlsx")
OUTPUT_DIR = Path("/root/Desktop/btc/get_data/get_btc_data")
OUTPUT_JSON = OUTPUT_DIR / "combo.json"

TRAIN_SIZE = 900
EMBARGO_DAYS = 11
BEAR_MEDIAN_THRESHOLD = 0.01
BEAR_PROB_THRESHOLD_5D = 0.55
MIN_SHARED_POSITIONS = 4
MIN_COMBO_OBSERVATIONS = 3
MIN_PATTERN_SUPPORT = 2

K_COLUMNS = ["k10", "k11", "k12", "k13", "k14", "k15"]


def load_data() -> pd.DataFrame:
    df = pd.read_excel(DATA_FILE)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    numeric_cols = [col for col in df.columns if col != "date"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    df["ret_fwd_5d"] = df["close"].shift(-5) / df["close"] - 1.0
    df["ret_fwd_10d"] = df["close"].shift(-10) / df["close"] - 1.0

    df = df.dropna(subset=K_COLUMNS).copy()
    df = df.dropna(subset=["ret_fwd_5d", "ret_fwd_10d"]).reset_index(drop=True)

    for col in K_COLUMNS:
        df[col] = df[col].astype(int)

    df["k_combo"] = df[K_COLUMNS].apply(lambda row: tuple(int(v) for v in row), axis=1)
    return df


def get_raw_last_date() -> pd.Timestamp:
    df_raw = pd.read_excel(DATA_FILE, usecols=["date"])
    df_raw["date"] = pd.to_datetime(df_raw["date"])
    return df_raw["date"].max().normalize()


def compute_stats(returns: pd.Series) -> dict[str, float]:
    returns = pd.to_numeric(returns, errors="coerce").dropna()
    up = returns[returns > 0]
    down = returns[returns < 0]
    total = len(returns)

    if total == 0:
        return {
            "count": 0,
            "up_prob": np.nan,
            "down_prob": np.nan,
            "median_down_return": np.nan,
        }

    return {
        "count": int(total),
        "up_prob": float(len(up) / total),
        "down_prob": float(len(down) / total),
        "median_down_return": float(down.median()) if len(down) else np.nan,
    }


def classify_bear(stats_5d: dict[str, float], stats_10d: dict[str, float]) -> bool:
    bear_5d = stats_5d["down_prob"] > stats_5d["up_prob"] and stats_5d["down_prob"] >= BEAR_PROB_THRESHOLD_5D
    bear_10d = stats_10d["down_prob"] > stats_10d["up_prob"]
    strong_bear = pd.notna(stats_5d["median_down_return"]) and abs(stats_5d["median_down_return"]) > BEAR_MEDIAN_THRESHOLD
    return bear_5d and bear_10d and strong_bear


def combo_to_string(combo: tuple[int, ...]) -> str:
    return "|".join(str(v) for v in combo)


def combo_pattern(combo_a: tuple[int, ...], combo_b: tuple[int, ...]) -> dict[str, int] | None:
    shared = {k_col: combo_a[idx] for idx, k_col in enumerate(K_COLUMNS) if combo_a[idx] == combo_b[idx]}
    if len(shared) >= MIN_SHARED_POSITIONS:
        return shared
    return None


def extract_patterns(regime_combos: list[tuple[int, ...]]) -> list[str]:
    pattern_map: dict[tuple[tuple[str, int], ...], set[str]] = {}

    for combo_a, combo_b in combinations(regime_combos, 2):
        shared = combo_pattern(combo_a, combo_b)
        if shared is None:
            continue

        key = tuple(sorted(shared.items()))
        source = pattern_map.setdefault(key, set())
        source.add(combo_to_string(combo_a))
        source.add(combo_to_string(combo_b))

    patterns: list[str] = []
    for key, sources in pattern_map.items():
        if len(sources) >= MIN_PATTERN_SUPPORT:
            patterns.append("|".join(f"{k}={v}" for k, v in key))
    return patterns


def parse_pattern_str(pattern: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in pattern.split("|"):
        key, value = part.split("=", 1)
        out[key] = value
    return out


def parse_pattern_int(pattern: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for part in pattern.split("|"):
        key, value = part.split("=", 1)
        out[key] = int(value)
    return out


def is_strict_subset(pattern_a: str, pattern_b: str) -> bool:
    a = parse_pattern_str(pattern_a)
    b = parse_pattern_str(pattern_b)
    if len(a) >= len(b):
        return False
    return all(k in b and b[k] == v for k, v in a.items())


def dedup_patterns(patterns: list[str]) -> list[str]:
    patterns = sorted(set(patterns), key=lambda x: (len(parse_pattern_str(x)), x))
    keep: list[str] = []
    for pattern in patterns:
        if any(is_strict_subset(existing, pattern) for existing in keep):
            continue
        keep.append(pattern)
    return keep


def build_bear_patterns(train_df: pd.DataFrame) -> list[str]:
    bear_combos: list[tuple[int, ...]] = []

    for combo, combo_df in train_df.groupby("k_combo"):
        stats_5d = compute_stats(combo_df["ret_fwd_5d"])
        stats_10d = compute_stats(combo_df["ret_fwd_10d"])

        if stats_5d["count"] < MIN_COMBO_OBSERVATIONS or stats_10d["count"] < MIN_COMBO_OBSERVATIONS:
            continue
        if classify_bear(stats_5d, stats_10d):
            bear_combos.append(combo)

    return dedup_patterns(extract_patterns(bear_combos))


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    raw_last_date = get_raw_last_date()
    df = load_data()

    eligible_end_date = raw_last_date - pd.Timedelta(days=EMBARGO_DAYS)
    eligible_rows = df.index[df["date"] <= eligible_end_date].tolist()
    if len(eligible_rows) < TRAIN_SIZE:
        raise ValueError(f"Not enough data: need {TRAIN_SIZE}, got {len(eligible_rows)}")

    train_end_idx = eligible_rows[-1] + 1
    train_start_idx = train_end_idx - TRAIN_SIZE
    train_df = df.iloc[train_start_idx:train_end_idx].copy()

    bear_patterns = build_bear_patterns(train_df)

    result = {
        "data_last_date": raw_last_date.strftime("%Y-%m-%d"),
        "train_start_date": train_df["date"].iloc[0].strftime("%Y-%m-%d"),
        "train_end_date": train_df["date"].iloc[-1].strftime("%Y-%m-%d"),
        "effective_start_date": pd.Timestamp.today().strftime("%Y-%m-%d"),
        "bear": [parse_pattern_int(p) for p in bear_patterns],
    }

    OUTPUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved_json={OUTPUT_JSON}")
    print(f"data_last_date={raw_last_date.strftime('%Y-%m-%d')}")
    print(f"train_range={result['train_start_date']} ~ {result['train_end_date']}")
    print(f"bear_patterns={len(bear_patterns)}")


if __name__ == "__main__":
    main()
