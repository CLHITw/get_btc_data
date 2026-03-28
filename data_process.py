# main_program.py

import pandas as pd
from get_data_module import get_data
from feature_calculator import feature_calculator
from kmeans_predict_module import predict_new_clusters
from kmeans_approx import KMeansApprox


def main():
    """
    BTC聚类预测主程序（增量更新模式）

    工作流程：
    1. 获取今天最新的BTC数据（1行）
    2. 追加到btc.xlsx的历史数据中（去重）
    3. 重新计算所有行的特征（保证滚动窗口正确）
    4. 预测btc.xlsx的最后一行，将k10-k15写回btc.xlsx
    5. 同时追加到cluster.xlsx（保留原有功能）
    6. 返回最新的k值
    """

    btc_file = "btc.xlsx"
    cluster_file = "cluster.xlsx"

    # ========================================
    # Step 1: 获取最新数据并追加到历史文件
    # ========================================
    print("🔹 Step 1/4: 获取并追加最新 BTC 数据...")

    try:
        new_data = get_data()

        if isinstance(new_data, dict):
            if not any(isinstance(v, (list, pd.Series)) for v in new_data.values()):
                df_new = pd.DataFrame([new_data])
            else:
                df_new = pd.DataFrame(new_data)
        elif isinstance(new_data, list):
            df_new = pd.DataFrame(new_data)
        else:
            df_new = pd.DataFrame(new_data)

        print(f"   📥 获取到 {len(df_new)} 条新数据")

        try:
            df_history = pd.read_excel(btc_file)
            print(f"   📚 历史数据: {len(df_history)} 条")
        except FileNotFoundError:
            print(f"   ⚠️ 未找到历史文件，将创建新文件")
            df_history = pd.DataFrame()

        if not df_history.empty:
            date_col = None
            for col in ['date', 'Date', 'datetime', 'Datetime']:
                if col in df_history.columns and col in df_new.columns:
                    date_col = col
                    break

            if date_col is None:
                df_combined = pd.concat([df_history, df_new], ignore_index=True)
                print(f"   ⚠️ 未找到日期列，直接追加数据")
            else:
                df_history[date_col] = pd.to_datetime(df_history[date_col])
                df_new[date_col] = pd.to_datetime(df_new[date_col])

                new_dates = df_new[date_col].values
                duplicate_dates = df_history[df_history[date_col].isin(new_dates)]

                if len(duplicate_dates) > 0:
                    print(f"   🔄 发现 {len(duplicate_dates)} 条重复日期，将更新数据")
                    df_history = df_history[~df_history[date_col].isin(new_dates)]

                df_combined = pd.concat([df_history, df_new], ignore_index=True)
                df_combined = df_combined.sort_values(date_col).reset_index(drop=True)
        else:
            df_combined = df_new

        df_combined.to_excel(btc_file, index=False)
        print(f"   ✅ 已保存到 {btc_file}")
        print(f"   📊 当前总计: {len(df_combined)} 条记录\n")

    except Exception as e:
        print(f"❌ 获取或追加数据失败: {e}")
        import traceback
        traceback.print_exc()
        return None

    # ========================================
    # Step 2: 计算特征
    # ========================================
    print("🔹 Step 2/4: 计算 BTC 特征...")

    try:
        df_with_features = feature_calculator(btc_file=btc_file)
        print("✅ 特征计算完成\n")

    except Exception as e:
        print(f"❌ 特征计算失败: {e}")
        import traceback
        traceback.print_exc()
        return None

    # ========================================
    # Step 3: 预测最后一行的 k10~k15
    # ========================================
    print("🔹 Step 3/4: 预测最新数据的 k10~k15...")

    try:
        k_df = predict_new_clusters(
            btc_file=btc_file,
            output_file=cluster_file,
            model_folder="./"
        )

        if k_df is None or k_df.empty:
            print("❌ 预测失败或结果为空\n")
            return None

    except Exception as e:
        print(f"❌ 预测过程出错: {e}")
        import traceback
        traceback.print_exc()
        return None

    # ========================================
    # Step 4: 将k10-k15写回btc.xlsx
    # ========================================
    print("🔹 Step 4/4: 将 k10~k15 写回 btc.xlsx...")

    try:
        # 读取btc.xlsx
        df_btc = pd.read_excel(btc_file)

        # 找到日期列
        date_col = None
        for col in ['date', 'Date', 'datetime', 'Datetime']:
            if col in df_btc.columns:
                date_col = col
                break

        # 确保k列存在于btc.xlsx中（如果不存在则添加）
        k_columns = ['k10', 'k11', 'k12', 'k13', 'k14', 'k15']
        for k_col in k_columns:
            if k_col not in df_btc.columns:
                df_btc[k_col] = None

        # 如果有日期列，按日期匹配更新
        if date_col and date_col in k_df.columns:
            df_btc[date_col] = pd.to_datetime(df_btc[date_col])
            k_df[date_col] = pd.to_datetime(k_df[date_col])

            # 遍历k_df的每一行，更新到df_btc对应日期的行
            for idx, row in k_df.iterrows():
                date_value = row[date_col]
                matching_indices = df_btc[df_btc[date_col] == date_value].index

                if len(matching_indices) > 0:
                    btc_idx = matching_indices[0]
                    for k_col in k_columns:
                        if k_col in row.index:
                            df_btc.loc[btc_idx, k_col] = row[k_col]
        else:
            # 如果没有日期列，按位置匹配（从后往前）
            print("   ⚠️ 未找到日期列，按位置匹配更新")
            start_idx = len(df_btc) - len(k_df)
            for i, (idx, row) in enumerate(k_df.iterrows()):
                btc_idx = start_idx + i
                if btc_idx >= 0 and btc_idx < len(df_btc):
                    for k_col in k_columns:
                        if k_col in row.index:
                            df_btc.loc[btc_idx, k_col] = row[k_col]

        # 保存回btc.xlsx
        df_btc.to_excel(btc_file, index=False)
        print(f"   ✅ k10~k15 已写回 {btc_file}")
        print(f"   📊 更新了 {len(k_df)} 行的 k 值\n")

    except Exception as e:
        print(f"❌ 写回k值失败: {e}")
        import traceback
        traceback.print_exc()
        # 即使写回失败，也继续返回结果
        pass

    # ========================================
    # Step 5: 提取并返回最新的 k 值
    # ========================================
    try:
        latest_row = k_df.iloc[-1]

        latest_k_values = {}

        for col in ['date', 'Date', 'datetime', 'Datetime', 'timestamp', 'time']:
            if col in k_df.columns:
                latest_k_values['date'] = str(latest_row[col])
                break

        for k in ['k10', 'k11', 'k12', 'k13', 'k14', 'k15']:
            if k in k_df.columns:
                latest_k_values[k] = int(latest_row[k])

        print("\n" + "=" * 60)
        print("📊 最新预测结果")
        print("=" * 60)
        if 'date' in latest_k_values:
            print(f"📅 日期: {latest_k_values['date']}")
        for k in ['k10', 'k11', 'k12', 'k13', 'k14', 'k15']:
            if k in latest_k_values:
                print(f"   {k.upper()}: {latest_k_values[k]}")
        print("=" * 60)
        print(f"💾 完整结果已保存到:")
        print(f"   - {btc_file} (包含所有数据+k值)")
        print(f"   - {cluster_file} (仅k值历史记录)\n")

        return latest_k_values

    except Exception as e:
        print(f"❌ 提取k值失败: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    k_values = main()

    if k_values:
        print("\n✅ 程序执行成功！")
        print(f"\n🎯 返回的k值:")
        for key, value in k_values.items():
            print(f"   {key}: {value}")
    else:
        print("\n❌ 程序执行失败")