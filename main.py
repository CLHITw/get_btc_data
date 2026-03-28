# scheduler_task.py

import schedule
import time
from datetime import datetime
from data_process import main  # 导入你的主函数
from kmeans_approx import  KMeansApprox

def trading_signal_checker():
    """
    每天凌晨1:10执行的交易信号检测任务

    逻辑：
    - 调用 data_process.main() 获取最新的 k 值
    - 检查 k13 == 10 → 输出"开仓"
    - 检查 k11 == 4 → 输出"平仓"
    """

    print("\n" + "=" * 70)
    print(f"⏰ 定时任务开始执行 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    try:
        # 调用主函数获取最新 k 值
        k_values = main()

        if k_values is None:
            print("\n❌ 主函数执行失败，无法获取 k 值")
            print("=" * 70)
            return

        # 提取 k11 和 k13
        k11 = k_values.get('k11')
        k13 = k_values.get('k13')
        date = k_values.get('date')
        close = k_values.get('close')

        print("\n" + "=" * 70)
        print("🔔 交易信号检测")
        print("=" * 70)

        if date:
            print(f"📅 日期: {date}")
        if close:
            print(f"💰 收盘价: {close:.2f}")

        print(f"\n📊 关键指标:")
        print(f"   K11: {k11}")
        print(f"   K13: {k13}")

        # 交易信号逻辑
        signals = []

        if k13 == 10:
            signal = "🟢 开仓信号"
            signals.append(signal)
            print(f"\n{signal}")
            print(f"   原因: k13 = {k13} (等于 10)")

        if k11 == 4:
            signal = "🔴 平仓信号"
            signals.append(signal)
            print(f"\n{signal}")
            print(f"   原因: k11 = {k11} (等于 4)")

        if not signals:
            print(f"\n⚪ 无交易信号")
            print(f"   k13 ≠ 10 且 k11 ≠ 4，保持当前仓位")

        print("=" * 70)

        # 可选：保存信号到日志文件
        log_signal_to_file(date, close, k11, k13, signals)

    except Exception as e:
        print(f"\n❌ 任务执行出错: {e}")
        import traceback
        traceback.print_exc()
        print("=" * 70)


def log_signal_to_file(date, close, k11, k13, signals):
    """
    将交易信号记录到日志文件

    Args:
        date: 日期
        close: 收盘价
        k11: k11 值
        k13: k13 值
        signals: 信号列表
    """
    try:
        import os

        # 创建日志文件夹
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # 日志文件路径
        log_file = os.path.join(log_dir, "trading_signals.txt")

        # 写入日志
        with open(log_file, 'a', encoding='utf-8') as f:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"\n{'=' * 50}\n")
            f.write(f"时间: {timestamp}\n")
            f.write(f"日期: {date}\n")
            f.write(f"收盘价: {close:.2f}\n")
            f.write(f"K11: {k11}, K13: {k13}\n")

            if signals:
                f.write(f"信号: {', '.join(signals)}\n")
            else:
                f.write(f"信号: 无交易信号\n")

            f.write(f"{'=' * 50}\n")

        print(f"\n📝 信号已记录到: {log_file}")

    except Exception as e:
        print(f"\n⚠️ 日志记录失败: {e}")


def test_run():
    """
    测试运行一次（不等待定时）
    """
    print("\n🧪 测试模式：立即执行一次任务\n")
    trading_signal_checker()


if __name__ == "__main__":
    import sys

    # 检查是否是测试模式
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # 测试模式：立即执行一次
        test_run()
    else:
        # 正式模式：设置定时任务
        print("=" * 70)
        print("🚀 交易信号监控系统启动")
        print("=" * 70)
        print(f"⏰ 定时任务已设置：每天凌晨 1:10 执行")
        print(f"📍 当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"💡 提示：按 Ctrl+C 停止程序")
        print("=" * 70)

        # 设置定时任务：每天凌晨 1:10 执行
        schedule.every().day.at("01:12").do(trading_signal_checker)

        # 可选：测试时使用（每分钟执行一次）
        # schedule.every(1).minutes.do(trading_signal_checker)

        # 运行调度器
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)  # 每秒检查一次
            except KeyboardInterrupt:
                print("\n\n⚠️ 收到停止信号")
                print("=" * 70)
                print("🛑 程序已停止")
                print("=" * 70)
                break
            except Exception as e:
                print(f"\n❌ 调度器出错: {e}")
                import traceback

                traceback.print_exc()
                # 继续运行
                time.sleep(60)