# -*- coding: utf-8 -*-
# @Time    : 2025/7/1 11:07
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : split_window.py
# @Software: PyCharm
import math
import subprocess
import sys


def _calculate_grid(n: int) -> tuple[int, int]:
    """
    计算给定窗口总数的最优网格布局（行数, 列数）。
    目标是使布局尽可能接近正方形。

    :param n: 窗口总数。
    :return: 一个包含 (rows, cols) 的元组。
    """
    if n <= 0:
        return 0, 0
    # 从总数的平方根开始向下寻找最接近的因子
    sqrt_n = int(math.sqrt(n))
    for rows in range(sqrt_n, 0, -1):
        if n % rows == 0:
            cols = n // rows
            return rows, cols
    # 如果是素数，则返回 1 行 n 列
    return 1, n


def create_windows_terminal_layout(num_windows: int = 4, profile_name: str = "PowerShell"):
    """
    使用Python调用 wt.exe，根据指定的窗口总数，创建一个大小均等的网格布局。

    :param num_windows: 您希望创建的窗口（窗格）总数。
    :param profile_name: 要在每个窗格中打开的终端配置文件名称。
                         常见的有 "PowerShell", "cmd", "Ubuntu" (如果您安装了WSL)。
    """
    if sys.platform != "win32":
        print("错误：此脚本专为 Windows Terminal 设计，请在 Windows 系统上运行。")
        return

    if num_windows <= 0:
        print("错误：窗口数量必须大于0。")
        return

    rows, cols = _calculate_grid(num_windows)
    print(f"为 {num_windows} 个窗口计算出的最佳布局为: {rows} 行 x {cols} 列。")
    print(f"准备创建布局，使用 '{profile_name}' 配置文件...")

    # 1. 构建命令的起点：打开第一个窗格 (左上角)
    pane_counter = 1
    # 使用 Read-Host 来保持窗口打开，直到用户按Enter
    test_command = f'echo test'
    command = ["wt.exe", "-p", profile_name, "new-tab", "--title", "Automated Layout", "--commandline", test_command]

    # 2. 逐列构建布局
    for c in range(1, cols + 1):
        # 创建当前列的剩余行
        for r in range(1, rows):
            pane_counter += 1
            # 在当前窗格下方创建一个新的水平窗格
            command.extend([";", "split-pane", "-p", profile_name, "-H", "--commandline", test_command])

        # 如果不是最后一列，则准备创建下一列
        if c < cols:
            # 首先，将焦点移回当前列的顶部窗格
            command.extend([";", "move-focus", "up"] * (rows - 1))
            # 然后，在右侧创建一个新的垂直窗格（即下一列的顶部窗格）
            pane_counter += 1
            command.extend([";", "split-pane", "-p", profile_name, "-V", "--commandline", test_command])

    print("\n生成的命令字符串:")
    # 使用 subprocess.list2cmdline 来安全地显示完整的命令
    # 注意：在PowerShell中直接运行此长命令可能需要额外的转义处理
    print(subprocess.list2cmdline(command))

    # 3. 执行命令
    try:
        subprocess.run(command, check=True, shell=True)  # 使用 shell=True 以更好地处理复杂的命令链
        print("\nWindows Terminal 布局已成功创建！")
    except FileNotFoundError:
        print("\n错误：找不到 'wt.exe'。请确保您已从 Microsoft Store 安装了 Windows Terminal。")
    except subprocess.CalledProcessError as e:
        print(f"\n执行命令时出错: {e}")


if __name__ == "__main__":
    # 您可以在这里自定义想要的窗口总数
    # 例如，创建一个包含 6 个均等大小窗格的布局
    create_windows_terminal_layout(num_windows=10)

    # 或者一个包含 8 个窗格的布局
    # create_windows_terminal_layout(num_windows=8)
