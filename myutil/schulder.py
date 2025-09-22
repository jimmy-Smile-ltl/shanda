import subprocess


def schedule_tasks(num_tasks):
    group_size = 4
    commands = []
    for start in range(1, num_tasks + 1, group_size):
        current_count = min(group_size, num_tasks - start + 1)
        tasks = [f"python task{n}.py" for n in range(start, start + current_count)]
        if current_count == 4:
            group_cmd = f'new-tab cmd /k "{tasks[0]}"'
            group_cmd += f' ; split-pane -H --percent 50 cmd /k "{tasks[1]}"'
            group_cmd += f' ; split-pane -V --percent 50 cmd /k "{tasks[2]}"'
            group_cmd += f' ; focus-pane -t 1'
            group_cmd += f' ; split-pane -V --percent 50 cmd /k "{tasks[3]}"'
        else:
            group_cmd = f'new-tab cmd /k "{tasks[0]}"'
            for task in tasks[1:]:
                group_cmd += f' ; split-pane -H cmd /k "{task}"'
        commands.append(group_cmd)
    full_command = "wt.exe " + " ; ".join(commands)
    subprocess.Popen(full_command, shell=True)


if __name__ == "__main__":
    num_tasks = int(input("请输入任务数量 (1-50): "))
    schedule_tasks(num_tasks)
