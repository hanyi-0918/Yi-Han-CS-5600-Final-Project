#include <stdio.h>    // 用于 printf
#include <stdlib.h>   // 用于 exit
#include <string.h>   // 用于 memcpy, memset
#include <unistd.h>   // 用于 sleep, write, read, close, fsync
#include <fcntl.h>    // 用于 open, O_RDONLY, O_WRONLY, O_CREAT
#include <signal.h>   // 用于 signal

/*
 * 这是我们模拟的“进程状态”。
 * 在真实应用中，这可能是数GB的复杂数据。
 * 在我们的模拟中，它只是一个计数器和一些数据。
 */
#define DATA_SIZE 1024 // 1KB 的数据
struct ProcessState {
    long update_counter;         // 我们总共完成了多少次“工作”
    char data[DATA_SIZE];      // 模拟的工作数据
};

// 检查点文件的名字
const char* CHECKPOINT_FILE = "checkpoint.dat";

// 信号处理函数，用于优雅地退出 (例如按 Ctrl+C)
void handle_exit(int sig) {
    printf("\n捕获到退出信号...正在关闭。\n");
    exit(0);
}

/*
 * 函数：保存检查点 (Save Checkpoint)
 * 将内存中的完整状态写入磁盘。
 */
void save_checkpoint(struct ProcessState *state) {
    printf("正在保存检查点 (Saving Checkpoint) (计数: %ld)...\n", state->update_counter);
    
    // O_WRONLY: 只写模式
    // O_CREAT: 如果文件不存在则创建
    // O_TRUNC: 如果文件已存在，则清空内容
    // 0644: 文件权限
    int fd = open(CHECKPOINT_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        perror("打开 checkpoint 文件失败");
        return;
    }

    // 1. 将整个结构体写入文件
    if (write(fd, state, sizeof(struct ProcessState)) != sizeof(struct ProcessState)) {
        perror("写入 checkpoint 失败");
        close(fd);
        return;
    }

    // 2. [关键] 强制将数据从 OS 缓冲区刷新到物理磁盘！
    // 这模拟了数据的持久化。
    fsync(fd); 
    
    close(fd);
    printf("检查点已保存。\n");
}

/*
 * 函数：加载检查点 (Load Checkpoint)
 * 在启动时调用，用于恢复状态。
 */
void load_checkpoint(struct ProcessState *state) {
    printf("尝试从 %s 加载检查点...\n", CHECKPOINT_FILE);

    int fd = open(CHECKPOINT_FILE, O_RDONLY);
    if (fd == -1) {
        // 文件不存在，这是一个“冷启动”
        printf("未找到检查点。正在初始化新状态。\n");
        state->update_counter = 0;
        memset(state->data, 0, DATA_SIZE); // 清空数据
        return;
    }

    // 尝试从文件读取状态
    if (read(fd, state, sizeof(struct ProcessState)) != sizeof(struct ProcessState)) {
        // 文件可能已损坏或不完整
        fprintf(stderr, "读取 checkpoint 失败或文件损坏。正在退出。\n");
        close(fd);
        exit(1);
    }
    
    close(fd);
    printf("成功恢复状态！从计数 %ld 继续。\n", state->update_counter);
}


/*
 * 主函数
 */
int main() {
    // 捕获 Ctrl+C 信号，以便能优雅退出
    signal(SIGINT, handle_exit);

    struct ProcessState state;

    // === 1. 恢复 (RECOVERY) ===
    // 程序启动时，首先尝试加载上一个检查点
    load_checkpoint(&state);

    // === 2. 模拟工作 (DO WORK) ===
    // 这是一个无限循环，模拟一个持续运行的进程
    while (1) {
        // 模拟做一些“工作”
        state.update_counter++;
        state.data[0] = 'A'; // 随便修改一些内存数据
        
        printf("完成工作单元 #%ld\n", state.update_counter);

        // === 3. 检查点 (CHECKPOINT) ===
        // 每完成 10 次工作，就保存一次检查点
        if (state.update_counter % 10 == 0) {
            save_checkpoint(&state);
        }
        
        // 慢一点，方便我们观察
        sleep(1);
    }

    return 0;
}