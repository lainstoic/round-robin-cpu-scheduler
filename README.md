# Round-Robin CPU Scheduling Simulator

Developed a process scheduling simulator in C that models operating system CPU scheduling using the Round-Robin algorithm.

The program simulates a kernel managing 10 child processes, using timer interrupts and System V message queues to coordinate CPU execution and I/O operations.

### Key Features

- Implemented Round-Robin scheduling with a configurable simulation duration.
- Created and managed 10 child processes using `fork()`.
- Implemented inter-process communication using System V message queues.
- Used `SIGALRM` and `setitimer()` to simulate periodic timer interrupts.
- Managed ready and I/O waiting queues using linked lists.
- Implemented process preemption with a 5-tick time quantum.
- Simulated randomized CPU and I/O bursts.
- Generated detailed scheduling logs across 10,000 simulation ticks.
- Implemented process termination and IPC resource cleanup.

### Results

- Simulated 10 concurrent child processes over 10,000 scheduling ticks.
- Demonstrated CPU allocation across processes without starvation in the recorded run.
- Generated execution logs documenting scheduling decisions, CPU bursts, and I/O transitions.

### Technologies

C · Linux · GCC · Operating Systems · CPU Scheduling · Inter-Process Communication · POSIX Signals · System V Message Queues
