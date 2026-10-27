#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        // One bulk launch. The object stays put for the life of the
        // task system (held by unique_ptr) so workers can touch the
        // atomics without the vector moving them.
        struct Launch {
            IRunnable* runnable;
            int total;
            std::atomic<int> next;
            std::atomic<int> remaining;
            int unmet;
            std::vector<TaskID> dependents;
            bool queued;
            bool done;

            Launch()
                : runnable(NULL), total(0), next(0), remaining(0),
                  unmet(0), queued(false), done(false) {}
        };

        // A slice of one launch. Workers run these, and a busy worker
        // can leave a slice on its deque for someone else to steal.
        struct Chunk {
            TaskID id;
            Launch* launch;
            int lo;
            int hi;
        };

        // Lock-free deque. The owner pushes and pops at the bottom.
        // Thieves pop at the top. Fixed capacity so we never allocate
        // on the steal path.
        struct WorkDeque {
            static const int kCap = 1024;
            Chunk buf[kCap];
            std::atomic<int> top;
            std::atomic<int> bottom;

            WorkDeque() : top(0), bottom(0) {}

            bool push(Chunk c);
            bool pop(Chunk* out);
            bool steal(Chunk* out);
        };

        // New launches land here. Only the thread that calls
        // runAsync / markComplete pushes. Any worker may pop.
        struct INode {
            Chunk chunk;
            std::atomic<INode*> next;
            INode() : next(NULL) {}
        };

        void workerLoop(int tid);
        bool popOwn(int tid, Chunk* out);
        bool steal(int tid, Chunk* out);
        bool popInjector(Chunk* out);
        void publishSplit(int tid, Chunk chunk);
        void runChunk(Chunk chunk);
        void enqueueReady(TaskID id);
        void markComplete(TaskID id);
        void wakeIfIdle(int prev_count);

        int num_threads_;
        std::vector<std::thread> workers_;
        std::vector<std::unique_ptr<WorkDeque> > deques_;

        std::mutex mutex_;
        std::condition_variable work_cv_;
        std::condition_variable sync_cv_;
        bool shutdown_;
        int unfinished_;
        std::atomic<int> ready_count_;
        std::atomic<INode*> injector_;
        // Stable addresses. Grown only while mutex_ is held, and only
        // before the node is published to a worker.
        std::deque<INode> nodes_;

        std::vector<std::unique_ptr<Launch> > launches_;
};

#endif
