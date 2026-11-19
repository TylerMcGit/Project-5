#include<unistd.h>
#include<sys/types.h>
#include<stdio.h>
#include<stdlib.h>
#include<string>
#include<iostream>
#include<fstream>
#include<time.h>
#include<sys/wait.h>
#include<sys/ipc.h>
#include<sys/shm.h>
#include<sys/msg.h>
#include<signal.h>
#include "resource.h"
using namespace std;

struct PCB {
        int occupied; // either true or false
        pid_t pid; // process id of this child
        unsigned int startSeconds; // time when it was forked
        unsigned int startNano; // time when it was forked
        int blocked; // 1 if it's waiting on a resource
        int resourcesAllocated[NUM_RESOURCES]; // how many of each resource it holds
        int requestedResource; // resource it's blocked on (-1 = none)
};

int shm_id;
int msq_id;
struct PCB p[MAX_PROCS];
int available[NUM_RESOURCES]; //instances of each resource nobody holds
ofstream logfile;
int log_lines = 0; //lines written to the log so far
unsigned int *sec; //global so helper functions can stamp the time on log lines
unsigned int *nano;

//statistics for the end report
int total_requests = 0; //every request a worker made
int immediate_grants = 0; //requests granted the moment they were asked
int grant_count = 0; //all grants, used for the table every 20 grants
int detection_runs = 0; //times the deadlock algorithm ran
int deadlock_kills = 0; //processes oss killed to break a deadlock
int normal_terms = 0; //processes that ended because their time was up

int parse(int argc, char *argv[], int &proc, int &simul, float &time_limit, float &interval, string &log_name);

void incrementClock(unsigned int *sec, unsigned int *nano, unsigned long long nano_to_add);

unsigned int *initializeMemory();

void initializeQueue();

void signalHandler(int sig);

void printOut(const char *line);

void printTable();

void printResources();

void printReport();

void grantResource(int idx, int r);

void releaseAll(int idx);

void unblockProcesses();

int detectDeadlock(int deadlocked[]);

void resolveDeadlock(int &proc_running);

int main(int argc, char* argv[]) {
        int proc_running = 0, total = 0;
        int status;
        int proc;
        int simul;
        float time_limit;
        float interval;
        string log_name;
        char line[200]; //holds one line of output before it goes to the screen and the log file

        parse(argc, argv, proc, simul, time_limit, interval, log_name); //parse first so -h doesn't leave shared memory behind

        logfile.open(log_name.c_str());
        if(!logfile) {
                cout << "Could not open log file " << log_name << endl;
                exit(1);
        }

        unsigned int *clock = initializeMemory();
        sec = &(clock[0]);
        nano = &(clock[1]);
        *sec = *nano = 0;

        initializeQueue();
        srand(time(NULL)); //so each run gives the workers different random times

        for(int r = 0; r < NUM_RESOURCES; r++)
                available[r] = INSTANCES; //everything starts free
        for(int j = 0; j < MAX_PROCS; j++)
                p[j].requestedResource = -1;

        signal(SIGINT, signalHandler); //ctrl-c
        signal(SIGALRM, signalHandler); //safety net if something hangs
        alarm(60);

        sprintf(line, "OSS starting, PID:%d PPID:%d\n", getpid(), getppid());
        printOut(line);
        sprintf(line, "Called with: -n %d -s %d -t %g -i %g -f %s\n", proc, simul, time_limit, interval, log_name.c_str());
        printOut(line);

        //split -t into the most seconds and nanoseconds a worker can live
        int max_sec = time_limit;
        int max_nano = (time_limit - max_sec) * 1000000000;

        int i = 0;
        int last_print = -1; //last half second the table was printed
        int last_detect = 0; //last whole second we checked for deadlock
        int last_index = -1; //last process table entry that got a turn (round robin)
        int launch_sec = 0, launch_nano = 0; //time the last child was launched
        time_t start_real = time(NULL); //real time oss started, for the 5 second launch limit
        struct msgbuffer buf;

        //keep going while we can still launch OR children are still in the system
        while((total < proc && time(NULL) - start_real < 5) || proc_running > 0) {
                //possibly launch a new child: more to launch, under 5 real seconds, under -s, and -i time has passed
                double since_launch = ((double)*sec - launch_sec) + ((double)*nano - launch_nano) / 1000000000.0;
                if(total < proc && time(NULL) - start_real < 5 && proc_running < simul && (total == 0 || since_launch >= interval)) {
                        for(i = 0; i < MAX_PROCS; i++) { //find the first free spot in the process table
                                if(!p[i].occupied)
                                        break;
                        }

                        //pick a random lifetime between 0 and -t for this worker
                        int run_sec = rand() % (max_sec + 1);
                        int run_nano;
                        if(run_sec == max_sec)
                                run_nano = rand() % (max_nano + 1); //can't go over the nanoseconds of -t
                        else
                                run_nano = rand() % 1000000000;

                        p[i].startSeconds = *sec; //time right before the fork
                        p[i].startNano = *nano;
                        pid_t pid = fork();
                        if (pid == -1) {
                                cout << "Failed to launch child" << endl;
                                signalHandler(0);
                        }
                        else if (pid == 0) {
                                execlp("./worker","./worker",to_string(run_sec).c_str(), to_string(run_nano).c_str(), (char*) NULL);
                                cout << "exec of ./worker failed" << endl; //only gets here if exec fails
                                exit(1);
                        }
                        //parent fills in the rest of the process table
                        p[i].occupied = 1;
                        p[i].pid = pid;
                        p[i].blocked = 0;
                        p[i].requestedResource = -1;
                        for(int r = 0; r < NUM_RESOURCES; r++)
                                p[i].resourcesAllocated[r] = 0;
                        proc_running++;
                        total++;

                        sprintf(line, "OSS: Generating process P%d with PID %d at time %u:%u\n", i, pid, *sec, *nano);
                        printOut(line);

                        launch_sec = *sec;
                        launch_nano = *nano;
                }

                //give resources to anybody blocked whose resource freed up
                unblockProcesses();

                incrementClock(sec, nano, 10000000); //10ms

                //round robin: find the next process after the last one that isn't blocked
                int next = -1;
                for(int k = 1; k <= MAX_PROCS; k++) {
                        int idx = (last_index + k) % MAX_PROCS;
                        if(p[idx].occupied && !p[idx].blocked) {
                                next = idx;
                                break;
                        }
                }

                if(next != -1) {
                        last_index = next;

                        buf.mtype = p[next].pid; //only that child picks this message up
                        buf.intData = MSG_GO;
                        if(msgsnd(msq_id, &buf, sizeof(buf) - sizeof(long), 0) == -1) {
                                perror("oss: msgsnd");
                                signalHandler(0);
                        }

                        //wait for the child to answer (workers send their message to oss's pid)
                        if(msgrcv(msq_id, &buf, sizeof(buf) - sizeof(long), getpid(), 0) == -1) {
                                perror("oss: msgrcv");
                                signalHandler(0);
                        }

                        if(buf.intData > 0) { //request
                                int r = buf.intData - 1; //undo the +1 the worker added
                                total_requests++;
                                if(VERBOSE) {
                                        sprintf(line, "OSS has detected Process P%d requesting R%d at time %u:%u\n", next, r, *sec, *nano);
                                        printOut(line);
                                }
                                if(available[r] > 0) {
                                        immediate_grants++;
                                        grantResource(next, r);
                                }
                                else { //none left, it waits (we just don't message it until it can be granted)
                                        p[next].blocked = 1;
                                        p[next].requestedResource = r;
                                        if(VERBOSE) {
                                                sprintf(line, "OSS: no instances of R%d available, P%d added to wait queue at time %u:%u\n", r, next, *sec, *nano);
                                                printOut(line);
                                        }
                                }
                        }
                        else if(buf.intData < 0) { //release
                                int r = -buf.intData - 1;
                                p[next].resourcesAllocated[r]--;
                                available[r]++;
                                if(VERBOSE) {
                                        sprintf(line, "OSS has acknowledged Process P%d releasing R%d at time %u:%u\n", next, r, *sec, *nano);
                                        printOut(line);
                                }
                        }
                        else { //terminating
                                sprintf(line, "OSS: Process P%d (PID %d) terminated at time %u:%u\n", next, p[next].pid, *sec, *nano);
                                printOut(line);
                                releaseAll(next);
                                waitpid(p[next].pid, &status, 0);
                                p[next] = {}; //clear the entry so it can be reused
                                p[next].requestedResource = -1;
                                proc_running--;
                                normal_terms++;
                        }
                }

                incrementClock(sec, nano, 10000000); //10ms

                //every half second of simulated time, output the tables
                int half_sec = *sec * 2 + *nano / 500000000;
                if(half_sec != last_print) {
                        last_print = half_sec;
                        printTable();
                }

                //every simulated second, check for deadlock
                if((int)*sec != last_detect) {
                        last_detect = *sec;
                        resolveDeadlock(proc_running);
                }
        }

        sprintf(line, "OSS PID:%d Terminating\n", getpid());
        printOut(line);
        sprintf(line, "%d workers were launched\n", total);
        printOut(line);
        sprintf(line, "Final clock: %u:%u\n", *sec, *nano);
        printOut(line);
        printReport();

        shmdt(clock); // Detach from the shared memory segment
        shmctl( shm_id, IPC_RMID, NULL ); // Free shared memory segment shm_id
        msgctl(msq_id, IPC_RMID, NULL); // Free the message queue
        logfile.close();
        return 0;
}

void grantResource(int idx, int r) { //gives one instance of r to process idx
        char line[200];
        available[r]--;
        p[idx].resourcesAllocated[r]++;
        grant_count++;
        if(VERBOSE) {
                sprintf(line, "OSS granting P%d request R%d at time %u:%u\n", idx, r, *sec, *nano);
                printOut(line);
                if(grant_count % 20 == 0) //every 20 grants show who has what
                        printResources();
        }
}

void releaseAll(int idx) { //takes back everything process idx holds
        string released = "        Resources released:";
        for(int r = 0; r < NUM_RESOURCES; r++) {
                if(p[idx].resourcesAllocated[r] > 0) {
                        available[r] += p[idx].resourcesAllocated[r];
                        released += " R" + to_string(r) + ":" + to_string(p[idx].resourcesAllocated[r]);
                        p[idx].resourcesAllocated[r] = 0;
                }
        }
        released += "\n";
        printOut(released.c_str());
}

void unblockProcesses() { //grant waiting requests if the resource is free now
        char line[200];
        for(int j = 0; j < MAX_PROCS; j++) {
                if(p[j].occupied && p[j].blocked && available[p[j].requestedResource] > 0) {
                        int r = p[j].requestedResource;
                        p[j].blocked = 0;
                        p[j].requestedResource = -1;
                        if(VERBOSE) {
                                sprintf(line, "OSS: R%d freed up, unblocking P%d\n", r, j);
                                printOut(line);
                        }
                        grantResource(j, r); //no message now, its next turn is the "granted" message
                }
        }
}

int detectDeadlock(int deadlocked[]) { //returns how many processes are deadlocked, marks them in deadlocked[]
        int work[NUM_RESOURCES];
        int finish[MAX_PROCS];

        for(int r = 0; r < NUM_RESOURCES; r++)
                work[r] = available[r]; //pretend we have what's free right now

        for(int j = 0; j < MAX_PROCS; j++) {
                finish[j] = !p[j].occupied || !p[j].blocked; //empty slots and running processes aren't waiting on anything
                if(p[j].occupied && !p[j].blocked) { //a running process can finish, so it gives back what it holds
                        for(int r = 0; r < NUM_RESOURCES; r++)
                                work[r] += p[j].resourcesAllocated[r];
                }
        }

        int changed = 1;
        while(changed) { //keep passing over the table until nobody new can finish
                changed = 0;
                for(int j = 0; j < MAX_PROCS; j++) {
                        if(!finish[j] && work[p[j].requestedResource] > 0) { //its one request could be met
                                for(int r = 0; r < NUM_RESOURCES; r++)
                                        work[r] += p[j].resourcesAllocated[r]; //it finishes and gives everything back
                                finish[j] = 1;
                                changed = 1;
                        }
                }
        }

        int count = 0;
        for(int j = 0; j < MAX_PROCS; j++) {
                deadlocked[j] = !finish[j]; //whoever couldn't finish is deadlocked
                if(deadlocked[j])
                        count++;
        }
        return count;
}

void resolveDeadlock(int &proc_running) { //run detection, kill processes until there's no deadlock
        char line[200];
        int deadlocked[MAX_PROCS];
        int status;

        while(1) {
                detection_runs++;
                sprintf(line, "OSS running deadlock detection at time %u:%u\n", *sec, *nano);
                printOut(line);

                if(detectDeadlock(deadlocked) == 0) {
                        printOut("        No deadlock detected.\n");
                        return;
                }

                string list = "        Processes";
                for(int j = 0; j < MAX_PROCS; j++)
                        if(deadlocked[j])
                                list += " P" + to_string(j);
                list += " deadlocked\n";
                printOut(list.c_str());
                printOut("        Attempting to resolve deadlock...\n");

                //victim = deadlocked process holding the most resources, frees up the most for everyone else
                int victim = -1, most = -1;
                for(int j = 0; j < MAX_PROCS; j++) {
                        if(!deadlocked[j])
                                continue;
                        int held = 0;
                        for(int r = 0; r < NUM_RESOURCES; r++)
                                held += p[j].resourcesAllocated[r];
                        if(held > most) {
                                most = held;
                                victim = j;
                        }
                }

                sprintf(line, "        Killing process P%d (PID %d):\n", victim, p[victim].pid);
                printOut(line);

                //it's blocked, so it's sitting in msgrcv waiting for us; tell it to die
                struct msgbuffer buf;
                buf.mtype = p[victim].pid;
                buf.intData = MSG_DIE;
                if(msgsnd(msq_id, &buf, sizeof(buf) - sizeof(long), 0) == -1) {
                        perror("oss: msgsnd");
                        signalHandler(0);
                }
                waitpid(p[victim].pid, &status, 0);

                releaseAll(victim);
                p[victim] = {};
                p[victim].requestedResource = -1;
                proc_running--;
                deadlock_kills++;

                unblockProcesses(); //freed resources might wake someone up before we check again
        }
}

void incrementClock(unsigned int *sec, unsigned int *nano, unsigned long long nano_to_add) {
        unsigned long long total = *nano + nano_to_add; //add first so the carry comes out right
        *nano = total % 1000000000; //set nano before the second so a worker never sees the clock a second ahead
        *sec += total / 1000000000;
}

void printOut(const char *line) { //sends output to the screen and the log file
        cout << line << flush;
        if(log_lines < 10000) { //stop writing to the log after 10000 lines so a bad loop can't make a giant file
                logfile << line << flush;
                for(int k = 0; line[k] != '\0'; k++) {
                        if(line[k] == '\n')
                                log_lines++;
                }
        }
}

void printResources() { //available vector and allocation matrix
        char line[200];
        string row = "Total resources available:\n   ";
        for(int r = 0; r < NUM_RESOURCES; r++)
                row += " R" + to_string(r) + " ";
        row += "\n   ";
        for(int r = 0; r < NUM_RESOURCES; r++) {
                sprintf(line, "%3d ", available[r]);
                row += line;
        }
        row += "\nResources allocated:\n   ";
        for(int r = 0; r < NUM_RESOURCES; r++)
                row += " R" + to_string(r) + " ";
        row += "\n";
        printOut(row.c_str());

        for(int j = 0; j < MAX_PROCS; j++) {
                if(!p[j].occupied)
                        continue; //only show processes that exist
                row = "P" + to_string(j);
                while(row.size() < 3)
                        row += " ";
                for(int r = 0; r < NUM_RESOURCES; r++) {
                        sprintf(line, "%3d ", p[j].resourcesAllocated[r]);
                        row += line;
                }
                row += "\n";
                printOut(row.c_str());
        }
}

void printTable() {
        char line[200];
        sprintf(line, "OSS PID:%d SysClockS: %u SysclockNano: %u\nProcess Table:\n", getpid(), *sec, *nano);
        printOut(line);
        sprintf(line, "%-6s%-9s%-8s%-8s%-11s%-8s%s\n", "Entry", "Occupied", "PID", "StartS", "StartN", "Blocked", "Waiting On");
        printOut(line);
        for(int j = 0; j < MAX_PROCS; j++) {
                string waiting = "-";
                if(p[j].blocked)
                        waiting = "R" + to_string(p[j].requestedResource);
                sprintf(line, "%-6d%-9d%-8d%-8u%-11u%-8d%s\n", j, p[j].occupied, p[j].pid, p[j].startSeconds, p[j].startNano, p[j].blocked, waiting.c_str());
                printOut(line);
        }

        printResources();

        printOut("Processes Blocked:\n");
        int any = 0;
        for(int j = 0; j < MAX_PROCS; j++) {
                if(p[j].occupied && p[j].blocked) {
                        sprintf(line, "P%d: Waiting on R%d\n", j, p[j].requestedResource);
                        printOut(line);
                        any = 1;
                }
        }
        if(!any)
                printOut("none\n");
        printOut("\n");
}

void printReport() {
        char line[200];
        double percent = 0;
        if(total_requests > 0)
                percent = 100.0 * immediate_grants / total_requests;
        printOut("\n----- Final Report -----\n");
        sprintf(line, "Total resource requests: %d\n", total_requests);
        printOut(line);
        sprintf(line, "Requests granted immediately: %d (%.2f%%)\n", immediate_grants, percent);
        printOut(line);
        sprintf(line, "Times deadlock detection ran: %d\n", detection_runs);
        printOut(line);
        sprintf(line, "Processes killed by deadlock recovery: %d\n", deadlock_kills);
        printOut(line);
        sprintf(line, "Processes that terminated normally: %d\n", normal_terms);
        printOut(line);
}

void signalHandler(int sig) {
        cout << endl << "OSS: stopping early, killing children and freeing shared memory and the message queue" << endl;
        logfile << endl << "OSS: stopping early, killing children and freeing shared memory and the message queue" << endl;
        printReport(); //still show the stats if we ran out of time
        for(int j = 0; j < MAX_PROCS; j++) {
                if(p[j].occupied)
                        kill(p[j].pid, SIGTERM);
        }
        shmctl(shm_id, IPC_RMID, NULL);
        msgctl(msq_id, IPC_RMID, NULL);
        logfile.close();
        exit(1);
}

unsigned int *initializeMemory() {
        int shm_key = ftok("oss.c",0);
        if (shm_key == -1) {
                fprintf(stderr,"Parent:... Error in ftok\n");
                exit(1);
        }
        shm_id = shmget(shm_key,BUFF_SZ,0700|IPC_CREAT);
        if (shm_id == -1) {
                fprintf(stderr,"Parent:... Error in shmget\n");
                exit(1);
        }
        unsigned int *clock = (unsigned int *)shmat(shm_id,0,0);
        if (clock == (unsigned int *)-1) {
                fprintf(stderr,"Parent:... Error in shmat\n");
                exit(1);
        }
        return clock;
}

void initializeQueue() {
        key_t msg_key = ftok("oss.c",1); //1 instead of 0 so it's a different key than the shared memory
        if (msg_key == -1) {
                fprintf(stderr,"Parent:... Error in ftok for message queue\n");
                signalHandler(0);
        }
        msq_id = msgget(msg_key, 0644|IPC_CREAT);
        if (msq_id == -1) {
                fprintf(stderr,"Parent:... Error in msgget\n");
                signalHandler(0);
        }
}

int parse(int argc, char *argv[], int &proc, int &simul, float &time_limit, float &interval, string &log_name) {
        int opt;
        proc = 1;
        simul = 1;
        time_limit = 1;
        interval = 1;
        log_name = "log.txt";
        while ((opt = getopt(argc, argv, "hn:s:t:i:f:")) != -1) {
                switch (opt) {
                        case 'h':
                                cout << "oss [-h] [-n proc] [-s simul] [-t timelimitForChildren] [-i fractionOfSecondToLaunchChildren] [-f logfile]" << endl
                                << "The proc parameter stands for number of total children to launch," << endl
                                << "the simul parameter indicates how many children to allow to run simultaneously." << endl
                                << "timelimitForChildren is the most simulated time (seconds) a child stays in the system; can be a float" << endl
                                << "fractionOfSecondToLaunchChildren is the minimum time (seconds) between launching children; can be a float or 0" << endl
                                << "logfile is where oss output gets saved (also printed to the screen)" << endl
                                << "./oss -n 3 -s 2 -t 4 -i 0.6 -f log.txt" << endl;
                                exit(0);
                        case 'n':
                                proc = atoi(optarg);
                                break;
                        case 's':
                                simul = atoi(optarg);
                                if (simul > 18) {
                                        cout << "-s can't be more than 18" << endl;
                                        exit(1);
                                }
                                else if (simul < 1) {
                                        cout << "-s can't be less than 1" << endl;
                                        exit(1);
                                }
                                break;
                        case 't':
                                time_limit = atof(optarg);
                                if (time_limit <= 0) {
                                        cout << "-t has to be more than 0" << endl;
                                        exit(1);
                                }
                                break;
                        case 'i':
                                interval = atof(optarg);
                                break;
                        case 'f':
                                log_name = optarg;
                                break;
                        default:
                                cout << "Use ./oss -h to see how to run oss" << endl;
                                exit(1);
                }
        }

        return 0;
}
