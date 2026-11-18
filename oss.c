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
#include<deque>
#include "resource.h"
using namespace std;

struct PCB {
        int occupied; // either true or false
        pid_t pid; // process id of this child
        unsigned int startSeconds; // time when it was forked
        unsigned int startNano; // time when it was forked
        unsigned int endingTimeSeconds; // estimated time it should end
        unsigned int endingTimeNano; // estimated time it should end
        unsigned int messagesSent; // total times oss sent a message to it
        unsigned int serviceTimeSeconds; // total seconds it has been "scheduled"
        unsigned int serviceTimeNano; // total nanoseconds it has been "scheduled"
        unsigned int eventWaitSec; // when it gets unblocked
        unsigned int eventWaitNano; // when it gets unblocked
        int blocked; // 1 if it's waiting on i/o
};

const int QUANTUM[3] = {25000000, 50000000, 100000000}; //q0 gets 25ms, q1 50ms, q2 100ms
int shm_key;
int shm_id;
int msq_id;
struct PCB p[20];
ofstream logfile;
int log_lines = 0; //lines written to the log so far
unsigned long long child_time = 0; //time the workers spent running
unsigned long long oss_time = 0; //time oss spent on overhead or sitting idle

int parse(int argc, char *argv[], int &proc, int &simul, float &time_limit, float &interval, string &log_name);

void incrementClock(unsigned int *sec, unsigned int *nano, unsigned long long nano_to_add);

unsigned int *initializeMemory();

void initializeQueue();

void signalHandler(int sig);

void printOut(const char *line);

void printTable(unsigned int sec, unsigned int nano);

void printReport();

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
        unsigned int *sec = &(clock[0]);
        unsigned int *nano = &(clock[1]);
        *sec = *nano = 0;

        initializeQueue();
        srand(time(NULL)); //so each run gives the workers different random times

        signal(SIGINT, signalHandler); //ctrl-c
        signal(SIGALRM, signalHandler); //3 real life seconds
        alarm(3);

        sprintf(line, "OSS starting, PID:%d PPID:%d\n", getpid(), getppid());
        printOut(line);
        sprintf(line, "Called with: -n %d -s %d -t %g -i %g -f %s\n", proc, simul, time_limit, interval, log_name.c_str());
        printOut(line);

        //split -t into the most seconds and nanoseconds a worker's burst can be
        int max_sec = time_limit;
        int max_nano = (time_limit - max_sec) * 1000000000;

        int i = 0;
        int last_print = -1; //last half second the table was printed
        int launch_sec = 0, launch_nano = 0; //time the last child was launched
        int total_messages = 0; //total messages oss sent
        struct msgbuffer buf;
        deque<int> q[3]; //the 3 ready queues, they hold process table entries not pids

        while(total < proc || proc_running > 0) { //still children to launch OR children still in the system
                //every half second of simulated time, output the process table
                int half_sec = *sec * 2 + *nano / 500000000; //how many half seconds have passed
                if(half_sec != last_print) {
                        last_print = half_sec;
                        printTable(*sec, *nano);
                }

                //possibly launch a new child: more to launch, under the -s limit, and -i time has passed since the last launch
                double since_launch = ((double)*sec - launch_sec) + ((double)*nano - launch_nano) / 1000000000.0; //doubles so the nano part can go negative without wrapping around
                if(total < proc && proc_running < simul && (total == 0 || since_launch >= interval)) {
                        for(i = 0; i < 20; i++) { //find the first free spot in the process table
                                if(!p[i].occupied)
                                        break;
                        }

                        //pick a random cpu burst between 0 and -t for this worker
                        int run_sec = rand() % (max_sec + 1);
                        int run_nano;
                        if(run_sec == max_sec)
                                run_nano = rand() % (max_nano + 1); //can't go over the nanoseconds of -t
                        else
                                run_nano = rand() % 1000000000;
                        if(run_sec == 0 && run_nano == 0)
                                run_nano = 1; //a burst of 0 would come back as -0 which just looks like 0

                        p[i].startSeconds = *sec; //time right before the fork
                        p[i].startNano = *nano;
                        pid_t pid = fork(); //creates copy of process that runs at the same time as eachother
                        if (pid == -1) {
                                cout << "Failed to launch child" << endl;
                                signalHandler(0); //kill children, free shared memory and the queue, and exit
                        }
                        else if (pid == 0) { //seperates child copy from parent copy
                                execlp("./worker","./worker",to_string(run_sec).c_str(), to_string(run_nano).c_str(), (char*) NULL); //calls child and ends
                                cout << "exec of ./worker failed" << endl; //only gets here if exec fails
                                exit(1);
                        }
                        //parent fills in the rest of the process table
                        p[i].occupied = 1;
                        p[i].pid = pid;
                        proc_running++; //parent keeps count of processes running and total
                        total++;

                        q[0].push_back(i); //new processes start in the highest priority queue
                        incrementClock(sec, nano, 10000); //setting up a process takes oss a little time
                        oss_time += 10000;
                        sprintf(line, "OSS: Generating process with PID %d and putting it in q0 queue at time %u:%u\n", pid, *sec, *nano);
                        printOut(line);

                        launch_sec = *sec;
                        launch_nano = *nano;
                }

                //wake up anybody whose 200ms of being blocked is over
                for(int j = 0; j < 20; j++) {
                        if(p[j].occupied && p[j].blocked && (*sec > p[j].eventWaitSec || (*sec == p[j].eventWaitSec && *nano >= p[j].eventWaitNano))) {
                                p[j].blocked = 0;
                                q[0].push_back(j); //unblocked processes go to the back of q0
                                incrementClock(sec, nano, 20000); //moving it out of blocked costs more than a normal dispatch
                                oss_time += 20000;
                                sprintf(line, "OSS: Process with PID %d is unblocked, putting it in q0 at time %u:%u\n", p[j].pid, *sec, *nano);
                                printOut(line);
                        }
                }

                //pick the highest queue that has somebody in it
                int cur_q = -1;
                for(int k = 0; k < 3; k++) {
                        if(!q[k].empty()) {
                                cur_q = k;
                                break;
                        }
                }

                if(cur_q != -1) {
                        //show the queues before every scheduling decision, like q0[3,5]q1[1,2]q2[],blocked[7]
                        string q_line = "OSS: ";
                        for(int k = 0; k < 3; k++) {
                                q_line += "q" + to_string(k) + "[";
                                for(int m = 0; m < (int)q[k].size(); m++) {
                                        if(m > 0)
                                                q_line += ",";
                                        q_line += to_string(p[q[k][m]].pid);
                                }
                                q_line += "]";
                        }
                        q_line += ",blocked[";
                        int listed = 0;
                        for(int j = 0; j < 20; j++) {
                                if(p[j].occupied && p[j].blocked) {
                                        if(listed > 0)
                                                q_line += ",";
                                        q_line += to_string(p[j].pid);
                                        listed++;
                                }
                        }
                        q_line += "]\n";
                        printOut(q_line.c_str());

                        int next = q[cur_q].front(); //front of the highest queue goes next
                        q[cur_q].pop_front();

                        int dispatch_ns = rand() % 9901 + 100; //making the scheduling decision takes oss 100 to 10000 ns
                        incrementClock(sec, nano, dispatch_ns);
                        oss_time += dispatch_ns;
                        sprintf(line, "OSS: Dispatching process with PID %d from q%d at time %u:%u\n", p[next].pid, cur_q, *sec, *nano);
                        printOut(line);
                        sprintf(line, "OSS: total time this dispatch was %d nanoseconds\n", dispatch_ns);
                        printOut(line);

                        buf.mtype = p[next].pid; //only that child picks this message up
                        buf.intData = QUANTUM[cur_q]; //how long it's allowed to run
                        if(msgsnd(msq_id, &buf, sizeof(buf) - sizeof(long), 0) == -1) {
                                perror("oss: msgsnd");
                                signalHandler(0);
                        }
                        total_messages++;

                        //wait for the child to answer (workers send their message to oss's pid)
                        if(msgrcv(msq_id, &buf, sizeof(buf) - sizeof(long), getpid(), 0) == -1) {
                                perror("oss: msgrcv");
                                signalHandler(0);
                        }

                        int ran_for = buf.intData;
                        if(ran_for < 0)
                                ran_for = -ran_for; //negative just means it's terminating, the time it ran is the same
                        incrementClock(sec, nano, ran_for);
                        child_time += ran_for;
                        incrementClock(&p[next].serviceTimeSeconds, &p[next].serviceTimeNano, ran_for); //same carry math works for its service time
                        sprintf(line, "OSS: Receiving that process with PID %d ran for %d nanoseconds\n", p[next].pid, ran_for);
                        printOut(line);

                        if(buf.intData < 0) { //child said it is done
                                sprintf(line, "OSS: Process with PID %d is terminating\n", p[next].pid);
                                printOut(line);
                                waitpid(p[next].pid, &status, 0); //give it time to clear out of the system
                                p[next] = {}; //clear the entry (all 0) so it can be reused
                                proc_running--;
                        }
                        else if(buf.intData == QUANTUM[cur_q]) { //used its whole quantum so it drops a queue (q2 just stays in q2)
                                int new_q = cur_q;
                                if(new_q < 2)
                                        new_q++;
                                q[new_q].push_back(next);
                                sprintf(line, "OSS: Putting process with PID %d into queue %d\n", p[next].pid, new_q);
                                printOut(line);
                        }
                        else { //only used part of it, so it got interrupted by i/o
                                p[next].blocked = 1;
                                p[next].eventWaitSec = *sec;
                                p[next].eventWaitNano = *nano;
                                incrementClock(&p[next].eventWaitSec, &p[next].eventWaitNano, 200000000); //blocked for 200ms from now
                                printOut("OSS: not using its entire time quantum\n");
                                sprintf(line, "OSS: Putting process with PID %d into blocked queue\n", p[next].pid);
                                printOut(line);
                        }
                }
                else {
                        //nobody is ready (everyone's blocked or we're waiting on -i), so let some time pass
                        incrementClock(sec, nano, 1000000);
                        oss_time += 1000000;
                }
        }

        sprintf(line, "OSS PID:%d Terminating\n", getpid());
        printOut(line);
        sprintf(line, "%d workers were launched and terminated\n", total);
        printOut(line);
        sprintf(line, "OSS sent a total of %d messages\n", total_messages);
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

void printTable(unsigned int sec, unsigned int nano) {
        char line[200];
        sprintf(line, "OSS PID:%d SysClockS: %u SysclockNano: %u\nProcess Table:\n", getpid(), sec, nano);
        printOut(line);
        sprintf(line, "%-6s%-9s%-8s%-8s%-11s%-10s%-11s%-8s%-11s%s\n", "Entry", "Occupied", "PID", "StartS", "StartN", "ServiceS", "ServiceN", "EventS", "EventN", "Blocked");
        printOut(line);
        for(int j = 0; j < 20; j++) {
                sprintf(line, "%-6d%-9d%-8d%-8u%-11u%-10u%-11u%-8u%-11u%d\n", j, p[j].occupied, p[j].pid, p[j].startSeconds, p[j].startNano, p[j].serviceTimeSeconds, p[j].serviceTimeNano, p[j].eventWaitSec, p[j].eventWaitNano, p[j].blocked);
                printOut(line);
        }
        string blocked_line = "Blocked processes:";
        for(int j = 0; j < 20; j++) {
                if(p[j].occupied && p[j].blocked)
                        blocked_line += " " + to_string(p[j].pid);
        }
        blocked_line += "\n\n";
        printOut(blocked_line.c_str());
}

void printReport() { //cpu utilization = time the workers actually ran / all the time that passed
        char line[200];
        unsigned long long all_time = child_time + oss_time; //this should match the clock
        double percent = 0;
        if(all_time > 0)
                percent = 100.0 * child_time / all_time;
        sprintf(line, "Time workers spent running: %llu ns\n", child_time);
        printOut(line);
        sprintf(line, "Time oss spent on overhead or idle: %llu ns\n", oss_time);
        printOut(line);
        sprintf(line, "CPU utilization: %.2f%%\n", percent);
        printOut(line);
}

void signalHandler(int sig) {
        cout << endl << "OSS: stopping early, killing children and freeing shared memory and the message queue" << endl;
        logfile << endl << "OSS: stopping early, killing children and freeing shared memory and the message queue" << endl;
        printReport(); //still show the stats if we ran out of time
        for(int j = 0; j < 20; j++) {
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
        if (shm_id == -1) { //shmget returns -1 on error (0 is a real id)
                fprintf(stderr,"Parent:... Error in shmget\n");
                exit(1);
        }
        unsigned int *clock = (unsigned int *)shmat(shm_id,0,0);
        if (clock == (unsigned int *)-1) { //shmat returns -1 on error, not 0
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

int parse(int argc, char *argv[], int &proc, int &simul, float &time_limit, float &interval, string &log_name) { //uses arguments as reference to have default arguments
        int opt;
        proc = 1; //processes
        simul = 1; //simulations
        time_limit = 1; //most cpu time a child can need
        interval = 1;   //interval 1 in seconds to launch children
        log_name = "log.txt"; //log file
        while ((opt = getopt(argc, argv, "hn:s:t:i:f:")) != -1) { //goes through each paramater h,n,s,t,i,f and gets the value
                switch (opt) { //each parameter goes in and gets chosen by its equivalent case
                        case 'h':
                                cout << "oss [-h] [-n proc] [-s simul] [-t timelimitForChildren] [-i fractionOfSecondToLaunchChildren] [-f logfile]" << endl
                                << "The proc parameter stands for number of total children to launch," << endl
                                << "the simul parameter indicates how many children to allow to run simultaneously." << endl
                                << "timelimitForChildren is the most cpu time (seconds) a child needs, it gets a random burst from 0 to this; can be a float" << endl
                                << "fractionOfSecondToLaunchChildren is the minimum time (seconds) between launching children; can be a float or 0 (e.g. 0.1 = 100 ms)" << endl
                                << "logfile is where oss output gets saved (also printed to the screen)" << endl
                                << "./oss -n 3 -s 2 -t 4 -i 0.6 -f log.txt" << endl;
                                exit(0);
                        case 'n':
                                proc = atoi(optarg);
                                break;
                        case 's':
                                simul = atoi(optarg);
                                if (simul > 18) { //the spec says space for up to 18 process control blocks
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
                                if (time_limit <= 0) { //bursts go from 0 up to -t so it just has to be positive
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
                        default: //if there is an unexpected option (optarg is NULL here, so don't use it)
                                cout << "Use ./oss -h to see how to run oss" << endl;
                                exit(1);
                }
        }

        return 0;
}
