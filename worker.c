#include<unistd.h>
#include<sys/types.h>
#include<stdio.h>
#include<stdlib.h>
#include<iostream>
#include<sys/ipc.h>
#include<sys/shm.h>
#include<sys/msg.h>
#include "resource.h"
using namespace std;

unsigned int *initializeMemory();

int initializeQueue();

int main(int argc, char** argv) {
        if(argc < 3) {
                cout << "Usage: ./worker seconds nanoseconds" << endl;
                return EXIT_FAILURE;
        }

        unsigned int *clock = initializeMemory();
        unsigned int *sec = &(clock[0]);
        unsigned int *nano = &(clock[1]);
        int msq_id = initializeQueue();

        srand(getpid()); //seed off the pid so every worker gets different random numbers

        //figure out when our time in the system is up: now + the lifetime oss gave us
        unsigned int end_sec = *sec + atoi(argv[1]);
        unsigned long long end_total = (unsigned long long)*nano + atoi(argv[2]);
        end_sec += end_total / 1000000000; //carry extra nanoseconds into seconds
        unsigned int end_nano = end_total % 1000000000;

        int my_alloc[NUM_RESOURCES] = {0}; //how many of each resource we hold
        int pending = -1; //resource we asked for and haven't gotten yet (-1 = none)

        cout << "WORKER PID:" << getpid() << " PPID:" << getppid() << " SysClockS: " << *sec << " SysclockNano: " << *nano
             << " TermTimeS: " << end_sec << " TermTimeNano: " << end_nano << endl
             << "--Just Starting" << endl;

        struct msgbuffer buf;
        while(1) {
                //wait for oss to give us a turn (mtype is our pid)
                if(msgrcv(msq_id, &buf, sizeof(buf) - sizeof(long), getpid(), 0) == -1) {
                        perror("worker: msgrcv");
                        exit(1);
                }

                if(buf.intData == MSG_DIE) { //oss picked us to break a deadlock, it already took our resources
                        cout << "WORKER PID:" << getpid() << " --Killed by oss to resolve deadlock" << endl;
                        break;
                }

                //any message from oss means our last request went through
                if(pending != -1) {
                        my_alloc[pending]++;
                        pending = -1;
                }

                buf.mtype = getppid(); //everything we send goes to oss

                //is our time up?
                if(*sec > end_sec || (*sec == end_sec && *nano >= end_nano)) {
                        buf.intData = MSG_TERMINATE;
                        if(msgsnd(msq_id, &buf, sizeof(buf) - sizeof(long), 0) == -1) {
                                perror("worker: msgsnd");
                                exit(1);
                        }
                        cout << "WORKER PID:" << getpid() << " SysClockS: " << *sec << " SysclockNano: " << *nano
                             << " --Time is up, terminating" << endl;
                        break;
                }

                //count what we hold so we know if a request or release is even possible
                int held = 0;
                for(int r = 0; r < NUM_RESOURCES; r++)
                        held += my_alloc[r];

                int request = (rand() % 100 < REQUEST_CHANCE); //70% request, 30% release
                if(held == 0)
                        request = 1; //nothing to release
                if(held == NUM_RESOURCES * INSTANCES)
                        request = 0; //we already have everything, can't ask for more

                //keep picking a resource until we get one that makes sense
                int r;
                do {
                        r = rand() % NUM_RESOURCES;
                } while((request && my_alloc[r] == INSTANCES) || (!request && my_alloc[r] == 0));

                if(request) {
                        buf.intData = r + 1; //positive = request
                        pending = r; //don't count it until oss answers
                }
                else {
                        buf.intData = -(r + 1); //negative = release
                        my_alloc[r]--; //releases always succeed, count it now
                }

                if(msgsnd(msq_id, &buf, sizeof(buf) - sizeof(long), 0) == -1) {
                        perror("worker: msgsnd");
                        exit(1);
                }
        }

        shmdt(clock); //detach only, oss frees the memory and the queue
        return EXIT_SUCCESS;
}

unsigned int *initializeMemory() {
        int shm_key = ftok("oss.c",0);
        if (shm_key == -1) {
                fprintf(stderr,"Child:... Error in ftok\n");
                exit(1);
        }
        int shm_id = shmget(shm_key,BUFF_SZ,0700);
        if (shm_id == -1) {
                fprintf(stderr,"child:... Error in shmget\n");
                exit(1);
        }
        unsigned int *clock = (unsigned int *)shmat(shm_id,0,0);
        if (clock == (unsigned int *)-1) { //shmat returns -1 on error, not 0
                fprintf(stderr,"Child:... Error in shmat\n");
                exit(1);
        }
        return clock;
}

int initializeQueue() {
        key_t msg_key = ftok("oss.c",1); //same key oss used
        if (msg_key == -1) {
                fprintf(stderr,"Child:... Error in ftok for message queue\n");
                exit(1);
        }
        int msq_id = msgget(msg_key, 0644); //no IPC_CREAT, oss already made it
        if (msq_id == -1) {
                fprintf(stderr,"Child:... Error in msgget\n");
                exit(1);
        }
        return msq_id;
}
