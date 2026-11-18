#ifndef RESOURCES_H
#define RESOURCES_H

//shared between oss and user_proc so both sides agree on the system
#define NUM_RESOURCES 5   //R0 to R4
#define INSTANCES 5       //each resource has 5 instances
#define MAX_PROCS 20      //size of the process table
#define REQUEST_CHANCE 70 //percent of the time a worker requests instead of releases
#define VERBOSE 1         //1 = log every request/grant/release, 0 = only tables and deadlocks

//oss to worker
#define MSG_GO 1          //th turn (also means your last request was granted)
#define MSG_DIE 0         //oss is killing you to break a deadlock

//worker to oss
//  r+1 = request one instance of resource r
//  -(r+1) = release one instance of resource r
//  0 = terminating
#define MSG_TERMINATE 0

struct msgbuffer {
        long mtype;
        int intData;
};

const int BUFF_SZ = sizeof(unsigned int)*2;

#endif
