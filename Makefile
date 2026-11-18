all: oss worker

oss: oss.c resource.h
	g++ -Wall -g -o oss oss.c

worker: worker.c resource.h
	g++ -Wall -g -o worker worker.c

clean:
	rm -f oss worker

