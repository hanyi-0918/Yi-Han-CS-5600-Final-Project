
CC = gcc

CFLAGS = -g -Wall


all: recovery_sim

recovery_sim: main.c
	$(CC) $(CFLAGS) -o recovery_sim main.c


clean:
	rm -f recovery_sim checkpoint.dat