.PHONY: clean

CFLAGS  := -Wall -g
LD      := gcc
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread -lm

APPS    := multibcast

all: ${APPS}

node: multibcast.o
	${LD} -o $@ $^ ${LDLIBS}

clean:
	rm -f *.o ${APPS}
