
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall -g

all: correctness persistence

correctness: SSTable.o SkipList.o kvstore.o correctness.o

persistence: SSTable.o SkipList.o kvstore.o persistence.o

clean:
	-rm -f correctness persistence *.o
