
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall -g

all: correctness persistence

correctness: level.o SSTable.o SkipList.o kvstore.o correctness.o

persistence: level.o SSTable.o SkipList.o kvstore.o persistence.o

clean:
	-rm -f correctness persistence *.o
