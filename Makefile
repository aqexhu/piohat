# Makefile for piohat project

CXX = g++
CXXFLAGS = -Wall -pthread -lgpiod -lmosquitto

TARGET = piohat
SOURCES = piohat.cpp

$(TARGET): $(SOURCES)
	$(CXX) $(SOURCES) -o $(TARGET) $(CXXFLAGS)

clean:
	rm -f $(TARGET)
