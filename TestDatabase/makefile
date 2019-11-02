CC = g++
CFLAGS = -c -Wall -O2 -pthread -std=c++11
objdir = ./objs/
def64IO = -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -D_LARGEFILE_SOURCE

utilobj = $(objdir)Util.o $(objdir)Bstr.o $(objdir)Triple.o $(objdir)VList.o 
parserobj = $(objdir)RDFParser.o $(objdir)TurtleParser.o
sitreeobj = $(objdir)SITree.o $(objdir)SIStorage.o $(objdir)SINode.o $(objdir)SIIntlNode.o $(objdir)SILeafNode.o $(objdir)SIHeap.o 
ivtreeobj = $(objdir)IVTree.o $(objdir)IVStorage.o $(objdir)IVNode.o $(objdir)IVIntlNode.o $(objdir)IVLeafNode.o $(objdir)IVHeap.o 
ivarrayobj = $(objdir)IVArray.o $(objdir)IVEntry.o $(objdir)IVBlockManager.o
isarrayobj = $(objdir)ISArray.o $(objdir)ISEntry.o $(objdir)ISBlockManager.o
kvstoreobj = $(objdir)KVstore.o $(sitreeobj) $(istreeobj) $(ivtreeobj) $(ivarrayobj) $(isarrayobj) #$(sstreeobj)
stringindexobj = $(objdir)StringIndex.o
databaseobj = $(objdir)Database.o

all: $(utilobj) $(parserobj) $(sitreeobj) $(ivtreeobj) $(ivarrayobj) $(isarrayobj) $(kvstoreobj) $(stringindexobj) $(databaseobj)
	@echo "Compilation ends successfully!"

$(objdir)Util.o:  Util/Util.cpp Util/Util.h
	$(CC) $(CFLAGS) Util/Util.cpp -o $(objdir)Util.o

$(objdir)Stream.o:  Util/Stream.cpp Util/Stream.h $(objdir)Util.o $(objdir)Bstr.o
	$(CC) $(CFLAGS) Util/Stream.cpp -o $(objdir)Stream.o $(def64IO)

$(objdir)Bstr.o: Util/Bstr.cpp Util/Bstr.h $(objdir)Util.o
	$(CC) $(CFLAGS)  Util/Bstr.cpp -o $(objdir)Bstr.o

$(objdir)Triple.o: Util/Triple.cpp Util/Triple.h $(objdir)Util.o
	$(CC) $(CFLAGS) Util/Triple.cpp -o $(objdir)Triple.o

$(objdir)VList.o:  Util/VList.cpp Util/VList.h Util/ClassForVlistCache.h
	$(CC) $(CFLAGS) Util/VList.cpp -o $(objdir)VList.o

$(objdir)TurtleParser.o: Parser/TurtleParser.cpp Parser/TurtleParser.h Parser/Type.h
	gcc  $(CFLAGS) Parser/TurtleParser.cpp -o $(objdir)TurtleParser.o

$(objdir)RDFParser.o: Parser/RDFParser.cpp Parser/RDFParser.h $(objdir)TurtleParser.o $(objdir)Triple.o
	gcc  $(CFLAGS) Parser/RDFParser.cpp -o $(objdir)RDFParser.o

$(objdir)TrieNode.o: Trie/TrieNode.cpp Trie/TrieNode.h
	$(CC) $(CFLAGS) Trie/TrieNode.cpp -o $(objdir)TrieNode.o

$(objdir)Trie.o: Trie/Trie.cpp Trie/Trie.h $(objdir)TrieNode.o $(objdir)Triple.o
	$(CC) $(CFLAGS) Trie/Trie.cpp $(inc) -o $(objdir)Trie.o

$(objdir)SITree.o: KVstore/SITree/SITree.cpp KVstore/SITree/SITree.h
	$(CC) $(CFLAGS) KVstore/SITree/SITree.cpp -o $(objdir)SITree.o

$(objdir)SIStorage.o: KVstore/SITree/storage/SIStorage.cpp KVstore/SITree/storage/SIStorage.h $(objdir)Util.o
	$(CC) $(CFLAGS) KVstore/SITree/storage/SIStorage.cpp -o $(objdir)SIStorage.o $(def64IO)

$(objdir)SINode.o: KVstore/SITree/node/SINode.cpp KVstore/SITree/node/SINode.h $(objdir)Util.o
	$(CC) $(CFLAGS) KVstore/SITree/node/SINode.cpp -o $(objdir)SINode.o

$(objdir)SIIntlNode.o: KVstore/SITree/node/SIIntlNode.cpp KVstore/SITree/node/SIIntlNode.h
	$(CC) $(CFLAGS) KVstore/SITree/node/SIIntlNode.cpp -o $(objdir)SIIntlNode.o

$(objdir)SILeafNode.o: KVstore/SITree/node/SILeafNode.cpp KVstore/SITree/node/SILeafNode.h
	$(CC) $(CFLAGS) KVstore/SITree/node/SILeafNode.cpp -o $(objdir)SILeafNode.o

$(objdir)SIHeap.o: KVstore/SITree/heap/SIHeap.cpp KVstore/SITree/heap/SIHeap.h $(objdir)Util.o
	$(CC) $(CFLAGS) KVstore/SITree/heap/SIHeap.cpp -o $(objdir)SIHeap.o

$(objdir)ISArray.o: KVstore/ISArray/ISArray.cpp KVstore/ISArray/ISArray.h $(objdir)VList.o
	$(CC) $(CFLAGS) KVstore/ISArray/ISArray.cpp -o $(objdir)ISArray.o

$(objdir)ISBlockManager.o: KVstore/ISArray/ISBlockManager.cpp KVstore/ISArray/ISBlockManager.h 
	$(CC) $(CFLAGS) KVstore/ISArray/ISBlockManager.cpp -o $(objdir)ISBlockManager.o

$(objdir)ISEntry.o: KVstore/ISArray/ISEntry.cpp KVstore/ISArray/ISEntry.h
	$(CC) $(CFLAGS) KVstore/ISArray/ISEntry.cpp -o $(objdir)ISEntry.o

$(objdir)IVTree.o: KVstore/IVTree/IVTree.cpp KVstore/IVTree/IVTree.h $(objdir)Stream.o $(objdir)VList.o
	$(CC) $(CFLAGS) KVstore/IVTree/IVTree.cpp -o $(objdir)IVTree.o

$(objdir)IVStorage.o: KVstore/IVTree/storage/IVStorage.cpp KVstore/IVTree/storage/IVStorage.h $(objdir)Util.o
	$(CC) $(CFLAGS) KVstore/IVTree/storage/IVStorage.cpp -o $(objdir)IVStorage.o $(def64IO)

$(objdir)IVNode.o: KVstore/IVTree/node/IVNode.cpp KVstore/IVTree/node/IVNode.h $(objdir)Util.o
	$(CC) $(CFLAGS) KVstore/IVTree/node/IVNode.cpp -o $(objdir)IVNode.o

$(objdir)IVIntlNode.o: KVstore/IVTree/node/IVIntlNode.cpp KVstore/IVTree/node/IVIntlNode.h
	$(CC) $(CFLAGS) KVstore/IVTree/node/IVIntlNode.cpp -o $(objdir)IVIntlNode.o

$(objdir)IVLeafNode.o: KVstore/IVTree/node/IVLeafNode.cpp KVstore/IVTree/node/IVLeafNode.h
	$(CC) $(CFLAGS) KVstore/IVTree/node/IVLeafNode.cpp -o $(objdir)IVLeafNode.o

$(objdir)IVHeap.o: KVstore/IVTree/heap/IVHeap.cpp KVstore/IVTree/heap/IVHeap.h $(objdir)Util.o
	$(CC) $(CFLAGS) KVstore/IVTree/heap/IVHeap.cpp -o $(objdir)IVHeap.o

$(objdir)IVArray.o: KVstore/IVArray/IVArray.cpp KVstore/IVArray/IVArray.h $(objdir)VList.o 
	$(CC) $(CFLAGS) KVstore/IVArray/IVArray.cpp -o $(objdir)IVArray.o

$(objdir)IVBlockManager.o: KVstore/IVArray/IVBlockManager.cpp KVstore/IVArray/IVBlockManager.h 
	$(CC) $(CFLAGS) KVstore/IVArray/IVBlockManager.cpp -o $(objdir)IVBlockManager.o

$(objdir)IVEntry.o: KVstore/IVArray/IVEntry.cpp KVstore/IVArray/IVEntry.h
	$(CC) $(CFLAGS) KVstore/IVArray/IVEntry.cpp -o $(objdir)IVEntry.o

$(objdir)KVstore.o: KVstore/KVstore.cpp KVstore/KVstore.h KVstore/Tree.h 
	$(CC) $(CFLAGS) KVstore/KVstore.cpp -o $(objdir)KVstore.o

$(objdir)StringIndex.o: StringIndex/StringIndex.cpp StringIndex/StringIndex.h $(objdir)KVstore.o $(objdir)Util.o
	$(CC) $(CFLAGS) StringIndex/StringIndex.cpp -o $(objdir)StringIndex.o $(def64IO)

$(objdir)Database.o: Database/Database.cpp Database/Database.h $(objdir)Triple.o \
	$(objdir)KVstore.o $(objdir)Util.o $(objdir)RDFParser.o $(objdir)StringIndex.o
	$(CC) $(CFLAGS) Database/Database.cpp $(inc) -o $(objdir)Database.o

clean:
	rm -f $(objdir)*.o