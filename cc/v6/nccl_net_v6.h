#ifndef NCCL_NET_V6_H_
#define NCCL_NET_V6_H_

// Base types (ncclResult_t, ncclDebugLogger_t, NCCL_PTR_*) come from the v5 header.
#include "nccl_net_v5.h"

// Structs copied verbatim from NCCL master src/include/plugin/net/net_v6.h
// (ABI must match the loader exactly).
typedef struct {
  char* name;     // Used mostly for logging.
  char* pciPath;  // Path to the PCI device in /sys.
  uint64_t guid;  // Unique identifier for the NIC chip. Important for
                  // cards with multiple PCI functions (Physical or virtual).
  int ptrSupport; // [NCCL_PTR_HOST|NCCL_PTR_CUDA|NCCL_PTR_DMABUF]
  int speed;      // Port speed in Mbps.
  int port;       // Port number.
  float latency;  // Network latency
  int maxComms;   // Maximum number of comms we can create
  int maxRecvs;   // Maximum number of grouped receives.
} ncclNetProperties_v6_t;

typedef struct {
  const char* name;
  ncclResult_t (*init)(ncclDebugLogger_t logFunction);
  ncclResult_t (*devices)(int* ndev);
  ncclResult_t (*getProperties)(int dev, ncclNetProperties_v6_t* props);
  ncclResult_t (*listen)(int dev, void* handle, void** listenComm);
  ncclResult_t (*connect)(int dev, void* handle, void** sendComm);
  ncclResult_t (*accept)(void* listenComm, void** recvComm);
  ncclResult_t (*regMr)(void* comm, void* data, int size, int type, void** mhandle);
  ncclResult_t (*regMrDmaBuf)(void* comm, void* data, size_t size, int type, uint64_t offset, int fd, void** mhandle);
  ncclResult_t (*deregMr)(void* comm, void* mhandle);
  ncclResult_t (*isend)(void* sendComm, void* data, int size, int tag, void* mhandle, void** request);
  ncclResult_t (*irecv)(void* recvComm, int n, void** data, int* sizes, int* tags, void** mhandles, void** request);
  ncclResult_t (*iflush)(void* recvComm, int n, void** data, int* sizes, void** mhandles, void** request);
  ncclResult_t (*test)(void* request, int* done, int* sizes);
  ncclResult_t (*closeSend)(void* sendComm);
  ncclResult_t (*closeRecv)(void* recvComm);
  ncclResult_t (*closeListen)(void* listenComm);
} ncclNet_v6_t;

#endif
