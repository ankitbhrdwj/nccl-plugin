#include <netinet/in.h>
#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

#include "nccl_net_v6.h"
#include "bagua_net.h"

#define __hidden __attribute__((visibility("hidden")))

// Defined in v5/nccl_net_v5.cc; both inits set it.
extern ncclDebugLogger_t NCCL_DEBUG_LOG;
#define NCCL_TRACE(FLAGS, ...) NCCL_DEBUG_LOG(NCCL_LOG_TRACE, (FLAGS), __func__, __LINE__, __VA_ARGS__)
#define NCCL_INFO(FLAGS, ...) NCCL_DEBUG_LOG(NCCL_LOG_INFO, (FLAGS), __func__, __LINE__, __VA_ARGS__)
#define NCCL_WARN(...) NCCL_DEBUG_LOG(NCCL_LOG_WARN, NCCL_ALL, __FILE__, __LINE__, __VA_ARGS__)

__hidden ncclResult_t baguaNetInit_v6(ncclDebugLogger_t logFunction)
{
    NCCL_DEBUG_LOG = logFunction;
    BaguaNet::instance();
    NCCL_TRACE(NCCL_ALL, "baguaNetInit_v6!");
    return ncclSuccess;
}

__hidden ncclResult_t baguaNetDevices_v6(int *ndev)
{
    if (BaguaNet::instance().devices((int32_t *)ndev) != 0)
    {
        NCCL_WARN("baguaNetDevices_v6 failed, ndev=%d", *ndev);
        return ncclInternalError;
    }
    return ncclSuccess;
}

__hidden ncclResult_t baguaNetGetProperties_v6(int dev, ncclNetProperties_v6_t *props)
{
    NCCLNetPropertiesC inner_props;
    int ret = BaguaNet::instance().get_properties(dev, &inner_props);
    if (ret != 0)
    {
        NCCL_WARN("baguaNetGetProperties_v6 failed, ret=%d, dev=%d", ret, dev);
        return ncclInternalError;
    }
    props->name = const_cast<char *>(inner_props.name);
    props->pciPath = const_cast<char *>(inner_props.pci_path);
    props->guid = inner_props.guid;
    props->ptrSupport = inner_props.ptr_support;
    props->speed = inner_props.speed;
    props->port = inner_props.port;
    props->latency = 0;
    props->maxComms = inner_props.max_comms;
    // The BaguaNet wrappers service one buffer per irecv (no grouped receives).
    props->maxRecvs = 1;
    return ncclSuccess;
}

__hidden ncclResult_t baguaNetListen_v6(int dev, void *handle, void **listenComm)
{
    if (BaguaNet::instance().listen(dev, handle, listenComm) != 0)
    {
        NCCL_WARN("baguaNetListen_v6 failed, dev=%d", dev);
        return ncclInternalError;
    }
    return ncclSuccess;
}

__hidden ncclResult_t baguaNetConnect_v6(int dev, void *handle, void **sendComm)
{
    if (BaguaNet::instance().connect(dev, handle, sendComm) != 0)
    {
        NCCL_WARN("baguaNetConnect_v6 failed, dev=%d", dev);
        return ncclInternalError;
    }
    return ncclSuccess;
}

__hidden ncclResult_t baguaNetAccept_v6(void *listenComm, void **recvComm)
{
    if (BaguaNet::instance().accept(listenComm, recvComm) != 0)
    {
        NCCL_WARN("baguaNetAccept_v6 failed, listenComm=%p", listenComm);
        return ncclInternalError;
    }
    return ncclSuccess;
}

__hidden ncclResult_t baguaNetRegMr_v6(void *comm, void *data, int size, int type, void **mhandle)
{
    return (type != NCCL_PTR_HOST) ? ncclInternalError : ncclSuccess;
}

__hidden ncclResult_t baguaNetRegMrDmaBuf_v6(void *comm, void *data, size_t size, int type,
                                             uint64_t offset, int fd, void **mhandle)
{
    // Host-pointer-only transport; DMA-BUF is never advertised (ptrSupport).
    return ncclInternalError;
}

__hidden ncclResult_t baguaNetDeregMr_v6(void *comm, void *mhandle)
{
    return ncclSuccess;
}

__hidden ncclResult_t baguaNetIsend_v6(void *sendComm, void *data, int size, int tag, void *mhandle, void **request)
{
    if (BaguaNet::instance().isend(sendComm, data, size, tag, mhandle, request) != 0)
    {
        NCCL_WARN("baguaNetIsend_v6 failed, sendComm=%p, size=%d", sendComm, size);
        return ncclInternalError;
    }
    return ncclSuccess;
}

__hidden ncclResult_t baguaNetIrecv_v6(void *recvComm, int n, void **data, int *sizes, int *tags,
                                       void **mhandles, void **request)
{
    if (n != 1)
    {
        NCCL_WARN("baguaNetIrecv_v6: grouped receives unsupported (n=%d)", n);
        return ncclInternalError;
    }
    if (BaguaNet::instance().irecv(recvComm, *data, *sizes, *tags, *mhandles, request) != 0)
    {
        NCCL_WARN("baguaNetIrecv_v6 failed, recvComm=%p", recvComm);
        return ncclInternalError;
    }
    return ncclSuccess;
}

__hidden ncclResult_t baguaNetFlush_v6(void *recvComm, int n, void **data, int *sizes, void **mhandles, void **request)
{
    // Host memory only; no flush needed.
    return ncclInternalError;
}

__hidden ncclResult_t baguaNetTest_v6(void *request, int *done, int *sizes)
{
    bool b_done = false;
    uintptr_t nbytes = 0;
    if (BaguaNet::instance().test(request, &b_done, &nbytes) != 0)
    {
        NCCL_WARN("baguaNetTest_v6 failed, request_id=%ld", *static_cast<uintptr_t *>(request));
        return ncclInternalError;
    }
    *done = b_done ? 1 : 0;
    if (b_done && sizes != NULL) *sizes = nbytes;
    return ncclSuccess;
}

__hidden ncclResult_t baguaNetCloseSend_v6(void *sendComm)
{
    return BaguaNet::instance().close_send(sendComm) == 0 ? ncclSuccess : ncclInternalError;
}

__hidden ncclResult_t baguaNetCloseRecv_v6(void *recvComm)
{
    return BaguaNet::instance().close_recv(recvComm) == 0 ? ncclSuccess : ncclInternalError;
}

__hidden ncclResult_t baguaNetCloseListen_v6(void *listenComm)
{
    return BaguaNet::instance().close_listen(listenComm) == 0 ? ncclSuccess : ncclInternalError;
}

extern "C" {
ncclNet_v6_t ncclNetPlugin_v6 = {
    "BaguaNet",
    baguaNetInit_v6,
    baguaNetDevices_v6,
    baguaNetGetProperties_v6,
    baguaNetListen_v6,
    baguaNetConnect_v6,
    baguaNetAccept_v6,
    baguaNetRegMr_v6,
    baguaNetRegMrDmaBuf_v6,
    baguaNetDeregMr_v6,
    baguaNetIsend_v6,
    baguaNetIrecv_v6,
    baguaNetFlush_v6,
    baguaNetTest_v6,
    baguaNetCloseSend_v6,
    baguaNetCloseRecv_v6,
    baguaNetCloseListen_v6};
}
