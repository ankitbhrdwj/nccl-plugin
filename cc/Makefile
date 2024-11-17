PKG_CONFIG?=pkg-config

INC:= -I. -I./v5 -I./v3 -I/usr/share/tpa
PLUGIN_SO:=libnccl-net.so
BAGUA_NET_LIB:=libbagua_net.a
NCCL_NET_V5:=./v5/nccl_net_v5.cc
NCCL_NET_V3:=./v3/nccl_net_v3.cc
LDFLAGS+=$(shell $(PKG_CONFIG) --libs --static libtpa)
LDFLAGS += -lm

feature=default
ifeq ($(MAKECMDGOALS),storage)
    feature=storage
endif

default: $(PLUGIN_SO) $(BAGUA_NET_LIB)
storage: $(PLUGIN_SO) $(BAGUA_NET_LIB)

$(PLUGIN_SO): bagua_net.cc $(NCCL_NET_V5) $(NCCL_NET_V3) $(BAGUA_NET_LIB)
	$(CXX) -v $(INC) \
	-std=c++17 -fPIC -shared -O3 -march=native -flto \
	-o $@ $^ \
	-L. -l:$(BAGUA_NET_LIB) -lz -lcurl -libverbs -lmlx5 -lnuma $(LDFLAGS)

$(BAGUA_NET_LIB):
	cargo build --release --features $(feature) && cp ../target/release/$(BAGUA_NET_LIB) ./$(BAGUA_NET_LIB)

test:
	cargo test --verbose

clean:
	rm -f $(PLUGIN_SO) $(BAGUA_NET_LIB)
	cargo clean

tar: $(PLUGIN_SO)
	mkdir build && mv $(PLUGIN_SO) build \
		&& tar czf build.tar.gz build
