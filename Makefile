CXX = /usr/bin/g++
STRIP = /usr/bin/strip

PROJECT_ROOT ?= $(abspath $(CURDIR)/..)
GINIT_DIR = $(PROJECT_ROOT)/ginit
GLOBAL_SRC_DIR = $(PROJECT_ROOT)/src
APT_VENDOR_DIR = vendor/apt
APT_VENDOR_INCLUDE_DIR = $(APT_VENDOR_DIR)/include
APT_PKG_DIR = $(APT_VENDOR_DIR)/apt-pkg
APT_PRIVATE_DIR = $(APT_VENDOR_DIR)/apt-private
SYS_INFO_HEADER ?= $(GLOBAL_SRC_DIR)/sys_info.h
GPKG_VERSION_HELPER ?= $(PROJECT_ROOT)/tools/gpkg_version.py
ROOTFS ?=
TARGET_ROOTFS := $(strip $(ROOTFS))
ifeq ($(TARGET_ROOTFS),)
TARGET_ROOTFS := $(PROJECT_ROOT)/rootfs
endif
TARGET_CXX_VERSION := $(shell find $(TARGET_ROOTFS)/usr/include/c++ -maxdepth 1 -mindepth 1 -type d -printf '%f\n' 2>/dev/null | grep -E '^[0-9]+$$' | sort -V | tail -n1)
GPKG_VERSION ?=
GPKG_CODENAME ?=
LZMA_STATIC := $(firstword \
	$(wildcard $(TARGET_ROOTFS)/usr/lib/x86_64-linux-gnu/liblzma.a) \
	$(wildcard $(PROJECT_ROOT)/rootfs/usr/lib/x86_64-linux-gnu/liblzma.a))
ifeq ($(strip $(GPKG_VERSION)),)
ifneq ($(wildcard $(GPKG_VERSION_HELPER)),)
GPKG_VERSION := $(shell /usr/bin/python3 $(GPKG_VERSION_HELPER) --root-dir $(PROJECT_ROOT) --export-root $(PROJECT_ROOT)/export 2>/dev/null)
endif
endif
ifeq ($(strip $(GPKG_VERSION)),)
GPKG_VERSION := $(shell sed -n 's/^#define OS_VERSION "\(.*\)"/\1/p' $(SYS_INFO_HEADER) | head -n1)
endif
ifeq ($(strip $(GPKG_CODENAME)),)
GPKG_CODENAME := $(shell sed -n 's/^#define OS_CODENAME "\(.*\)"/\1/p' $(SYS_INFO_HEADER) | head -n1)
endif

BASE_CXXFLAGS += -std=gnu++23 -Wall -Wextra -O2
BASE_CXXFLAGS += -I./src -I$(GINIT_DIR)/src -I$(GLOBAL_SRC_DIR)
BASE_CXXFLAGS += -I$(APT_VENDOR_INCLUDE_DIR) -I$(APT_PKG_DIR) -I$(APT_PRIVATE_DIR)
BASE_CXXFLAGS += '-DGPKG_VERSION="$(GPKG_VERSION)"' '-DGPKG_CODENAME="$(GPKG_CODENAME)"'
APT_VENDOR_CXXFLAGS += -DGPKG_HAVE_WORKING_LIBAPT_PKG_BACKEND -DAPT_COMPILING_APT -DHAVE_CONFIG_H
ifneq ($(wildcard $(TARGET_ROOTFS)),)
BASE_CXXFLAGS += --sysroot=$(TARGET_ROOTFS)
BASE_LDFLAGS += --sysroot=$(TARGET_ROOTFS)
endif
ifneq ($(strip $(TARGET_ROOTFS)),)
ifneq ($(wildcard $(TARGET_ROOTFS)/usr/include),)
BASE_CXXFLAGS += -I$(TARGET_ROOTFS)/usr/include
endif
endif
BASE_CXXFLAGS += -I/usr/include -I/usr/include/x86_64-linux-gnu
ifneq ($(strip $(TARGET_CXX_VERSION)),)
BASE_CXXFLAGS += -nostdinc++
BASE_CXXFLAGS += -isystem $(TARGET_ROOTFS)/usr/include/c++/$(TARGET_CXX_VERSION)
BASE_CXXFLAGS += -isystem $(TARGET_ROOTFS)/usr/include/x86_64-linux-gnu/c++/$(TARGET_CXX_VERSION)
BASE_CXXFLAGS += -isystem $(TARGET_ROOTFS)/usr/include/c++/$(TARGET_CXX_VERSION)/backward
endif
APT_VENDOR_SOURCES := $(sort $(shell find $(APT_PKG_DIR) -name '*.cc' -print))

OPTIONAL_VENDOR_LIBS :=
ifneq ($(wildcard $(TARGET_ROOTFS)/usr/lib/x86_64-linux-gnu/libsystemd.so*),)
OPTIONAL_VENDOR_LIBS += -lsystemd
endif
ifneq ($(wildcard $(TARGET_ROOTFS)/usr/lib/x86_64-linux-gnu/libudev.so*),)
OPTIONAL_VENDOR_LIBS += -ludev
endif
ifneq ($(wildcard $(TARGET_ROOTFS)/usr/lib/x86_64-linux-gnu/libseccomp.so*),)
OPTIONAL_VENDOR_LIBS += -lseccomp
endif
ifneq ($(wildcard $(TARGET_ROOTFS)/usr/lib/x86_64-linux-gnu/libxxhash.so*),)
OPTIONAL_VENDOR_LIBS += -lxxhash
endif

GPKG_LDFLAGS = $(BASE_LDFLAGS) $(LDFLAGS) -L$(GINIT_DIR)/lib -lgemcore -lssl -lcrypto -lz -lzstd -lbz2 -llz4 -ldl -lpthread -lcrypt -lresolv -lutil $(OPTIONAL_VENDOR_LIBS)
ifeq ($(strip $(LZMA_STATIC)),)
GPKG_LDFLAGS += -llzma
else
GPKG_LDFLAGS += $(LZMA_STATIC)
endif
WORKER_LDFLAGS = $(BASE_LDFLAGS) $(LDFLAGS) -lssl -lcrypto -lz -lzstd -ldl -lpthread
ifeq ($(strip $(LZMA_STATIC)),)
WORKER_LDFLAGS += -llzma
else
WORKER_LDFLAGS += $(LZMA_STATIC)
endif
ifneq ($(strip $(TARGET_CXX_VERSION)),)
GPKG_LDFLAGS += $(TARGET_ROOTFS)/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2
WORKER_LDFLAGS += $(TARGET_ROOTFS)/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2
endif

SRCDIR = src
OBJDIR = obj
BINDIR = bin
GPKG_FRAGMENTS = $(wildcard $(SRCDIR)/*.ipp)
APT_VENDOR_OBJECTS := $(patsubst %.cc,$(OBJDIR)/%.o,$(APT_VENDOR_SOURCES))

TARGETS = $(BINDIR)/gpkg $(BINDIR)/gpkg-worker
BUILD_CONFIG_STAMP = $(OBJDIR)/build-config.stamp

all: $(BINDIR) $(OBJDIR) $(BUILD_CONFIG_STAMP) $(TARGETS)

$(BINDIR) $(OBJDIR):
	mkdir -p $@

FORCE:

$(BUILD_CONFIG_STAMP): FORCE | $(OBJDIR)
	@tmp="$@.tmp"; \
	printf 'GPKG_VERSION=%s\nGPKG_CODENAME=%s\nTARGET_CXX_VERSION=%s\nROOTFS=%s\n' \
		'$(GPKG_VERSION)' '$(GPKG_CODENAME)' '$(TARGET_CXX_VERSION)' '$(TARGET_ROOTFS)' > "$$tmp"; \
	if [ -f "$@" ] && cmp -s "$@" "$$tmp"; then \
		rm -f "$$tmp"; \
	else \
		mv "$$tmp" "$@"; \
	fi

$(OBJDIR)/%.o: %.cc $(BUILD_CONFIG_STAMP)
	@mkdir -p $(dir $@)
	$(CXX) $(BASE_CXXFLAGS) $(APT_VENDOR_CXXFLAGS) $(CXXFLAGS) -c -o $@ $<

$(BINDIR)/gpkg: $(SRCDIR)/gpkg.cpp $(GPKG_FRAGMENTS) $(APT_VENDOR_OBJECTS) $(BUILD_CONFIG_STAMP)
	$(CXX) $(BASE_CXXFLAGS) $(APT_VENDOR_CXXFLAGS) $(CXXFLAGS) -o $@ $< $(APT_VENDOR_OBJECTS) $(GPKG_LDFLAGS)
	$(STRIP) $@

$(BINDIR)/gpkg-worker: $(SRCDIR)/gpkg_worker.cpp $(GPKG_FRAGMENTS) $(BUILD_CONFIG_STAMP)
	$(CXX) $(BASE_CXXFLAGS) $(CXXFLAGS) -o $@ $< $(WORKER_LDFLAGS)
	$(STRIP) $@

install: all
	mkdir -p $(DESTDIR)/bin/apps/system
	mkdir -p $(DESTDIR)/bin
	rm -f $(DESTDIR)/bin/apps/system/gpkg-v2 $(DESTDIR)/bin/apps/system/gpkg-v2-worker
	rm -f $(DESTDIR)/bin/gpkg-v2 $(DESTDIR)/bin/gpkg-v2-worker
	cp $(BINDIR)/gpkg $(DESTDIR)/bin/apps/system/gpkg
	cp $(BINDIR)/gpkg-worker $(DESTDIR)/bin/apps/system/gpkg-worker
	ln -sf /bin/apps/system/gpkg $(DESTDIR)/bin/gpkg

clean:
	rm -rf $(OBJDIR) $(BINDIR)

.PHONY: all install clean FORCE
