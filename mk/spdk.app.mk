#
#  BSD LICENSE
#
#  Copyright (c) Intel Corporation.
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions
#  are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in
#      the documentation and/or other materials provided with the
#      distribution.
#    * Neither the name of Intel Corporation nor the names of its
#      contributors may be used to endorse or promote products derived
#      from this software without specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
#  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
#  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
#  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
#  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
#  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

include $(SPDK_ROOT_DIR)/mk/spdk.app_vars.mk

# Applications in app/ go into build/bin/.
# Applications in examples/ go into build/examples/.
# Use findstring to identify if the current directory is in the app
# or examples directory. If it is, change the APP location.
APP_NAME := $(notdir $(APP))
ifneq (,$(findstring $(SPDK_ROOT_DIR)/app,$(CURDIR)))
	APP := $(APP_NAME:%=$(SPDK_ROOT_DIR)/build/bin/%)
else
ifneq (,$(findstring $(SPDK_ROOT_DIR)/examples,$(CURDIR)))
	APP := $(APP_NAME:%=$(SPDK_ROOT_DIR)/build/examples/%)
endif
endif

APP := $(APP)$(EXEEXT)

LIBS_FOLDER_INCLUDE := fault-injection-framework/libs/libfops/build

LIBS += $(SPDK_LIB_LINKER_ARGS)

LIBS += `pkg-config --libs glib-2.0`

LIBS += -L/usr/include/json-c -ljson-c

CLEAN_FILES = $(APP)

ifeq ($(findstring vfio_user,$(SPDK_LIB_FILES)),vfio_user)
VFIO_USER_LIB_FILE=$(VFIO_USER_LIBRARY_DIR)/libvfio-user.a
endif

all : $(APP)
	@:

install: empty_rule

uninstall: empty_rule

# To avoid overwriting warning
empty_rule:
	@:

$(APP) : $(OBJS) $(SPDK_LIB_FILES) $(ENV_LIBS) $(VFIO_USER_LIB_FILE)
	$(LINK_C)

clean :
	$(CLEAN_C) $(CLEAN_FILES)

include $(SPDK_ROOT_DIR)/mk/spdk.deps.mk
