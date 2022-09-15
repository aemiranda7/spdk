/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2021 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * This is a simple example of a virtual block device module that passes IO
 * down to a bdev (or bdevs) that its configured to attach to.
 */


#include "json-c/json.h"
#include "spdk/stdinc.h"
#include "glib.h"

#include "vbdev_faulty.h"
#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/endian.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"
#include "spdk/libfops.h" //my faults library.
#include "spdk/file.h"

#include "spdk/bdev_module.h"
#include "spdk/log.h"


static int vbdev_faulty_init(void);
static int vbdev_faulty_get_ctx_size(void);
static void vbdev_faulty_examine(struct spdk_bdev *bdev);
static void vbdev_faulty_finish(void);
static int vbdev_faulty_config_json(struct spdk_json_write_ctx *w);

static struct spdk_bdev_module faulty_if = {
	.name = "faulty",
	.module_init = vbdev_faulty_init,
	.get_ctx_size = vbdev_faulty_get_ctx_size,
	.examine_config = vbdev_faulty_examine,
	.module_fini = vbdev_faulty_finish,
	.config_json = vbdev_faulty_config_json
};


typedef enum faultType{
    CORRUPT_CONTENT, /**< Corrupt the content of the buffer */
    DELAY_OPERATION, /**< Delays the operation */
    MEDIUM_ERROR /**< Returns a given error */
} FaultType;

typedef enum faultFrequency{
	ALL, /**< Corrupt all the requests */
	ALL_AFTER, /**< Corrupt all the requests after x requests made*/
	INTERVAL /**< Corrupt one requests every x requests*/
} FaultFrequency;

struct spdk_file_faults{
	int total_requests;
	pthread_mutex_t *req_lock;

	FaultType fault_type;
	FaultFrequency fault_freq;
	int n_requests;
	union{
		struct {
			BufferCorruptionPattern pattern;
			int customOffset;
			int customIndex;
		} content_corruption;
		double delay_time;
		int error_number;
	}u;
};


GHashTable *write_hashtable;
GHashTable *read_hashtable;

SPDK_BDEV_MODULE_REGISTER(faulty, &faulty_if)

/* List of pt_bdev names and their base bdevs via configuration file.
 * Used so we can parse the conf once at init and use this list in examine().
 */
struct bdev_names {
	char			*vbdev_name;
	char			*bdev_name;
	TAILQ_ENTRY(bdev_names)	link;
};
static TAILQ_HEAD(, bdev_names) g_bdev_names = TAILQ_HEAD_INITIALIZER(g_bdev_names);

/* List of virtual bdevs and associated info for each. */
struct vbdev_faulty {
	struct spdk_bdev		*base_bdev; /* the thing we're attaching to */
	struct spdk_bdev_desc		*base_desc; /* its descriptor we get from open */
	struct spdk_bdev		pt_bdev;    /* the PT virtual bdev */
	TAILQ_ENTRY(vbdev_faulty)	link;
	struct spdk_thread		*thread;    /* thread where base device is opened */
};
static TAILQ_HEAD(, vbdev_faulty) g_pt_nodes = TAILQ_HEAD_INITIALIZER(g_pt_nodes);

/* The pt vbdev channel struct. It is allocated and freed on my behalf by the io channel code.
 * If this vbdev needed to implement a poller or a queue for IO, this is where those things
 * would be defined. This faulty bdev doesn't actually need to allocate a channel, it could
 * simply pass back the channel of the bdev underneath it but for example purposes we will
 * present its own to the upper layers.
 */
struct pt_io_channel {
	struct spdk_io_channel	*base_ch; /* IO channel of base device */
};

/* Just for fun, this pt_bdev module doesn't need it but this is essentially a per IO
 * context that we get handed by the bdev layer.
 */
struct faulty_bdev_io {
	uint8_t test;

	/* bdev related */
	struct spdk_io_channel *ch;

	/* for bdev_io_wait */
	struct spdk_bdev_io_wait_entry bdev_io_wait;
};

//-------------------------------my added functions----------------------------------
static void 
corrupt_request_file(struct spdk_bdev_io *bdev_io,char* fileName);

static void 
_print_corruption_pattern(BufferCorruptionPattern pattern);

static void 
_print_fault_struct (gpointer key, gpointer value, gpointer user_data);

static void 
_print_hashtable(GHashTable *hashtable);

static int 
vbdev_read_conf_file(void);
//----------------------------------end of my added functions--------------------------/

static void
vbdev_faulty_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io);


/* Callback for unregistering the IO device. */
static void
_device_unregister_cb(void *io_device)
{
	struct vbdev_faulty *pt_node  = io_device;

	/* Done with this pt_node. */
	free(pt_node->pt_bdev.name);
	free(pt_node);
}

/* Wrapper for the bdev close operation. */
static void
_vbdev_faulty_destruct(void *ctx)
{
	struct spdk_bdev_desc *desc = ctx;

	spdk_bdev_close(desc);
}

/* Called after we've unregistered following a hot remove callback.
 * Our finish entry point will be called next.
 */
static int
vbdev_faulty_destruct(void *ctx)
{
	struct vbdev_faulty *pt_node = (struct vbdev_faulty *)ctx;

	/* It is important to follow this exact sequence of steps for destroying
	 * a vbdev...
	 */

	TAILQ_REMOVE(&g_pt_nodes, pt_node, link);

	/* Unclaim the underlying bdev. */
	spdk_bdev_module_release_bdev(pt_node->base_bdev);

	/* Close the underlying bdev on its same opened thread. */
	if (pt_node->thread && pt_node->thread != spdk_get_thread()) {
		spdk_thread_send_msg(pt_node->thread, _vbdev_faulty_destruct, pt_node->base_desc);
	} else {
		spdk_bdev_close(pt_node->base_desc);
	}

	/* Unregister the io_device. */
	spdk_io_device_unregister(pt_node, _device_unregister_cb);

	return 0;
}

/* Completion callback for IO that were issued from this bdev. The original bdev_io
 * is passed in as an arg so we'll complete that one with the appropriate status
 * and then free the one that this module issued.
 */
static void
_pt_complete_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
	struct faulty_bdev_io *io_ctx = (struct faulty_bdev_io *)orig_io->driver_ctx;

	/* We setup this value in the submission routine, just showing here that it is
	 * passed back to us.
	 */
	if (io_ctx->test != 0x5a) {
		SPDK_ERRLOG("Error, original IO device_ctx is wrong! 0x%x\n",
			    io_ctx->test);
	}

	char* file_id = (char *) orig_io->flag;
	if(file_id && (orig_io->type==SPDK_BDEV_IO_TYPE_READ)){
		//printf("This read is for the file %s\n",(char*)orig_io->flag);
		//printf("The original content of read is %s\n",(char*)orig_io->u.bdev.iovs->iov_base);
		corrupt_request_file(orig_io,file_id);
		//free(file_id);
	}
	

	/* Complete the original IO and then free the one that we created here
	 * as a result of issuing an IO via submit_request.
	 */
	spdk_bdev_io_complete(orig_io, status);
	spdk_bdev_free_io(bdev_io);
}

static void
_pt_complete_zcopy_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
	struct faulty_bdev_io *io_ctx = (struct faulty_bdev_io *)orig_io->driver_ctx;

	/* We setup this value in the submission routine, just showing here that it is
	 * passed back to us.
	 */
	if (io_ctx->test != 0x5a) {
		SPDK_ERRLOG("Error, original IO device_ctx is wrong! 0x%x\n",
			    io_ctx->test);
	}

	/* Complete the original IO and then free the one that we created here
	 * as a result of issuing an IO via submit_request.
	 */
	spdk_bdev_io_set_buf(orig_io, bdev_io->u.bdev.iovs[0].iov_base, bdev_io->u.bdev.iovs[0].iov_len);
	spdk_bdev_io_complete(orig_io, status);
	spdk_bdev_free_io(bdev_io);
}

static void
vbdev_faulty_resubmit_io(void *arg)
{
	struct spdk_bdev_io *bdev_io = (struct spdk_bdev_io *)arg;
	struct faulty_bdev_io *io_ctx = (struct faulty_bdev_io *)bdev_io->driver_ctx;

	vbdev_faulty_submit_request(io_ctx->ch, bdev_io);
}

static void
vbdev_faulty_queue_io(struct spdk_bdev_io *bdev_io)
{
	struct faulty_bdev_io *io_ctx = (struct faulty_bdev_io *)bdev_io->driver_ctx;
	struct pt_io_channel *pt_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	int rc;

	io_ctx->bdev_io_wait.bdev = bdev_io->bdev;
	io_ctx->bdev_io_wait.cb_fn = vbdev_faulty_resubmit_io;
	io_ctx->bdev_io_wait.cb_arg = bdev_io;

	/* Queue the IO using the channel of the base device. */
	rc = spdk_bdev_queue_io_wait(bdev_io->bdev, pt_ch->base_ch, &io_ctx->bdev_io_wait);
	if (rc != 0) {
		SPDK_ERRLOG("Queue io failed in vbdev_faulty_queue_io, rc=%d.\n", rc);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

/* Callback for getting a buf from the bdev pool in the event that the caller passed
 * in NULL, we need to own the buffer so it doesn't get freed by another vbdev module
 * beneath us before we're done with it. That won't happen in this example but it could
 * if this example were used as a template for something more complex.
 */
static void
pt_read_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	struct vbdev_faulty *pt_node = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_faulty,
					 pt_bdev);
	struct pt_io_channel *pt_ch = spdk_io_channel_get_ctx(ch);
	struct faulty_bdev_io *io_ctx = (struct faulty_bdev_io *)bdev_io->driver_ctx;
	int rc;

	if (!success) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	if (bdev_io->u.bdev.md_buf == NULL) {
		rc = spdk_bdev_readv_blocks(pt_node->base_desc, pt_ch->base_ch, bdev_io->u.bdev.iovs,
					    bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks, _pt_complete_io,
					    bdev_io);
	} else {
		rc = spdk_bdev_readv_blocks_with_md(pt_node->base_desc, pt_ch->base_ch,
						    bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						    bdev_io->u.bdev.md_buf,
						    bdev_io->u.bdev.offset_blocks,
						    bdev_io->u.bdev.num_blocks,
						    _pt_complete_io, bdev_io);
	}

	if (rc != 0) {
		if (rc == -ENOMEM) {
			SPDK_ERRLOG("No memory, start to queue io for faulty.\n");
			io_ctx->ch = ch;
			vbdev_faulty_queue_io(bdev_io);
		} else {
			SPDK_ERRLOG("ERROR on bdev_io submission!\n");
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}
}


/*static int 
corrupt(struct spdk_bdev_io *bdev_io, struct fault_injection_tag *flag){
	int rc = 0;
	switch (flag->fault_type)
	{
	case CORRUPT_BUFFER:
		SPDK_NOTICELOG("Corrupting the content!");
		corrupt_buffer(bdev_io->u.bdev.iovs->iov_base,bdev_io->u.bdev.iovs->iov_len,
		               flag->u.content_corruption.pattern,flag->u.content_corruption.customOffset,
					   flag->u.content_corruption.customIndex);
		break;

	case DELAY_OPERATION:
		SPDK_NOTICELOG("Delaying the operation!");
		operation_delay(flag->u.delay_time);
		break;

	case MEDIUM_ERROR:
		SPDK_NOTICELOG("Returning an error!");
		rc = medium_error(flag->u.error_number);
		break;
	default:
		break;
	}
	return rc;
}*/

/* Called when someone above submits IO to this pt vbdev. We're simply passing it on here
 * via SPDK IO calls which in turn allocate another bdev IO and call our cpl callback provided
 * below along with the original bdev_io so that we can complete it once this IO completes.
 */
static void
vbdev_faulty_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_faulty *pt_node = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_faulty, pt_bdev);
	struct pt_io_channel *pt_ch = spdk_io_channel_get_ctx(ch);
	struct faulty_bdev_io *io_ctx = (struct faulty_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;
	
	

	/* Setup a per IO context value; we don't do anything with it in the vbdev other
	 * than confirm we get the same thing back in the completion callback just to
	 * demonstrate.
	 */
	io_ctx->test = 0x5a;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		spdk_bdev_io_get_buf(bdev_io, pt_read_get_buf_cb,
				     bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		if(bdev_io->flag){
			 printf("This write is for the file %s\n",(char *)bdev_io->flag);
			 //printf("The content of write is : %s\n",(char*)bdev_io->u.bdev.iovs->iov_base);
			 corrupt_request_file(bdev_io,(char *)bdev_io->flag);
		}
		if (bdev_io->u.bdev.md_buf == NULL && rc==0) {
			//corrupt_buffer(bdev_io->u.bdev.iovs->iov_base,bdev_io->u.bdev.iovs->iov_len,REPLACE_ALL_ZEROS,-1,-1);
			//operation_delay(5);

			//corrupt(bdev_io,spdk_thread_get_tag());
		    rc = spdk_bdev_writev_blocks(pt_node->base_desc, pt_ch->base_ch, bdev_io->u.bdev.iovs,
						     bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
						     bdev_io->u.bdev.num_blocks, _pt_complete_io,
						     bdev_io);
			
			//rc = medium_error(-1);
		} else if (rc==0){
			rc = spdk_bdev_writev_blocks_with_md(pt_node->base_desc, pt_ch->base_ch,
							     bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
							     bdev_io->u.bdev.md_buf,
							     bdev_io->u.bdev.offset_blocks,
							     bdev_io->u.bdev.num_blocks,
							     _pt_complete_io, bdev_io);
		}
		break;
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		rc = spdk_bdev_write_zeroes_blocks(pt_node->base_desc, pt_ch->base_ch,
						   bdev_io->u.bdev.offset_blocks,
						   bdev_io->u.bdev.num_blocks,
						   _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_UNMAP:
		rc = spdk_bdev_unmap_blocks(pt_node->base_desc, pt_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_FLUSH:
		rc = spdk_bdev_flush_blocks(pt_node->base_desc, pt_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_RESET:
		rc = spdk_bdev_reset(pt_node->base_desc, pt_ch->base_ch,
				     _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_ZCOPY:
		rc = spdk_bdev_zcopy_start(pt_node->base_desc, pt_ch->base_ch, NULL, 0,
					   bdev_io->u.bdev.offset_blocks,
					   bdev_io->u.bdev.num_blocks, bdev_io->u.bdev.zcopy.populate,
					   _pt_complete_zcopy_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_ABORT:
		rc = spdk_bdev_abort(pt_node->base_desc, pt_ch->base_ch, bdev_io->u.abort.bio_to_abort,
				     _pt_complete_io, bdev_io);
		break;
	default:
		SPDK_ERRLOG("faulty: unknown I/O type %d\n", bdev_io->type);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}
	if (rc != 0) {
		if (rc == -ENOMEM) {
			SPDK_ERRLOG("No memory, start to queue io for faulty.\n");
			io_ctx->ch = ch;
			vbdev_faulty_queue_io(bdev_io);
		} else {
			SPDK_ERRLOG("ERROR on bdev_io submission!\n");
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}
}

/* We'll just call the base bdev and let it answer however if we were more
 * restrictive for some reason (or less) we could get the response back
 * and modify according to our purposes.
 */
static bool
vbdev_faulty_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	struct vbdev_faulty *pt_node = (struct vbdev_faulty *)ctx;

	return spdk_bdev_io_type_supported(pt_node->base_bdev, io_type);
}

/* We supplied this as an entry point for upper layers who want to communicate to this
 * bdev.  This is how they get a channel. We are passed the same context we provided when
 * we created our PT vbdev in examine() which, for this bdev, is the address of one of
 * our context nodes. From here we'll ask the SPDK channel code to fill out our channel
 * struct and we'll keep it in our PT node.
 */
static struct spdk_io_channel *
vbdev_faulty_get_io_channel(void *ctx)
{
	struct vbdev_faulty *pt_node = (struct vbdev_faulty *)ctx;
	struct spdk_io_channel *pt_ch = NULL;

	/* The IO channel code will allocate a channel for us which consists of
	 * the SPDK channel structure plus the size of our pt_io_channel struct
	 * that we passed in when we registered our IO device. It will then call
	 * our channel create callback to populate any elements that we need to
	 * update.
	 */
	pt_ch = spdk_get_io_channel(pt_node);

	return pt_ch;
}

/* This is the output for bdev_get_bdevs() for this vbdev */
static int
vbdev_faulty_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct vbdev_faulty *pt_node = (struct vbdev_faulty *)ctx;

	spdk_json_write_name(w, "faulty");
	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&pt_node->pt_bdev));
	spdk_json_write_named_string(w, "base_bdev_name", spdk_bdev_get_name(pt_node->base_bdev));
	spdk_json_write_object_end(w);

	return 0;
}

/* This is used to generate JSON that can configure this module to its current state. */
static int
vbdev_faulty_config_json(struct spdk_json_write_ctx *w)
{
	struct vbdev_faulty *pt_node;

	TAILQ_FOREACH(pt_node, &g_pt_nodes, link) {
		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w, "method", "bdev_faulty_create");
		spdk_json_write_named_object_begin(w, "params");
		spdk_json_write_named_string(w, "base_bdev_name", spdk_bdev_get_name(pt_node->base_bdev));
		spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&pt_node->pt_bdev));
		spdk_json_write_object_end(w);
		spdk_json_write_object_end(w);
	}
	return 0;
}

/* We provide this callback for the SPDK channel code to create a channel using
 * the channel struct we provided in our module get_io_channel() entry point. Here
 * we get and save off an underlying base channel of the device below us so that
 * we can communicate with the base bdev on a per channel basis.  If we needed
 * our own poller for this vbdev, we'd register it here.
 */
static int
pt_bdev_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct pt_io_channel *pt_ch = ctx_buf;
	struct vbdev_faulty *pt_node = io_device;

	pt_ch->base_ch = spdk_bdev_get_io_channel(pt_node->base_desc);

	return 0;
}

/* We provide this callback for the SPDK channel code to destroy a channel
 * created with our create callback. We just need to undo anything we did
 * when we created. If this bdev used its own poller, we'd unregister it here.
 */
static void
pt_bdev_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct pt_io_channel *pt_ch = ctx_buf;

	spdk_put_io_channel(pt_ch->base_ch);
}

/* Create the faulty association from the bdev and vbdev name and insert
 * on the global list. */
static int
vbdev_faulty_insert_name(const char *bdev_name, const char *vbdev_name)
{
	struct bdev_names *name;

	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(vbdev_name, name->vbdev_name) == 0) {
			SPDK_ERRLOG("faulty bdev %s already exists\n", vbdev_name);
			return -EEXIST;
		}
	}

	name = calloc(1, sizeof(struct bdev_names));
	if (!name) {
		SPDK_ERRLOG("could not allocate bdev_names\n");
		return -ENOMEM;
	}

	name->bdev_name = strdup(bdev_name);
	if (!name->bdev_name) {
		SPDK_ERRLOG("could not allocate name->bdev_name\n");
		free(name);
		return -ENOMEM;
	}

	name->vbdev_name = strdup(vbdev_name);
	if (!name->vbdev_name) {
		SPDK_ERRLOG("could not allocate name->vbdev_name\n");
		free(name->bdev_name);
		free(name);
		return -ENOMEM;
	}

	TAILQ_INSERT_TAIL(&g_bdev_names, name, link);

	return 0;
}

/* On init, just perform bdev module specific initialization. */
static int
vbdev_faulty_init(void)
{
	write_hashtable = g_hash_table_new(g_str_hash,g_str_equal); 
	read_hashtable  = g_hash_table_new(g_str_hash,g_str_equal);
	vbdev_read_conf_file();
	return 0;
}

/* Called when the entire module is being torn down. */
static void
vbdev_faulty_finish(void)
{
	struct bdev_names *name;

	while ((name = TAILQ_FIRST(&g_bdev_names))) {
		TAILQ_REMOVE(&g_bdev_names, name, link);
		free(name->bdev_name);
		free(name->vbdev_name);
		free(name);
	}
}

/* During init we'll be asked how much memory we'd like passed to us
 * in bev_io structures as context. Here's where we specify how
 * much context we want per IO.
 */
static int
vbdev_faulty_get_ctx_size(void)
{
	return sizeof(struct faulty_bdev_io);
}

/* Where vbdev_faulty_config_json() is used to generate per module JSON config data, this
 * function is called to output any per bdev specific methods. For the PT module, there are
 * none.
 */
static void
vbdev_faulty_write_config_json(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	/* No config per bdev needed */
}

static int
vbdev_faulty_get_memory_domains(void *ctx, struct spdk_memory_domain **domains, int array_size)
{
	struct vbdev_faulty *pt_node = (struct vbdev_faulty *)ctx;

	/* Passthru bdev doesn't work with data buffers, so it supports any memory domain used by base_bdev */
	return spdk_bdev_get_memory_domains(pt_node->base_bdev, domains, array_size);
}

/* When we register our bdev this is how we specify our entry points. */
static const struct spdk_bdev_fn_table vbdev_faulty_fn_table = {
	.destruct		= vbdev_faulty_destruct,
	.submit_request		= vbdev_faulty_submit_request,
	.io_type_supported	= vbdev_faulty_io_type_supported,
	.get_io_channel		= vbdev_faulty_get_io_channel,
	.dump_info_json		= vbdev_faulty_dump_info_json,
	.write_config_json	= vbdev_faulty_write_config_json,
	.get_memory_domains	= vbdev_faulty_get_memory_domains,
};

static void
vbdev_faulty_base_bdev_hotremove_cb(struct spdk_bdev *bdev_find)
{
	struct vbdev_faulty *pt_node, *tmp;

	TAILQ_FOREACH_SAFE(pt_node, &g_pt_nodes, link, tmp) {
		if (bdev_find == pt_node->base_bdev) {
			spdk_bdev_unregister(&pt_node->pt_bdev, NULL, NULL);
		}
	}
}

/* Called when the underlying base bdev triggers asynchronous event such as bdev removal. */
static void
vbdev_faulty_base_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
				  void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		vbdev_faulty_base_bdev_hotremove_cb(bdev);
		break;
	default:
		SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
		break;
	}
}

/* Create and register the faulty vbdev if we find it in our list of bdev names.
 * This can be called either by the examine path or RPC method.
 */
static int
vbdev_faulty_register(const char *bdev_name)
{
	struct bdev_names *name;
	struct vbdev_faulty *pt_node;
	struct spdk_bdev *bdev;
	int rc = 0;

	/* Check our list of names from config versus this bdev and if
	 * there's a match, create the pt_node & bdev accordingly.
	 */
	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(name->bdev_name, bdev_name) != 0) {
			continue;
		}

		SPDK_NOTICELOG("Match on %s\n", bdev_name);
		pt_node = calloc(1, sizeof(struct vbdev_faulty));
		if (!pt_node) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate pt_node\n");
			break;
		}

		pt_node->pt_bdev.name = strdup(name->vbdev_name);
		if (!pt_node->pt_bdev.name) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate pt_bdev name\n");
			free(pt_node);
			break;
		}
		pt_node->pt_bdev.product_name = "faulty";

		/* The base bdev that we're attaching to. */
		rc = spdk_bdev_open_ext(bdev_name, true, vbdev_faulty_base_bdev_event_cb,
					NULL, &pt_node->base_desc);
		if (rc) {
			if (rc != -ENODEV) {
				SPDK_ERRLOG("could not open bdev %s\n", bdev_name);
			}
			free(pt_node->pt_bdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("base bdev opened\n");

		bdev = spdk_bdev_desc_get_bdev(pt_node->base_desc);
		pt_node->base_bdev = bdev;

		/* Copy some properties from the underlying base bdev. */
		pt_node->pt_bdev.write_cache = bdev->write_cache;
		pt_node->pt_bdev.required_alignment = bdev->required_alignment;
		pt_node->pt_bdev.optimal_io_boundary = bdev->optimal_io_boundary;
		pt_node->pt_bdev.blocklen = bdev->blocklen;
		pt_node->pt_bdev.blockcnt = bdev->blockcnt;

		pt_node->pt_bdev.md_interleave = bdev->md_interleave;
		pt_node->pt_bdev.md_len = bdev->md_len;
		pt_node->pt_bdev.dif_type = bdev->dif_type;
		pt_node->pt_bdev.dif_is_head_of_md = bdev->dif_is_head_of_md;
		pt_node->pt_bdev.dif_check_flags = bdev->dif_check_flags;

		/* This is the context that is passed to us when the bdev
		 * layer calls in so we'll save our pt_bdev node here.
		 */
		pt_node->pt_bdev.ctxt = pt_node;
		pt_node->pt_bdev.fn_table = &vbdev_faulty_fn_table;
		pt_node->pt_bdev.module = &faulty_if;
		TAILQ_INSERT_TAIL(&g_pt_nodes, pt_node, link);

		spdk_io_device_register(pt_node, pt_bdev_ch_create_cb, pt_bdev_ch_destroy_cb,
					sizeof(struct pt_io_channel),
					name->vbdev_name);
		SPDK_NOTICELOG("io_device created at: 0x%p\n", pt_node);

		/* Save the thread where the base device is opened */
		pt_node->thread = spdk_get_thread();

		rc = spdk_bdev_module_claim_bdev(bdev, pt_node->base_desc, pt_node->pt_bdev.module);
		if (rc) {
			SPDK_ERRLOG("could not claim bdev %s\n", bdev_name);
			spdk_bdev_close(pt_node->base_desc);
			TAILQ_REMOVE(&g_pt_nodes, pt_node, link);
			spdk_io_device_unregister(pt_node, NULL);
			free(pt_node->pt_bdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("bdev claimed\n");

		rc = spdk_bdev_register(&pt_node->pt_bdev);
		if (rc) {
			SPDK_ERRLOG("could not register pt_bdev\n");
			spdk_bdev_module_release_bdev(&pt_node->pt_bdev);
			spdk_bdev_close(pt_node->base_desc);
			TAILQ_REMOVE(&g_pt_nodes, pt_node, link);
			spdk_io_device_unregister(pt_node, NULL);
			free(pt_node->pt_bdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("pt_bdev registered\n");
		SPDK_NOTICELOG("created pt_bdev for: %s\n", name->vbdev_name);
	}

	return rc;
}

/* Create the faulty disk from the given bdev and vbdev name. */
int
bdev_faulty_create_disk(const char *bdev_name, const char *vbdev_name)
{
	int rc;

	/* Insert the bdev name into our global name list even if it doesn't exist yet,
	 * it may show up soon...
	 */
	rc = vbdev_faulty_insert_name(bdev_name, vbdev_name);
	if (rc) {
		return rc;
	}

	rc = vbdev_faulty_register(bdev_name);
	if (rc == -ENODEV) {
		/* This is not an error, we tracked the name above and it still
		 * may show up later.
		 */
		SPDK_NOTICELOG("vbdev creation deferred pending base bdev arrival\n");
		rc = 0;
	}

	return rc;
}

void
bdev_faulty_delete_disk(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn, void *cb_arg)
{
	struct bdev_names *name;

	if (!bdev || bdev->module != &faulty_if) {
		cb_fn(cb_arg, -ENODEV);
		return;
	}

	/* Remove the association (vbdev, bdev) from g_bdev_names. This is required so that the
	 * vbdev does not get re-created if the same bdev is constructed at some other time,
	 * unless the underlying bdev was hot-removed.
	 */
	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(name->vbdev_name, bdev->name) == 0) {
			TAILQ_REMOVE(&g_bdev_names, name, link);
			free(name->bdev_name);
			free(name->vbdev_name);
			free(name);
			break;
		}
	}

	/* Additional cleanup happens in the destruct callback. */
	spdk_bdev_unregister(bdev, cb_fn, cb_arg);
}

/* Because we specified this function in our pt bdev function table when we
 * registered our pt bdev, we'll get this call anytime a new bdev shows up.
 * Here we need to decide if we care about it and if so what to do. We
 * parsed the config file at init so we check the new bdev against the list
 * we built up at that time and if the user configured us to attach to this
 * bdev, here's where we do it.
 */
static void
vbdev_faulty_examine(struct spdk_bdev *bdev)
{
	vbdev_faulty_register(bdev->name);

	spdk_bdev_module_examine_done(&faulty_if);
}

static void _print_corruption_pattern(BufferCorruptionPattern pattern){
	switch (pattern)
	{
	case REPLACE_ALL_ZEROS:
		printf("corruptionPattern: REPLACE_ALL_ZEROS\n");
		break;
	case REPLACE_ALL_ONES:
		printf("corruptionPattern: REPLACE_ALL_ONES\n");
		break;
	case BITFLIP_RANDOM_INDEX:
		printf("corruptionPattern: BITFLIP_RANDOM_INDEX\n");
		break;
	case BITFLIP_CUSTOM_INDEX:
		printf("corruptionPattern: BITFLIP_CUSTOM_INDEX\n");
		break;
	default:
		break;
	}
}

static void _print_fault_struct (gpointer key, gpointer value, gpointer user_data){

	printf("fileName : %s\n",(char*)key);
	struct spdk_file_faults *fault = (struct spdk_file_faults *) value;

	switch (fault->fault_freq)
	{
	case ALL:
		printf("faultFreq: ALL\n");
		break;
	case ALL_AFTER:
		printf("faultFreq: ALL_AFTER\n");
		printf("nReq: %d\n",fault->n_requests);
		break;
	case INTERVAL:
		printf("faultFreq: INTERVAL\n");
		printf("nReq: %d\n",fault->n_requests);
		break;
	default:
		break;
	}
	
	
	switch (fault->fault_type)
	{
		case CORRUPT_CONTENT:
			printf("faultType: CORRUPT_CONTENT\n");
			_print_corruption_pattern(fault->u.content_corruption.pattern);
			if(fault->u.content_corruption.pattern==BITFLIP_CUSTOM_INDEX){
				printf("customOffset: %d\n",fault->u.content_corruption.customOffset);
				printf("customIndex: %d\n",fault->u.content_corruption.customIndex);
			}
			break;
		case DELAY_OPERATION:
			printf("faultType: DELAY_OPERATION\n");
			printf("delay_time: %f\n", fault->u.delay_time);
			break;
		case MEDIUM_ERROR:
			printf("faultType: MEDIUM_ERROR\n");
			printf("error_number: %d\n", fault->u.error_number);
			break;
		default:
			break;
	}
}

static void _print_hashtable(GHashTable * hashtable){
	g_hash_table_foreach(hashtable,_print_fault_struct,NULL);
}


static void _load_wr_files(struct json_object *write, struct json_object *read){
	GHashTable *aux_hash[2] = {write_hashtable,read_hashtable};
	json_object *aux[2] = {write,read};
	char *aux_str[2]; 
	aux_str[0] = strdup("write"); 
	aux_str[1] = strdup("read");

	for(int a = 0; a<2; a++){
		struct json_object *files;

		if(json_object_object_get_ex(aux[a],"files",&files)){
			SPDK_NOTICELOG("%s files loaded successfully!\n",aux_str[a]);
		}
		else SPDK_ERRLOG("%s files not loaded!\n",aux_str[a]);

		size_t n_files = json_object_array_length(files);

		
		
		for(size_t i = 0; i<n_files; i++){

			struct spdk_file_faults *file_faults = (struct spdk_file_faults *)malloc(sizeof(struct spdk_file_faults));


			struct json_object *aux_file = json_object_array_get_idx(files,i);
			struct json_object *file_name_obj;
			if(!json_object_object_get_ex(aux_file,"fileName",&file_name_obj)){
				SPDK_ERRLOG(
					"The file number %lu specified in the %s object lacks the \'fileName\' field so it will be ignored!\n"
					,i,aux_str[a]);
				continue;
			}
			const char* file_name_str = json_object_get_string(file_name_obj);
			struct json_object *fault_injection_obj;
			if(!json_object_object_get_ex(aux_file,"faultInjection",&fault_injection_obj)){
				SPDK_ERRLOG(
					"The file number %lu specified in the %s object lacks the \'faultInjection\' field so it will be ignored!\n"
					,i,aux_str[a]);
				continue;
			}
			struct json_object *fault_injection_frequency;
			if(!json_object_object_get_ex(fault_injection_obj,"frequency",&fault_injection_frequency)){
				SPDK_ERRLOG(
					"The file number %lu specified in the %s object lacks the \'frequency\' field inside the faultInjection object so it will be ignored!\n"
					,i,aux_str[a]);
				continue;
			}
			const char* frequency_str = json_object_get_string(fault_injection_frequency);
			if(strcmp(frequency_str,"ALL")==0) file_faults->fault_freq = ALL;
			else if(strcmp(frequency_str,"ALL_AFTER")==0) file_faults->fault_freq = ALL_AFTER;
			else if(strcmp(frequency_str,"INTERVAL")==0) file_faults->fault_freq = INTERVAL;
			else{
				SPDK_ERRLOG(
					"The file number %lu specified in the %s object has the \'frequency\' field inside the faultInjection object wrongly defined, so it will be ignored!\n"
					,i,aux_str[a]);
				continue;
			}

			struct json_object *fault_type_obj;
			if(!json_object_object_get_ex(fault_injection_obj,"faultType",&fault_type_obj)){
				SPDK_ERRLOG(
					"The file number %lu specified in the %s object lacks the \'faultType\' field inside the faultInjection object so it will be ignored!\n"
					,i,aux_str[a]);
				continue;
			}
			
			struct json_object *type_obj;
			if(!json_object_object_get_ex(fault_type_obj,"type",&type_obj)){
				SPDK_ERRLOG(
					"The file number %lu specified in the %s object lacks the \'type\' field inside the \'faultType\' object so it will be ignored!\n"
					,i, aux_str[a]);
				continue;
			}
			const char* type_str = json_object_get_string(type_obj);

			
			if(strcmp(type_str,"CORRUPT_CONTENT")==0) file_faults->fault_type = CORRUPT_CONTENT;
			else if(strcmp(type_str,"DELAY_OPERATION")==0) file_faults->fault_type = DELAY_OPERATION;
			else if(strcmp(type_str,"MEDIUM_ERROR")==0) file_faults->fault_type = MEDIUM_ERROR;
			else{
					SPDK_ERRLOG(
						"The file number %lu specified in the %s object has the \'type\' field inside the faultType object wrongly defined, so it will be ignored!\n"
						,i, aux_str[a]);
					continue;
				}
			

			if(file_faults->fault_type==CORRUPT_CONTENT){

				struct json_object *corruption_obj;
				if(!json_object_object_get_ex(fault_type_obj,"corruptionPattern",&corruption_obj)){
					SPDK_ERRLOG(
						"The file number %lu specified in the %s object lacks the \'corruptionPattern\' field inside the \'faultType\' object that must be defined when the \'type\' is defined as \'CORRUPT_CONTENT\' so it will be ignored!\n"
						,i, aux_str[a]);
					continue;
				}
				const char* corruption_str = json_object_get_string(corruption_obj);
				
				if(strcmp(corruption_str,"REPLACE_ALL_ZEROS")==0) file_faults->u.content_corruption.pattern = REPLACE_ALL_ZEROS;
				else if(strcmp(corruption_str,"REPLACE_ALL_ONES")==0) file_faults->u.content_corruption.pattern = REPLACE_ALL_ONES;
				else if(strcmp(corruption_str,"BITFLIP_RANDOM_INDEX")==0) file_faults->u.content_corruption.pattern = BITFLIP_RANDOM_INDEX;
				else if(strcmp(corruption_str,"BITFLIP_CUSTOM_INDEX")==0) file_faults->u.content_corruption.pattern = BITFLIP_CUSTOM_INDEX;
				else{
					SPDK_ERRLOG(
						"The file number %lu specified in the %s object has the \'corruptionPattern\' field inside the faultType object wrongly defined, so it will be ignored!\n"
						,i, aux_str[a]);
					continue;
				}
				
				struct json_object *offset_obj;
				struct json_object *index_obj;
				
				switch (file_faults->u.content_corruption.pattern)
				{
					case BITFLIP_CUSTOM_INDEX:

						if(!json_object_object_get_ex(fault_type_obj,"offset",&offset_obj)){
						SPDK_WARNLOG(
							"The file number %lu specified in the %s object lacks the \'offset\' field inside the \'faultType\' object which should be defined when the field type is \'BITFLIP_CUSTOM_INDEX\' so it will be set to the default 0!\n"
							,i, aux_str[a]);
						}
						else{
							if(json_object_is_type(offset_obj,json_type_int)){
								file_faults->u.content_corruption.customOffset = json_object_get_int(offset_obj);
								if (file_faults->u.content_corruption.customOffset<0){
									SPDK_WARNLOG(
										"The file number %lu specified in the %s object must have the \'offset\' field >=0 so it will be set to the default 0!\n"
										,i, aux_str[a]);							
									file_faults->u.content_corruption.customOffset=0;
								}
							}
							else SPDK_WARNLOG(
										"The file number %lu specified in the %s object must have the \'offset\' defined has an integer. It will be set to the default 0!\n"
										,i, aux_str[a]);					
						}	

					
						if(!json_object_object_get_ex(fault_type_obj,"index",&index_obj)){
						SPDK_WARNLOG(
							"The file number %lu specified in the %s object lacks the \'index\' field inside the \'faultType\' object which should be defined when the field type is \'BITFLIP_CUSTOM_INDEX\' so it will be set to the default 0!\n"
							,i, aux_str[a]);
						}
						else{
							if(json_object_is_type(index_obj,json_type_int)){
								file_faults->u.content_corruption.customIndex = json_object_get_int(index_obj);
								if (file_faults->u.content_corruption.customIndex<0 || file_faults->u.content_corruption.customIndex>=8){
									SPDK_WARNLOG(
										"The file number %lu specified in the %s object must have the \'index\' field >=0 and <8 so it will be set to the default 0!\n"
										,i, aux_str[a]);							
									file_faults->u.content_corruption.customIndex=0;
								}
							}
							else SPDK_WARNLOG(
										"The file number %lu specified in the %s object must have the \'index\' defined has an integer. It will be set to the default 0!\n"
										,i, aux_str[a]);					
						}
						break;
					default:
						break;
				}
			}

			else if(file_faults->fault_type==DELAY_OPERATION){
				struct json_object *time_obj;
				if(!json_object_object_get_ex(fault_type_obj,"time",&time_obj)){
					SPDK_WARNLOG(
						"The file number %lu specified in the %s lacks the \'time\' field inside the \'faultType\' object which should be defined when the field type is \'DELAY_OPERATION\' so it will be set to the default 0!\n"
						,i,aux_str[a]);
				}
				else{
					if(json_object_is_type(time_obj,json_type_double)){
							file_faults->u.delay_time = json_object_get_double(time_obj);
							if(file_faults->u.delay_time<0){
								SPDK_WARNLOG(
									"The file number %lu specified in the %s must have the \'time\' field >=0 so it will be set to the default 0!\n"
									,i,aux_str[a]);
								file_faults->u.delay_time = 0;
							}
						}
						else SPDK_WARNLOG(
									"The file number %lu specified in the %s must have the \'time\' defined has a double. It will be set to the default 0!\n"
									,i,aux_str[a]);
				}
			}

			else if(file_faults->fault_type==MEDIUM_ERROR){
				struct json_object *err_obj;
				if(!json_object_object_get_ex(fault_type_obj,"error",&err_obj)){
					SPDK_WARNLOG(
						"The file number %lu specified in the %s lacks the \'error\' field inside the \'faultType\' object which should be defined when the field type is \'MEDIUM_ERROR\' so it will be set to the default 0!\n"
						,i,aux_str[a]);
				}
				else{
					if(json_object_is_type(err_obj,json_type_int)){
							file_faults->u.error_number = json_object_get_int(err_obj);
						}
						else SPDK_WARNLOG(
									"The file number %lu specified in the %s must have the \'error\' field defined has an integer. It will be set to the default 0!\n"
									,i,aux_str[a]);
				}
			}


			file_faults->n_requests = 1;
			struct json_object *nRequests_obj;
			switch (file_faults->fault_freq)
			{
				case ALL: 
					break;
				case ALL_AFTER:
				case INTERVAL:
					
					if(!json_object_object_get_ex(fault_injection_obj,"nRequests",&nRequests_obj)){
					SPDK_WARNLOG(
						"The file number %lu specified in the %s lacks the \'nRequests\' field inside the \'faultType\' object which should be defined when the field frequency is \'ALL_AFTER\' or \'INTERVAL\' so it will be set to the default 1!\n"
						,i, aux_str[a]);
					}
					else{
						if(json_object_is_type(nRequests_obj,json_type_int)){
							file_faults->n_requests = json_object_get_int(nRequests_obj);
							if(file_faults->n_requests<=0){
								SPDK_WARNLOG(
									"The file number %lu specified in the %s must have the \'nRequests\' field >0 so it will be set to the default 1!\n"
									,i, aux_str[a]);
								file_faults->n_requests = 1;
							}
						}
						else SPDK_WARNLOG(
									"The file number %lu specified in the %s must have the \'nRequests\' defined has an integer. It will be set to the default 1!\n"
									,i, aux_str[a]);
					}
					break;
			
				default:
					break;
			}

			file_faults->total_requests=0;
			file_faults->req_lock = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
			pthread_mutex_init(file_faults->req_lock,NULL);

			g_hash_table_insert(aux_hash[a],strdup(file_name_str),file_faults);
		
		}
	}
}


static int 
vbdev_read_conf_file(void)
{
	FILE *confFile = fopen("/home/gsd/rocksdb-spdk/spdk/module/bdev/faultBdev/configFile.json","r");

	if(!confFile){
		SPDK_ERRLOG("Something went wrong opening configFile.json!!\n");
		return -1;
	}
	else SPDK_NOTICELOG("Config file opened successfully\n");

	size_t size;

	char* data = (char*) spdk_posix_file_load(confFile,&size);

	fclose(confFile);

	if(!data){
		SPDK_ERRLOG("Something went wrong reading configFile.json!!\n");
		return -1;
	}

	struct json_object *complete_json = json_tokener_parse(data);

	struct json_object *write, *read;

	if(json_object_object_get_ex(complete_json,"write",&write)){
		SPDK_NOTICELOG("Write loaded successfully!\n");
	}
	else SPDK_ERRLOG("Write not loaded!\n");

	if(json_object_object_get_ex(complete_json,"read",&read)){
		SPDK_NOTICELOG("Read loaded successfully!\n");
	}
	else SPDK_ERRLOG("Read not loaded!\n");

	_load_wr_files(write,read);

	//printf("\n------------------WRITE HASHTABLE----------------\n");
	//_print_hashtable(write_hashtable);

	//printf("\n------------------READ HASHTABLE----------------\n");
	//_print_hashtable(read_hashtable);

	return 0;
}

static void
corrupt_request_file(struct spdk_bdev_io *bdev_io,char* fileName){
	struct spdk_file_faults *fault = NULL;
	if (bdev_io->type == SPDK_BDEV_IO_TYPE_WRITE) fault = g_hash_table_lookup(write_hashtable, fileName);
	else if (bdev_io->type == SPDK_BDEV_IO_TYPE_READ) fault =  g_hash_table_lookup(read_hashtable, fileName);
	else return;

	if (!fault) return;

	pthread_mutex_lock(fault->req_lock);
	int current_total_requests = fault->total_requests++;
	pthread_mutex_unlock(fault->req_lock);

	//If freq is all_after x and total requests is not bigger than x then do not corrupt
	if(fault->fault_freq == ALL_AFTER && current_total_requests<=fault->n_requests) return;

	//If freq is interval of x and total requests can not be divided by x then do not corrupt
	if(fault->fault_freq == INTERVAL && ((current_total_requests%fault->n_requests)!=0 || current_total_requests==0)) return;
	switch (fault->fault_type)
	{
	case CORRUPT_CONTENT:
		corrupt_buffer(bdev_io->u.bdev.iovs->iov_base,
					   bdev_io->u.bdev.iovs->iov_len,
					   fault->u.content_corruption.pattern,
					   fault->u.content_corruption.customOffset,
					   fault->u.content_corruption.customIndex);
		break;
	case DELAY_OPERATION:
		operation_delay(fault->u.delay_time);
		break;
	case MEDIUM_ERROR:
		//TODO
		break;
	default:
		break;
	}
}


SPDK_LOG_REGISTER_COMPONENT(vbdev_faulty)