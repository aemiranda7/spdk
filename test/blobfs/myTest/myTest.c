#include "spdk/stdinc.h"

#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/blobfs.h"
#include "spdk/blobfs_bdev.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/blob_bdev.h"
#include "spdk/thread.h"
//#include "blobfs/blobfs.c"

static uint64_t g_cluster_size;

uint32_t g_lcore = -1;
//static bool g_thread_exit = false;

struct blobfs_operation_ctx {
	struct spdk_app_opts              *opts;
	const char                   *bdev_name;
	struct spdk_filesystem              *fs;
	int                             fserrno;
	struct spdk_bs_dev              *bs_dev;
	struct spdk_fs_thread_ctx *sync_channel;
	struct spdk_io_channel   *async_channel;
	struct spdk_thread      *working_thread;
	struct spdk_thread      *main_thread;
	struct spdk_file        *test_file;
	


	pthread_t                     thread_id;
	volatile bool                     ready;
	volatile bool                            working;
};



static void poll_threads(struct blobfs_operation_ctx* ctx){
	while(spdk_thread_poll( ctx->main_thread, 0, 0)>0);
    if( ctx->working_thread )
        while(spdk_thread_poll(  ctx->working_thread, 0, 0)>0);
}



static void
__call_fn(void *arg1, void *arg2)
{
	fs_request_fn fn;

	fn = (fs_request_fn)arg1;
	fn(arg2);
}

static void
__send_request(fs_request_fn fn, void *arg)
{
	struct spdk_event *event;

	event = spdk_event_allocate(g_lcore, __call_fn, (void *)fn, arg);
	spdk_event_call(event);
}

static void
send_request(fs_request_fn fn, void *arg)
{
	printf("Sending req....\n");
	spdk_thread_send_msg(spdk_get_thread(), (spdk_msg_fn)fn, arg);
	//fn(arg);
	printf("Send request exits....\n");
}

static void
blobfs_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		     void *event_ctx)
{
	SPDK_WARNLOG("Async event(%d) is triggered in bdev %s\n", type, spdk_bdev_get_name(bdev));
}


static void
_blobfs_unload_cb(void *_ctx, int fserrno)
{
	struct blobfs_operation_ctx *ctx = _ctx;

	printf("finished unloading!\n");

	if (fserrno) {
		SPDK_ERRLOG("Failed to unload blobfs on bdev %s: errno %d\n", ctx->bdev_name, fserrno);
	}

	ctx->ready=true;
}


static void
blobfs_unload(void *_ctx)
{
	struct blobfs_operation_ctx *ctx = _ctx;

	if(ctx->working_thread){

		spdk_set_thread(ctx->working_thread);

		//if(ctx->sync_channel!=NULL){
		//	spdk_fs_free_thread_ctx(ctx->sync_channel);
		//}

		if(ctx->async_channel!=NULL){
			spdk_fs_free_io_channel(ctx->async_channel);
		}
	}
	spdk_set_thread(ctx->main_thread);

	ctx->ready = false;

	printf("Ready to unload\n");

	spdk_fs_unload(ctx->fs, _blobfs_unload_cb, ctx);

	while(!ctx->ready){
		spdk_thread_poll(ctx->main_thread,0,0);
		//spdk_thread_poll(ctx->working_thread,0,0);
	}
}


static void write_cb(void*arg,int fserrno){
	struct blobfs_operation_ctx *ctx = arg;
	if(fserrno == 0){
		 printf("write completed!\n");
	//	 spdk_thread_set_file_id(NULL);
	}
	else printf("write failed!\n");
	ctx->ready = true;

}

static void read_cb(void*arg,int fserrno){
	struct blobfs_operation_ctx *ctx = arg;
	if(fserrno == 0) printf("read completed!\n");
	else printf("read failed!\n");
	ctx->ready = true;

}

static void sync_cb(void*arg,int fserrno){
	struct blobfs_operation_ctx *ctx = arg;
	if(fserrno == 0) printf("sync completed!\n");
	else printf("sync failed!\n");
	ctx->ready = true;

}

static void create_cb(void*arg,int fserrno){
	struct blobfs_operation_ctx *ctx = arg;
	if(fserrno == 0) printf("create file completed!\n");
	else printf("create file failed!\n");
	ctx->ready = true;

}

static void open_cb(void*arg,struct spdk_file*f,int fserrno){
	struct blobfs_operation_ctx *ctx = arg;
	ctx->test_file = f;
	if(fserrno == 0) printf("open file completed!\n");
	else printf("open file failed!\n");
	//ctx->ready = true;

}

static void close_cb(void*arg,int fserrno){
	struct blobfs_operation_ctx *ctx = arg;
	if(fserrno == 0) printf("close completed!\n");
	else printf("close failed!\n");
	ctx->ready = true;

}

static void stat_cb(void*arg,struct spdk_file_stat* stats,int fserrno){
	struct blobfs_operation_ctx *ctx = arg;
	if(fserrno == 0){
		 printf("stats completed!\n");
		 printf("Size: %ld\nBlobId: %ld\n",stats->size,stats->blobid);
	}
	else printf("stats failed!\n");
	ctx->ready = true;

}


static void
fs_op_with_handle_complete(void *arg, struct spdk_filesystem *fs, int fserrno)
{

	struct blobfs_operation_ctx *ctx = arg;

	ctx->fs = fs;
	ctx->fserrno = fserrno;

	if(fserrno==0){ 
		
		SPDK_NOTICELOG("\nPreencheu fs!\n");

		if(fs!=NULL) printf("fs preenchido\n");
		else printf("fs null\n");
	}
	else { 
		SPDK_WARNLOG("\nError creating fs!\n");
		//blobfs_unload
		//spdk_app_stop(-1);
		return;

	}

	// create spdk thread here
    if( ctx->working_thread == NULL )
    {
    	printf( "creating spdk thread\n" );
    	ctx->working_thread = spdk_thread_create( "spdk_working_thread", NULL );
		spdk_set_thread( ctx->working_thread );
    }

	//spdk_set_thread( ctx->working_thread );
        
	ctx->sync_channel = spdk_fs_alloc_thread_ctx(ctx->fs);

	ctx->async_channel = spdk_fs_alloc_io_channel(ctx->fs);

	poll_threads(ctx);

	if(ctx->async_channel!=NULL) printf("\nCHANNEL CREATED\n");
	else printf("CHANNEL NOT CREATED\n");

	sleep(2);

	ctx->ready=false;

	printf("CREATING FILE\n");
	spdk_fs_create_file(fs,ctx->sync_channel,"testfile");

	//while(!ctx->ready) poll_threads(ctx);

	printf("Opening FILE\n");
	spdk_fs_open_file_async(fs,"testfile",SPDK_BLOBFS_OPEN_CREATE,open_cb,ctx);
	while(ctx->test_file==NULL) poll_threads(ctx);

	ctx->ready=false;
	printf("WRITING TO FILE\n");
	//struct bs_file_id *file_id = malloc(sizeof(struct bs_file_id *));
	//file_id->file_name = spdk_file_get_name(ctx->test_file);
	//spdk_thread_set_file_id(file_id);
	spdk_file_write_async(ctx->test_file,ctx->async_channel,"OLA MUNDO\n",0,10,write_cb,ctx);
	while(!ctx->ready) poll_threads(ctx);

	ctx->ready = false;
	spdk_file_sync_async(ctx->test_file,ctx->async_channel,sync_cb,ctx);
	while(!ctx->ready) poll_threads(ctx);

	ctx->ready=false;
	char buf[10];
	spdk_file_read_async(ctx->test_file,ctx->async_channel,buf,0,10,read_cb,ctx);
	while(!ctx->ready) poll_threads(ctx);
	printf("%s\n",buf);

	ctx->ready=false;
	spdk_file_close_async(ctx->test_file,close_cb,ctx);
	while(!ctx->ready) poll_threads(ctx);


	ctx->ready=false;
	struct spdk_file_stat * stats = calloc(1,sizeof(*stats));
	spdk_fs_file_stat_async(fs,"testfile",stat_cb,ctx);
	while(!ctx->ready) poll_threads(ctx);

	ctx->working = false;
}




static void fs_init(void* arg){
	struct blobfs_operation_ctx *ctx = arg;

	struct spdk_blobfs_opts blobfs_opt;
	spdk_fs_opts_init(&blobfs_opt);

	printf("fsinit()\n");

	if(ctx->fs==NULL){
		g_lcore = spdk_env_get_first_core();
		spdk_fs_init(ctx->bs_dev, &blobfs_opt, send_request, fs_op_with_handle_complete, ctx);
		printf("\ng_lcore = %d\n",g_lcore);
	}

}

static void initialize_spdk_ready(void *arg1)
{
    struct blobfs_operation_ctx *ctx = arg1;
    
    printf( "initialize_spdk_cb\n" );

    int rc = spdk_bdev_create_bs_dev_ext(ctx->bdev_name, blobfs_bdev_event_cb, NULL, &ctx->bs_dev);
    if (rc!=0) {
        printf("Could not create blob bdev, name=%s!!\n", 
			ctx->bdev_name);
        spdk_app_stop(-1);
        ctx->working = false;
        return;
    }

    ctx->ready = true;
}

static void *
initialize_spdk(void *arg)
{
	struct blobfs_operation_ctx *ctx = arg;
    printf( "initialize_spdk()\n" );
    struct spdk_app_opts *opts = ctx->opts;
    int rc; 
    
    rc = spdk_app_start(opts, initialize_spdk_ready, ctx);
    if( rc )
    {
        printf( "initialize_spdk() fails\n" );
    }
    printf( "initialize_spdk() exits\n" );
    pthread_exit( NULL );
}


static void
mkfs_usage(void)
{
	printf(" -C <size>                 cluster size\n");
}

static int
mkfs_parse_arg(int ch, char *arg)
{
	bool has_prefix;

	switch (ch) {
	case 'C':
		spdk_parse_capacity(arg, &g_cluster_size, &has_prefix);
		break;
	default:
		return -EINVAL;
	}
	return 0;
}

int main(int argc, char **argv)
{

	struct spdk_app_opts opts = {};
	
	int rc = 0;

	if (argc < 3) {
		SPDK_ERRLOG("usage: %s <conffile> <bdevname>\n", argv[0]);
		exit(1);
	}

	spdk_app_opts_init(&opts, sizeof(opts));
	opts.name = "blobfs_test";
	opts.json_config_file = argv[1];
	opts.reactor_mask = "0x1";
	opts.tpoint_group_mask = "0x80";
	opts.shutdown_cb = NULL;

	spdk_fs_set_cache_size(4096);

    struct blobfs_operation_ctx *ctx = calloc(1, sizeof(*ctx));
	if (ctx == NULL) {
		SPDK_ERRLOG("Failed to allocate ctx.\n");

		return -ENOMEM;
	}
	ctx->bdev_name = argv[2];
	if ((rc = spdk_app_parse_args(argc, argv, &opts, "C:", NULL,
				      mkfs_parse_arg, mkfs_usage)) !=
	    SPDK_APP_PARSE_ARGS_SUCCESS) {
		exit(rc);
	}

	ctx->ready = false;
    ctx->opts = &opts;
	ctx->fs = NULL;
	ctx->test_file = NULL;
	ctx->working = true; //to begin the work

    pthread_attr_t attr;
    
    pthread_attr_init( &attr );
    pthread_attr_setstacksize( &attr, 256* 1024 );

    pthread_create( &ctx->thread_id, &attr, &initialize_spdk, ctx );

    while( !ctx->ready ){}

	//const char * nameThread = spdk_thread_get_name(spdk_get_thread());
	printf("NThreads: %d\n",spdk_thread_get_count());

	struct spdk_thread* main_thread = NULL;
	main_thread = spdk_thread_create( "spdk main thread", NULL );
	ctx->main_thread = main_thread;
    spdk_set_thread( main_thread );

	spdk_thread_send_msg( main_thread, fs_init, ctx);

	struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 1000000L;

	sleep(1);
	poll_threads(ctx);
	
    while( ctx->working )
    {		
        nanosleep( &ts, NULL );
		poll_threads(ctx);
    }

	blobfs_unload(ctx);

	spdk_set_thread(ctx->working_thread);
	spdk_thread_exit(ctx->working_thread);
	while (!spdk_thread_is_exited(ctx->working_thread)) {
		spdk_thread_poll(ctx->working_thread, 0, 0);
	}
	spdk_thread_destroy(ctx->working_thread);

	spdk_set_thread(ctx->main_thread);

	spdk_thread_exit(ctx->main_thread);
	while (!spdk_thread_is_exited(ctx->main_thread)) {
		spdk_thread_poll(ctx->main_thread, 0, 0);
	}
	spdk_thread_destroy(ctx->main_thread);
	
	spdk_app_stop(0);

	pthread_join(ctx->thread_id,NULL);

	free(ctx);

	spdk_app_fini();



	return 0;
}