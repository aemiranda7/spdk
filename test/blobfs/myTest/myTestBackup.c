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

uint32_t g_lcore = 0;

struct spdk_filesystem *g_fs;
struct spdk_file *g_file;
int g_fserrno;
struct spdk_thread *g_dispatch_thread = NULL;


//static bool g_thread_exit = false;
#




struct ut_request {
	fs_request_fn fn;
	void *arg;
	volatile int done;
};

struct blobfs_operation_ctx {
	struct spdk_app_opts *opts;
	const char *bdev_name;
	struct spdk_filesystem *fs;
	int fserrno;
	struct spdk_bs_dev *bs_dev;

	/* If cb_fn is already called in other function, not _blobfs_bdev_unload_cb.
	 * cb_fn should be set NULL after its being called, in order to avoid repeated
	 * calling in _blobfs_bdev_unload_cb.
	 */
	spdk_blobfs_bdev_op_complete cb_fn;
	void *cb_arg;

	/* Variables for mount operation */
	const char *mountpoint;
	struct spdk_thread *fs_loading_thread;

	/* Used in bdev_event_cb to do some proper operations on blobfs_fuse for
	 * asynchronous event of the backend bdev.
	 */
	struct spdk_blobfs_fuse *bfuse;
	pthread_t                   thread_id;
	volatile bool ready;
};


/*static void *
spdk_thread(void *arg)
{
	struct spdk_thread *thread = arg;

	spdk_set_thread(thread);

	while (!g_thread_exit) {
		spdk_thread_poll(thread, 0, 0);
	}

	return NULL;
}*/




/*static void
ut_call_fn(void *arg)
{
	struct ut_request *req = arg;

	req->fn(req->arg);
	req->done = 1;
}

static void
ut_send_request(fs_request_fn fn, void *arg)
{
	struct ut_request req;

	req.fn = fn;
	req.arg = arg;
	req.done = 0;

	spdk_thread_send_msg(g_dispatch_thread, ut_call_fn, &req);

	// Wait for this to finish 
	while (req.done == 0) {}
}*/

/*
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

	printf("Sending req....\n");

	event = spdk_event_allocate(g_lcore, __call_fn, (void *)fn, arg);
	spdk_event_call(event);

	printf("Sending req....\n");
}*/

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
fs_op_with_handle_complete(void *arg, struct spdk_filesystem *fs, int fserrno)
{

	struct blobfs_operation_ctx *ctx = arg;

	ctx->fs = fs;
	ctx->fserrno = fserrno;

	if(fserrno==0){ 
		
		SPDK_WARNLOG("\nPreencheu fs!\n");

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
    //if( g_dispatch_thread == NULL )
    //{
        printf( "creating spdk thread\n" );
        g_dispatch_thread = spdk_thread_create( "spdk_test_thread", NULL );
        spdk_set_thread( g_dispatch_thread );
    //}
        
	struct spdk_fs_thread_ctx *channel = NULL;
	channel = spdk_fs_alloc_thread_ctx(ctx->fs);

	if(channel!=NULL) printf("\nCHANNEL CREATED\n");
	else printf("CHANNEL NOT CREATED");

	sleep(2);

	int rc = spdk_fs_create_file(fs,channel,"testfile");

	if(rc == 0){
		printf("file created successfully!\n");
	}
	else{
		printf("something went wrong!\n");
	}

	struct spdk_file * file;

	rc = spdk_fs_open_file(fs,channel,"testfile",O_RDWR,&file);

	if(rc==0){
		printf("file opened successfully!\n");
	}
	else{
		printf("something went wrong opening your file!\n");
	}

	rc = spdk_file_write(file,channel,"OLA MUNDO\n",0,10);

	/*struct spdk_file_stat * stats = calloc(1,sizeof(*stats));
	rc = spdk_fs_file_stat(fs,channel,"testfile",stats);

	if(rc==0){
		printf("size = %ld\n",stats->size);
	}
	else{
		printf("failed to get stats\n");
	}*/

	
}

/*static void
fs_thread_poll(void)
{
	struct spdk_thread *thread;

	thread = spdk_get_thread();
	while (spdk_thread_poll(thread, 0, 0) > 0) {}
	//while (spdk_thread_poll(g_cache_pool_thread, 0, 0) > 0) {}
}

static void
_fs_init(void *arg)
{
	struct blobfs_operation_ctx *ctx = arg;
	struct spdk_bs_dev *bs_dev;

	ctx->fs = NULL;
	ctx->fserrno = -1;
	int rc = spdk_bdev_create_bs_dev_ext(ctx->bdev_name, blobfs_bdev_event_cb, NULL, &bs_dev);
	if (rc) {
		SPDK_WARNLOG("\nFailed to create a blobstore block device from bdev (%s)\n",
			     ctx->bdev_name);
	}
	else{SPDK_WARNLOG("\nCreated a blobstore block device in bdev (%s)\n",
			     ctx->bdev_name);}
	struct spdk_blobfs_opts blobfs_opt;
	spdk_fs_opts_init(&blobfs_opt);
	spdk_fs_init(bs_dev, &blobfs_opt, send_request, fs_op_with_handle_complete, ctx);

	//SPDK_WARNLOG("END");
	fs_thread_poll();

	//wait for fs to be created!
	while(ctx->fs==NULL){
		printf("\nNOTCREATED\n");
	}
	printf("\nCREATED\n");


	//SPDK_CU_ASSERT_FATAL(ctx->fs != NULL);
	//SPDK_CU_ASSERT_FATAL(g_fs->bdev == dev);
	//CU_ASSERT(g_fserrno == 0);
}*/

/*
static void
_blobfs_unload_cb(void *_ctx, int fserrno)
{
	struct blobfs_operation_ctx *ctx = _ctx;

	if (fserrno) {
		SPDK_ERRLOG("Failed to unload blobfs on bdev %s: errno %d\n", ctx->bdev_name, fserrno);
	}

	if (ctx->cb_fn) {
		ctx->cb_fn(ctx->cb_arg, fserrno);
	}

	free(ctx);
	SPDK_NOTICELOG("Context freed!\n");

	spdk_app_stop(0);
}


static void
blobfs_unload(void *_ctx)
{
	struct blobfs_operation_ctx *ctx = _ctx;

	spdk_fs_unload(ctx->fs, _blobfs_unload_cb, ctx);
}*/

/*static void
afterInit(void *_ctx, struct spdk_filesystem *fs, int fserrno)
{
	struct blobfs_operation_ctx *ctx = _ctx;

	if (fserrno) {
		ctx->cb_fn(ctx->cb_arg, fserrno);
		free(ctx);
		return;
	}

    SPDK_NOTICELOG("File system created!\n");

	ctx->fs = fs;

	struct spdk_fs_thread_ctx* thread_ctx = spdk_fs_alloc_thread_ctx(ctx->fs);
    SPDK_NOTICELOG("Thread allocated!\n");
	
	int creatFile = spdk_fs_create_file(ctx->fs,thread_ctx,"file1");

	//if(creatFile == 0){
	//	SPDK_NOTICELOG("Created File!\n");
	//}

	spdk_fs_free_thread_ctx(thread_ctx);
	SPDK_NOTICELOG("Thread freed\n");
//finale:
	spdk_thread_send_msg(spdk_get_thread(), blobfs_unload, ctx);

	blobfs_unload(ctx);




	


	//spdk_thread_send_msg(spdk_get_thread(), blobfs_bdev_unload, ctx);
}*/



/*static void
spdk_mkfs_run(void *arg)
{
	//struct spdk_thread *thread = spdk_thread_create("test_thread", NULL);
	//spdk_set_thread(thread);

	pthread_t	spdk_tid;
    g_dispatch_thread = spdk_thread_create("dispatch_thread", NULL);
	pthread_create(&spdk_tid, NULL, spdk_thread, g_dispatch_thread);

	struct spdk_fs_thread_ctx *channel = NULL;
    //SPDK_INFOLOG(blobfs_bdev,"SPDK env intialized!\n");

	struct blobfs_operation_ctx *ctx = arg;
	ut_send_request(_fs_init, ctx);

	while(ctx->fs==NULL){
		printf("\nNOTCREATED\n");
	}
	printf("\nCREATED\n");

	channel = spdk_fs_alloc_thread_ctx(ctx->fs);

	if(channel!=NULL) printf("\nCHANNEL CREATED\n");
	else printf("CHANNEL NOT CREATED");

	int rc = 1;

	rc = spdk_fs_create_file(ctx->fs, channel, "testfile");

	while (rc>1){
		printf("\nNOTDONE\n");
	}

	if(rc == 0){
		printf("\nSUCCESS CREATING FILE!\n");
	}
	else{
		printf("\nERROR CREATING FILE\n");
	}

	struct spdk_file *file = NULL;

	spdk_fs_open_file(ctx->fs,channel,"testfile",O_WRONLY,&file);

	if(file != NULL){
		printf("\nSUCCESS OPENING FILE!\n");
	}
	else{
		printf("\nERROR OPENING FILE\n");
	}

	rc = spdk_file_write(file,channel,"OLA MUNDO!\n",0,11);

	if(rc == 0){
		printf("\nSUCCESS WRITING FILE!\n");
	}
	else{
		printf("\nERROR WRITING FILE\n");
	}




	//SPDK_INFOLOG(blobfs_bdev,"thread allocated!");

	//blobfs_unload(ctx);
	
	
    

	//----------------------
}*/

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

static void fs_init(void* arg){
	struct blobfs_operation_ctx *ctx = arg;

	struct spdk_blobfs_opts blobfs_opt;
	spdk_fs_opts_init(&blobfs_opt);

	

	//struct spdk_thread* mainT = spdk_get_thread();
	//spdk_set_thread(g_dispatch_thread);
	spdk_fs_init(ctx->bs_dev, &blobfs_opt, send_request, fs_op_with_handle_complete, ctx);

}

__attribute__((unused))
static void newExperiment(void* arg){
	struct blobfs_operation_ctx *ctx = arg;
	

	//pthread_t	spdk_tid;
    //g_dispatch_thread = spdk_thread_create("dispatch_thread", NULL);
	//pthread_create(&spdk_tid, NULL, spdk_thread, g_dispatch_thread);

	ctx->fs = NULL;

	
	struct spdk_thread *mainThread;
	mainThread = spdk_thread_create( "spdk main thread", NULL );
    spdk_set_thread( mainThread );

	spdk_thread_send_msg( mainThread, fs_init, ctx);

	printf("AFTERSEND!!\n");
	struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 1000000L;
    

    while( 1 )
    {
		
        int r = spdk_thread_poll( mainThread, 0, 0);
		printf("RESULT = %d\n",r);
        if( g_dispatch_thread )
            spdk_thread_poll(  g_dispatch_thread, 0, 0);

        nanosleep( &ts, NULL );
    }

	//struct spdk_blobfs_opts blobfs_opt;
	//spdk_fs_opts_init(&blobfs_opt);

	

	//struct spdk_thread* mainT = spdk_get_thread();
	//spdk_set_thread(g_dispatch_thread);
	//spdk_fs_init(ctx->bs_dev, &blobfs_opt, send_request, fs_op_with_handle_complete, ctx);
	//spdk_set_thread(mainT);

	//wait until its created
	/*while(ctx->fs==NULL){
		printf("\nNOT CREATED\n");
		sleep(1);
	}*/

	//printf("\nCREATED BIGG\n");



}

static void initialize_spdk_ready(void *arg1)
{
    struct blobfs_operation_ctx *ctx = arg1;
    //struct spdk_bdev *bdev;

    printf( "initialize_spdk_cb\n" );
    /*bdev = spdk_bdev_get_by_name( ctx->bdev_name );
    if (bdev == NULL) {
        printf("%s not found\n", cli_context->bdev_name  );
        cli_context->loop = false;
        return;
    }*/

    //g_lcore = spdk_env_get_first_core();

    int rc = spdk_bdev_create_bs_dev_ext(ctx->bdev_name, blobfs_bdev_event_cb, NULL, &ctx->bs_dev);
    if (rc!=0) {
        printf("Could not create blob bdev, name=%s!!\n", 
			ctx->bdev_name);
        //spdk_app_stop(-1);
        //cli_context->loop = false;
        return;
    }

	g_lcore = spdk_env_get_first_core();


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

    pthread_attr_t attr;
    
    pthread_attr_init( &attr );
    pthread_attr_setstacksize( &attr, 256* 1024 );

    pthread_create( &ctx->thread_id, &attr, &initialize_spdk, ctx );

    while( !ctx->ready )
        ;

	struct spdk_thread *mainThread;
	mainThread = spdk_thread_create( "spdk main thread", NULL );
    spdk_set_thread( mainThread );

	spdk_thread_send_msg( mainThread, fs_init, ctx);

	printf("AFTERSEND!!\n");
	struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 1000000L;
    

    while( 1 )
    {
		
        spdk_thread_poll( mainThread, 0, 0);
		//printf("RESULT = %d\n",r);
        if( g_dispatch_thread )
            spdk_thread_poll(  g_dispatch_thread, 0, 0);

        nanosleep( &ts, NULL );
    }
	//rc = spdk_app_start(&opts, spdk_mkfs_run, ctx);

	//rc = spdk_app_start(&opts, newExperiment, ctx);

	spdk_app_fini();

	return rc;
}