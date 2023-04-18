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

//-------------------------------MD5 Implementation--------------------------------
/*typedef union uwb {
	unsigned w;
	unsigned char b[4];
} MD5union;

typedef unsigned DigestArray[4];

unsigned func0(unsigned abcd[]) {
	return (abcd[1] & abcd[2]) | (~abcd[1] & abcd[3]);
}

unsigned func1(unsigned abcd[]) {
	return (abcd[3] & abcd[1]) | (~abcd[3] & abcd[2]);
}

unsigned func2(unsigned abcd[]) {
	return  abcd[1] ^ abcd[2] ^ abcd[3];
}

unsigned func3(unsigned abcd[]) {
	return abcd[2] ^ (abcd[1] | ~abcd[3]);
}

typedef unsigned(*DgstFctn)(unsigned a[]);

unsigned *calctable(unsigned *k)
{
	double s, pwr;
	int i;

	pwr = pow(2.0, 32);
	for (i = 0; i<64; i++) {
		s = fabs(sin(1.0 + i));
		k[i] = (unsigned)(s * pwr);
	}
	return k;
}

unsigned rol(unsigned r, short N)
{
	unsigned  mask1 = (1 << N) - 1;
	return ((r >> (32 - N)) & mask1) | ((r << N) & ~mask1);
}

unsigned* Algorithms_Hash_MD5(const char *msg, int mlen)
{
	static DigestArray h0 = { 0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476 };
	static DgstFctn ff[] = { &func0, &func1, &func2, &func3 };
	static short M[] = { 1, 5, 3, 7 };
	static short O[] = { 0, 1, 5, 0 };
	static short rot0[] = { 7, 12, 17, 22 };
	static short rot1[] = { 5, 9, 14, 20 };
	static short rot2[] = { 4, 11, 16, 23 };
	static short rot3[] = { 6, 10, 15, 21 };
	static short *rots[] = { rot0, rot1, rot2, rot3 };
	static unsigned kspace[64];
	static unsigned *k;

	static DigestArray h;
	DigestArray abcd;
	DgstFctn fctn;
	short m, o, g;
	unsigned f;
	short *rotn;
	union {
		unsigned w[16];
		char     b[64];
	}mm;
	int os = 0;
	int grp, grps, q, p;
	unsigned char *msg2;

	if (k == NULL) k = calctable(kspace);

	for (q = 0; q<4; q++) h[q] = h0[q];

	{
		grps = 1 + (mlen + 8) / 64;
		msg2 = (unsigned char*)malloc(64 * grps);
		memcpy(msg2, msg, mlen);
		msg2[mlen] = (unsigned char)0x80;
		q = mlen + 1;
		while (q < 64 * grps) { msg2[q] = 0; q++; }
		{
			MD5union u;
			u.w = 8 * mlen;
			q -= 8;
			memcpy(msg2 + q, &u.w, 4);
		}
	}

	for (grp = 0; grp<grps; grp++)
	{
		memcpy(mm.b, msg2 + os, 64);
		for (q = 0; q<4; q++) abcd[q] = h[q];
		for (p = 0; p<4; p++) {
			fctn = ff[p];
			rotn = rots[p];
			m = M[p]; o = O[p];
			for (q = 0; q<16; q++) {
				g = (m*q + o) % 16;
				f = abcd[1] + rol(abcd[0] + fctn(abcd) + k[q + 16 * p] + mm.w[g], rotn[q % 4]);

				abcd[0] = abcd[3];
				abcd[3] = abcd[2];
				abcd[2] = abcd[1];
				abcd[1] = f;
			}
		}
		for (p = 0; p<4; p++)
			h[p] += abcd[p];
		os += 64;
	}
	return h;
}

const char* GetMD5String(const char *msg, int mlen) {
	char* str = malloc(33*sizeof(char));
	strcpy(str, "");
	int j, k;
	unsigned *d = Algorithms_Hash_MD5(msg, strlen(msg));
	MD5union u;
	for (j = 0; j<4; j++) {
		u.w = d[j];
		char* s[8];
		sprintf(s, "%02x%02x%02x%02x", u.b[0], u.b[1], u.b[2], u.b[3]);
		strcat(str, s);
	}

	return str;
}*/
//-------------------------End of MD5 implementation---------------------------------





static uint64_t g_cluster_size;

uint32_t g_lcore = -1;
//static bool g_thread_exit = false;

size_t bs = 4096;

struct blobfs_operation_ctx {
	struct spdk_app_opts              *opts;
	const char                   *bdev_name;
	struct spdk_filesystem              *fs;
	int                             fserrno;
	struct spdk_bs_dev              *bs_dev;
	struct spdk_fs_thread_ctx *sync_channel;
	struct spdk_io_channel   *async_channel;
	struct spdk_thread      *working_thread;
	struct spdk_thread         *main_thread;
	struct spdk_file             *test_file;
	
	int bionic_fd;
	int  focal_fd;      
	int  jammy_fd;

	size_t bionic_size;
	size_t  focal_size;
	size_t  jammy_size;

	pthread_t                     thread_id;
	volatile bool                     ready;
	volatile bool                   working;
};

char* bionic_path = "/home/gsd/blobfs_data/bionic-server-cloudimg-amd64.img";
char* new_bionic_path = "/home/gsd/blobfs_data/bionic-server-cloudimg-amd64.spdk.img";
char* focal_path = "/home/gsd/blobfs_data/focal-server-cloudimg-amd64.img";
char* jammy_path = "/home/gsd/blobfs_data/jammy-server-cloudimg-amd64.img";


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
		 //printf("write completed!\n");
	//	 spdk_thread_set_file_id(NULL);
	}
	else ;//printf("write failed!\n");
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
	if(fserrno == 0){} //printf("sync completed!\n");
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

	struct blobfs_operation_ctx *ctx = arg;

	ctx->fs = fs;
	ctx->fserrno = fserrno;

	//open the files
	ctx->bionic_fd = open(bionic_path,O_RDONLY);
	//ctx->focal_fd = open(focal_path,O_RDONLY);
	//ctx->jammy_fd = open(jammy_path,O_RDONLY);

	//find out the size of the files
	ctx->bionic_size = lseek(ctx->bionic_fd, 0, SEEK_END); lseek(ctx->bionic_fd, 0, SEEK_SET);
	//ctx->focal_size  = lseek(ctx->focal_fd, 0,  SEEK_END); lseek(ctx->focal_fd, 0,  SEEK_SET);
	//ctx->jammy_size  = lseek(ctx->jammy_fd, 0,  SEEK_END); lseek(ctx->jammy_fd, 0,  SEEK_SET);


	size_t align = spdk_bdev_get_buf_align(ctx->bs_dev->get_base_bdev(ctx->bs_dev));

	//allocate the buffers and fill them with the files
	//void* bionic_payload = spdk_dma_zmalloc(bs, align, NULL);// read(ctx->bionic_fd, bionic_payload, ctx->bionic_size);
	//void* focal_payload  = spdk_dma_zmalloc(ctx->focal_size,  align, NULL); read(ctx->focal_fd,   focal_payload,  ctx->focal_size);
	//void* jammy_payload  = spdk_dma_zmalloc(ctx->jammy_size,  align, NULL); read(ctx->jammy_fd,   jammy_payload,  ctx->jammy_size);

	

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

	//spdk_fs_free_io_channel(ctx->async_channel);

	//while(!ctx->ready) poll_threads(ctx);

	printf("Opening FILE\n");
	spdk_fs_open_file_async(fs,"testfile",SPDK_BLOBFS_OPEN_CREATE,open_cb,ctx);
	while(ctx->test_file==NULL) poll_threads(ctx);

	
	ctx->ready=false;
	printf("WRITING TO FILE\n");
	struct bs_file_id *file_id = malloc(sizeof(struct bs_file_id));
	file_id->file_name = strdup(spdk_file_get_name(ctx->test_file));
	
	
	for(uint64_t off = 0;off<=ctx->bionic_size;off+=bs){
		uint64_t s = bs;
		if(off+bs>ctx->bionic_size) s = ctx->bionic_size - (off+1);
		void* bionic_payload = spdk_dma_zmalloc(bs, align, NULL);
		if(read(ctx->bionic_fd, bionic_payload, s)<=0) printf("READ ERROR : %lu",off);
		//ctx->async_channel = spdk_fs_alloc_io_channel(ctx->fs);
		spdk_file_write_async(ctx->test_file,ctx->async_channel,bionic_payload,off,s,write_cb,ctx);
		while(!ctx->ready) poll_threads(ctx);
		ctx->ready = false;
		spdk_file_sync_async(ctx->test_file,ctx->async_channel,sync_cb,ctx);
		while(!ctx->ready) poll_threads(ctx);
		ctx->ready = false;
		spdk_file_sync_async(ctx->test_file,ctx->async_channel,sync_cb,ctx);
		while(!ctx->ready) poll_threads(ctx);
		//spdk_fs_free_io_channel(ctx->async_channel);

		spdk_dma_free(bionic_payload);
	}

	


	void* bionic_payload_spdk = spdk_dma_zmalloc(ctx->bionic_size, align, NULL);
	
	ctx->ready=false;
	//char* buf = spdk_dma_zmalloc(2, align, NULL);
	file_id->file_name = strdup(spdk_file_get_name(ctx->test_file));
	spdk_file_read_async_fid(ctx->test_file,ctx->async_channel,bionic_payload_spdk,0,ctx->bionic_size,file_id,read_cb,ctx);
	while(!ctx->ready) poll_threads(ctx);
	//printf("%s\n",buf);

	int bionic_spdk_fd = open(new_bionic_path,O_CREAT|O_WRONLY,0777);

	write(bionic_spdk_fd,bionic_payload_spdk,ctx->bionic_size);

	/*char* nValue = GetMD5String(buf,700);

	if(strcmp(value,nValue)){
		SPDK_NOTICELOG("The value is not the same!!!\n");
		printf("value: %s\n",value);
		printf("value: %s\n",nValue);

	} */

	//write(ctx->focal_fd,buf,2);
	
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

    while( !ctx->ready ){
		
	}

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