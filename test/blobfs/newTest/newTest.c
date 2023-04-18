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

char* bionic_path = "/home/gsd/blobfs_data/bionic-server-cloudimg-amd64.img";
char* new_bionic_path = "/home/gsd/blobfs_data/bionic-server-cloudimg-amd64-spdk.img";

char* focal_path = "/home/gsd/blobfs_data/focal-server-cloudimg-amd64.img";
char* new_focal_path = "/home/gsd/blobfs_data/focal-server-cloudimg-amd64-spdk.img";

char* jammy_path = "/home/gsd/blobfs_data/jammy-server-cloudimg-amd64.img";
char* new_jammy_path = "/home/gsd/blobfs_data/jammy-server-cloudimg-amd64-spdk.img";

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

struct blobfs_operation_ctx {
	struct spdk_app_opts              *opts;
	const char                   *bdev_name;
	struct spdk_filesystem              *fs;
	int                             fserrno;
	struct spdk_bs_dev              *bs_dev;
	struct spdk_fs_thread_ctx *sync_channel;

	struct spdk_thread      *working_thread;
	struct spdk_thread         *main_thread;
	struct spdk_file             *bionic_file;
	struct spdk_file             *jammy_file;
	struct spdk_file             *focal_file;
	
	pthread_t                     thread_id;
	volatile bool                     ready;
	volatile bool                  fs_error;
	volatile bool                   working;

	int bionic_fd;
	int  jammy_fd;
	int  focal_fd;

	int bionic_new_fd;
	int  jammy_new_fd;
	int  focal_new_fd;

	size_t bionic_size;
	size_t jammy_size;
	size_t focal_size;

	size_t align;
};

size_t bs = 4096;

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
base_bdev_event_cb(enum spdk_bdev_event_type type, __attribute__((unused)) struct spdk_bdev *bdev,
		   __attribute__((unused)) void *event_ctx)
{
	printf("Unsupported bdev event: type %d\n", type);
}

static void
fs_unload_cb(__attribute__((unused)) void *ctx,
	     __attribute__((unused)) int fserrno)
{
	assert(fserrno == 0);

	spdk_app_stop(0);
}

static void
__end_work(struct blobfs_operation_ctx *ctx){
	spdk_fs_free_thread_ctx(ctx->sync_channel);

	spdk_set_thread(ctx->main_thread);

	spdk_thread_exit(ctx->working_thread);

	spdk_thread_destroy(ctx->working_thread);

	spdk_fs_unload(ctx->fs,fs_unload_cb,ctx);
}

static void
__open_files(struct blobfs_operation_ctx *ctx){
	ctx->bionic_fd = open(bionic_path, O_RDONLY);
	ctx->focal_fd  = open(focal_path,  O_RDONLY);
	ctx->jammy_fd  = open(jammy_path,  O_RDONLY);

	ctx->bionic_new_fd = open(new_bionic_path, O_CREAT|O_WRONLY, 0777);
	ctx->focal_new_fd  = open(new_focal_path,  O_CREAT|O_WRONLY, 0777);
	ctx->jammy_new_fd  = open(new_jammy_path,  O_CREAT|O_WRONLY, 0777);
}

static void
__write_one_file(int fd, struct blobfs_operation_ctx *ctx, char* file_name,void *buf, struct spdk_file **file, size_t *size){
	spdk_fs_create_file(ctx->fs,ctx->sync_channel,file_name);
	spdk_fs_open_file(ctx->fs,ctx->sync_channel,file_name,SPDK_BLOBFS_OPEN_CREATE,file);
	*size = lseek(fd, 0, SEEK_END); lseek(fd, 0, SEEK_SET);
	struct bs_file_id *fid = malloc(sizeof(struct bs_file_id));
	fid->file_name=strdup(spdk_file_get_name(*file));
	
	for(uint64_t off=0;off<*size;off+=bs){
		int s = bs;
		if(off+bs>(*size)) s = *size - off;
		read(fd,buf,s);

		spdk_file_write(*file,ctx->sync_channel,buf,off,s);
		spdk_file_sync_fid(*file,ctx->sync_channel,fid);
	}
}

static void
__write_files(struct blobfs_operation_ctx *ctx){
	ctx->sync_channel = spdk_fs_alloc_thread_ctx(ctx->fs);
	ctx->align = spdk_bdev_get_buf_align(ctx->bs_dev->get_base_bdev(ctx->bs_dev));
	void* buf = spdk_dma_zmalloc(bs,ctx->align,NULL);
	
	//------bionic------
	__write_one_file(ctx->bionic_fd,ctx,"bionic",buf, &ctx->bionic_file, &ctx->bionic_size);

	//------jammy------
	__write_one_file(ctx->jammy_fd,ctx,"jammy",buf, &ctx->jammy_file, &ctx->jammy_size);

	//------focal------
	__write_one_file(ctx->focal_fd,ctx,"focal",buf, &ctx->focal_file, &ctx->focal_size);

	spdk_dma_free(buf);
	
}

static void
__read_one_file(int fd, size_t size, void* buf, struct spdk_file *file, struct spdk_fs_thread_ctx *sync_channel){
	printf("\n reading file : %s\n",spdk_file_get_name(file));
	for(uint64_t off = 0;off<=size;off+=bs){
		uint64_t s = bs;
		if(off+bs>size) s = size - off;
		spdk_file_read(file,sync_channel,buf,off,s);
		write(fd,buf,s);
	}
}

static void
__read_files(struct blobfs_operation_ctx *ctx){
	void* bufR = spdk_dma_zmalloc(bs,ctx->align,NULL);

	//------bionic------
	__read_one_file(ctx->bionic_new_fd,ctx->bionic_size,bufR,ctx->bionic_file, ctx->sync_channel);

	//------jammy------
	__read_one_file(ctx->jammy_new_fd,ctx->jammy_size,bufR,ctx->jammy_file, ctx->sync_channel);

	//------focal------
	__read_one_file(ctx->focal_new_fd,ctx->focal_size,bufR,ctx->focal_file, ctx->sync_channel);

	spdk_dma_free(bufR);
	
	spdk_file_close(ctx->bionic_file,ctx->sync_channel);
	spdk_file_close(ctx->jammy_file,ctx->sync_channel);
	spdk_file_close(ctx->focal_file,ctx->sync_channel);
}

static void
work(void *arg)
{
	//size_t size = 1297088512;
	//size_t bs   = 4096;
	struct blobfs_operation_ctx *ctx = arg;

	clock_t start,end;

	__open_files(ctx);
	start = clock();
	__write_files(ctx);
	end = clock();
	float dif = (float)(end-start)/CLOCKS_PER_SEC;
	printf("\n\n execution time = %.2lf\n\n",dif);

	__read_files(ctx);
	

	/*printf("CREATING FILE\n");
	

	printf("Opening FILE\n");
	spdk_fs_open_file(ctx->fs,ctx->sync_channel,"testfile",SPDK_BLOBFS_OPEN_CREATE,&ctx->test_file);
	struct bs_file_id *file_id = malloc(sizeof(struct bs_file_id));
	file_id->file_name = strdup(spdk_file_get_name(ctx->test_file));

	printf("WRITING TO FILE\n");
	for(uint64_t off = 0;off<=size;off+=bs){
		uint64_t s = bs;
		if(off+bs>size) s = size - off;
		//printf("WRITING TO FILE\n");
		//printf("writing %ld bytes",s);
		spdk_file_write(ctx->test_file,ctx->sync_channel,buf,off,s);

		//printf("Syncyng TO FILE\n");
		spdk_file_sync_fid(ctx->test_file,ctx->sync_channel,file_id);
	}
	printf("WRITE COMPLETE\n");

	printf("READING FROM FILE\n");
	for(uint64_t off = 0;off<=size;off+=bs){
		uint64_t s = bs;
		if(off+bs>size) s = size - off;
		spdk_file_read(ctx->test_file,ctx->sync_channel,bufR,off,s);
	}

	printf("CHECKING FILE STAT\n");
	struct spdk_file_stat * stats = calloc(1,sizeof(*stats));
	spdk_fs_file_stat(ctx->fs,ctx->sync_channel,"testfile",stats);
	printf("Size of testfile: %ld\n",stats->size);*/

	//spdk_file_close(ctx->test_file,ctx->sync_channel);

	__end_work(ctx);
}


static void
fs_init_cb(void *arg, struct spdk_filesystem *fs, int fserrno)
{
	struct blobfs_operation_ctx *ctx = arg;
	if(fserrno==0){ 		
		if(fs!=NULL) ctx->fs=fs;
		else printf("fs null\n");
	}
	else { 
		SPDK_WARNLOG("\nError creating fs!\n");
		ctx->fs_error=true;
		return;
	}
	ctx->main_thread = spdk_get_thread();
	ctx->ready = true;


}

static void initialize_spdk_ready(void *arg1)
{
    struct blobfs_operation_ctx *ctx = arg1;
    
    printf( "initialize_spdk_cb\n" );

    int rc = spdk_bdev_create_bs_dev_ext(ctx->bdev_name, base_bdev_event_cb, NULL, &ctx->bs_dev);
    if (rc!=0) {
        printf("Could not create blob bdev, name=%s!!\n", 
			ctx->bdev_name);
        spdk_app_stop(0);
        exit(1);
    }

	g_lcore = spdk_env_get_first_core();

	struct spdk_blobfs_opts blobfs_opt;
	spdk_fs_opts_init(&blobfs_opt);
	spdk_fs_init(ctx->bs_dev, &blobfs_opt, __send_request, fs_init_cb, ctx);
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


/*static void
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
}*/
//------------------------------------------

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

	ctx->ready     = false;
	ctx->fs_error  = false;
    ctx->opts      = &opts;
	ctx->fs        =  NULL;
	
	ctx->working   =  true;

    pthread_attr_t attr;
    
    pthread_attr_init( &attr );
    pthread_attr_setstacksize( &attr, 256* 1024 );

    pthread_create( &ctx->thread_id, &attr, &initialize_spdk, ctx );

    while( !ctx->ready && !ctx->fs_error){
	}
	if(ctx->fs_error){
		SPDK_ERRLOG("Error initiating the fs");
		return -1;
	}
//----------------------------------------------------------------------------
	//const char * nameThread = spdk_thread_get_name(spdk_get_thread());
	printf("NThreads: %d\n",spdk_thread_get_count());

	ctx->working_thread = spdk_thread_create( "spdk_working_thread", NULL );
	if(ctx->working_thread == NULL) printf("Failed To Create Thread\n");
    spdk_set_thread(ctx->working_thread);

	work(ctx);

	pthread_join(ctx->thread_id,NULL);

	free(ctx);

	spdk_app_fini();



	return 0;
}
