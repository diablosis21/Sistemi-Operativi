#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#define CANDIDATES_QUEUE_CAPACITY 10
#define SCARTI_QUEUE_CAPACITY 5

typedef struct 
{
    u_int8_t data[15];
    u_int8_t checksum;
    u_int8_t calculated_cs;
    unsigned int file_id;
    unsigned long record_num;

} queue_item_t;

typedef struct 
{
    u_int32_t dichiaarti;
    unsigned long integri;
    unsigned long corrotti;
    unsigned long totali;

}file_stats_t;

typedef struct 
{
    queue_item_t *items;
    unsigned long capacity;
    unsigned long head;
    unsigned long tail;
    unsigned long count;

    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
    unsigned int active_producers;

} safe_queue_t;

typedef struct 
{
    safe_queue_t candidates_queue;
    safe_queue_t scartati_queue;
    file_stats_t *stats;
    char **filenames;
    unsigned int num_files;
    pthread_mutex_t stats_lock;

} shared_context_t;

typedef struct 
{
    unsigned int file_idx;
    shared_context_t *shared;

}reader_arg_t;

typedef struct 
{
    unsigned int verifier_id;
    shared_context_t *shared;

}verifier_arg_t;

void exit_with_sys(char *s)
{
    perror(s);
    exit(EXIT_FAILURE);
}

void exit_with_sys_msg(char *s, int e)
{
    fprintf(stderr, "%s :%s", s, strerror(e));
    exit(EXIT_FAILURE);
}

void queue_init(safe_queue_t *q, unsigned long capacity, unsigned int producers)
{
    assert(q != NULL); 
    assert(capacity >0);
    assert(producers >0);
    int err;

    q->capacity = capacity;
    q->active_producers = producers;
    q->head = 0;
    q->tail = 0;
    q->count = 0;

    q->items = malloc(capacity * sizeof(queue_item_t));
    if(q->items == NULL)
        exit_with_sys("malloc queue items");

    if((err = pthread_mutex_init(&q->mutex, NULL)))
        exit_with_sys_msg("pthread_mutex_init", err);

    if((err = pthread_cond_init(&q->not_full, NULL)))
        exit_with_sys_msg("pthread_cond_init not_full", err);

    if((err = pthread_cond_init(&q->not_empty, NULL)))
        exit_with_sys_msg("pthread_cond_init not_empty", err);
}

void queue_destroy(safe_queue_t *q)
{
    assert(q!= NULL);
    int err;

    free(q->items);
    q->items = NULL;


    if((err = pthread_mutex_destroy(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_destroy", err);

    if((err = pthread_cond_destroy(&q->not_full)))
        exit_with_sys_msg("pthread_cond_destroy not_full", err);

    if((err = pthread_cond_destroy(&q->not_empty)))
        exit_with_sys_msg("pthread_cond_destroy not_empty", err);
}

void queue_push(safe_queue_t *q, queue_item_t item)
{
    assert(q!= NULL);
    int err;

    if((err = pthread_mutex_lock(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_lock", err);

    while(q->count == q->capacity)
    {
        if((err = pthread_cond_wait(&q->not_full, &q->mutex)))
            exit_with_sys_msg("pthread_cond_wait", err);
    }

    q->items[q->tail] = item;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

    if(q->count == 1)
    {
        if((err = pthread_cond_broadcast(&q->not_empty)))
            exit_with_sys_msg("pthread_cond_broadcast",err);
    }

    if((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_unlock", err);
}

bool queue_pop(safe_queue_t *q, queue_item_t *item_out)
{
    assert(q != NULL);
    assert(item_out != NULL);
    int err;
    bool estratto = false;

    if((err = pthread_mutex_lock(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_lock", err);

    while(q->count == 0 && q->active_producers > 0)
    {
        if((err = pthread_cond_wait(&q->not_empty, &q->mutex)))
             exit_with_sys_msg("pthread_cond_wait", err);
    }

    if(q->count == 0 && q->active_producers == 0)
        estratto = false;
    else
    {
        *item_out = q->items[q->head];
        q->head = (q->head +1) % q->capacity;
        q->count--;
        estratto = true;

        if((err = pthread_cond_broadcast(&q->not_full)))
            exit_with_sys_msg("pthread_cond_brodcast", err);
    }

    if((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_unlock", err);

    return estratto;
}

void queue_producer_done(safe_queue_t *q)
{
    assert(q != NULL);
    int err;

    if((err = pthread_mutex_lock(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_lock", err);

    q->active_producers--;

    if((err = pthread_cond_broadcast(&q->not_empty)))
        exit_with_sys_msg("pthread_cond_broadcast", err);

    if ((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_unlock", err);
}

void *reader_thread(void *arg)
{
    assert(arg != NULL);
    reader_arg_t *r_arg = (reader_arg_t *) arg;
    shared_context_t *shared = r_arg->shared;
    unsigned int file_idx = r_arg->file_idx;
    const char *filename = shared->filenames[file_idx];


    int fd = open(filename, O_RDONLY);
    if(fd == -1)
        exit_with_sys("open reader");

    struct stat sb;
    if(fstat(fd,&sb))
        exit_with_sys("fstat");

    size_t filesize = sb.st_size;
    if( filesize < 4)
    {
        fprintf(stderr, "File %s troppo piccolo per contenere un header valido\n", filename);
        close(fd);
        exit(EXIT_FAILURE);
    }

    unsigned long total_records = (filesize - 4) / 16;
    shared->stats[file_idx].totali = total_records;

    uint8_t *mapped = mmap(NULL,filesize,PROT_READ,MAP_PRIVATE,fd,0);

    if(mapped == MAP_FAILED)
        exit_with_sys("mmap");

    uint32_t dichiarati = *(uint32_t *) mapped;
    shared->stats[file_idx].dichiaarti = dichiarati;


    printf("[READER-%u] file '%s': %lu record totali, %u dichiarati integri\n",file_idx +1, filename, total_records, dichiarati);

    for(unsigned long i=0; i<total_records; i++)
    {
        unsigned long offset = 4 + (i * 16);
        uint8_t *record_ptr = mapped + offset;

        printf("READER-%u record candidato n.%lu (offset %lu)\n", file_idx+1, i+1, offset);

        queue_item_t item;
        memcpy(item.data,record_ptr,15);
        item.checksum = record_ptr[15];
        item.file_id = file_idx;
        item.record_num = i + 1;
        item.calculated_cs = 0;

        queue_push(&shared->candidates_queue, item); 
    }

    if(munmap(mapped,filesize) == -1)
            exit_with_sys("munmao");

    close(fd);

    printf("[READER-%u] terminazione con %lu record letti\n", file_idx + 1, total_records);
    queue_producer_done(&shared->candidates_queue);

    return NULL;
}

void *verifier_thread(void *arg)
{
    assert(arg != NULL);
    verifier_arg_t* v_arg = (verifier_arg_t*) arg;
    shared_context_t *shared = v_arg->shared;
    unsigned int v_id = v_arg->verifier_id;

    unsigned long verified_count = 0;
    queue_item_t item;
    int err;

    while(queue_pop(&shared->candidates_queue, &item))
    {
        verified_count++;

        unsigned int sum = 0;
        for(int i=0; i<15; i++)
        {
            sum += item.data[i];
        }

        uint8_t calculated_cs = (uint8_t) (sum %256);
        item.calculated_cs = calculated_cs;

        bool is_valid = (calculated_cs == item.checksum);
        const char *filename = shared->filenames[item.file_id];


        printf("[VERIF-%u] record n.%lu del file '%s': checksum dichiarato %u, calcolato %u -> %s\n",v_id+1, item.record_num, filename, item.checksum, calculated_cs, is_valid ? "integro" : "corrotto");

        if((err = pthread_mutex_lock(&shared->stats_lock)))
            exit_with_sys_msg("pthread_mutex_lock stats", err);

        if(is_valid)
            shared->stats[item.file_id].integri++;
        else
            shared->stats[item.file_id].corrotti++;

        if((err = pthread_mutex_unlock(&shared->stats_lock)))
            exit_with_sys_msg("pthread_mutex_unlock stats", err);

        if(!is_valid)
        {
            queue_push(&shared->scartati_queue, item);
        }
    }

    printf("[VERIF-%u] terminazione con %lu record verificati\n", v_id, verified_count);
    queue_producer_done(&shared->scartati_queue);

    return NULL;
}

int main(int argc, char *argv[])
{
    if(argc <3)
    {
        fprintf(stderr, "Uso: %s <M-verifiers> <file-bin-1> [file-bin-2 ...]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int M = atoi(argv[1]);
    if(M<0)
    {
        fprintf(stderr, "Numero di verificatori non valido\n");
        exit(EXIT_FAILURE);
    }

    unsigned int N = argc - 2;
    char **file_list = &argv[2];

    printf("[MAIN] creazione di %u thread lettori e %d thread verificatori\n", N, M);

    shared_context_t shared;
    shared.num_files = N;
    shared.filenames = file_list;
    shared.stats = calloc(N, sizeof(file_stats_t));

    if(shared.stats == NULL)
        exit_with_sys("calloc stats");

    int err;
    if((err = pthread_mutex_init(&shared.stats_lock,NULL)))
        exit_with_sys_msg("pthread_mutex_init stats", err);

    queue_init(&shared.candidates_queue, CANDIDATES_QUEUE_CAPACITY, N);
    queue_init(&shared.scartati_queue, SCARTI_QUEUE_CAPACITY, M);

    pthread_t reader_tids[N];
    reader_arg_t reader_args[N];
    for(unsigned int i=0; i<N; i++)
    {
        reader_args[i].file_idx = i;
        reader_args[i].shared = &shared;
        if((err = pthread_create(&reader_tids[i],NULL,reader_thread,&reader_args[i])))    
            exit_with_sys_msg("pthread_create reader", err);
    }

    pthread_t verifier_tids[M];
    verifier_arg_t verifier_arg[M];
    for(unsigned int j=0; j<M; j++)
    {
        verifier_arg[j].verifier_id = j;
        verifier_arg[j].shared = &shared;
        if((err = pthread_create(&verifier_tids[j],NULL,verifier_thread,&verifier_arg[j])))    
            exit_with_sys_msg("pthread_create reader", err);
    }

    queue_item_t bad_item;
    while(queue_pop(&shared.scartati_queue, &bad_item))
    {
        printf("[MAIN] record corrotto: n.%lu del file '%s' (checksum dichiarato %u, calcolato %u)\n",bad_item.record_num, shared.filenames[bad_item.file_id], bad_item.checksum, bad_item.calculated_cs);
    }

    for (unsigned int i = 0; i < N; i++) {
        if ((err = pthread_join(reader_tids[i], NULL)))
            exit_with_sys_msg("pthread_join reader", err);
    }

    for (int j = 0; j < M; j++) {
        if ((err = pthread_join(verifier_tids[j], NULL)))
            exit_with_sys_msg("pthread_join verifier", err);
    }

    unsigned int superate = 0;

    for (unsigned int i = 0; i < N; i++) 
    {
        file_stats_t *s = &shared.stats[i];
        const char *fn = shared.filenames[i];

        if (s->integri == s->dichiaarti)
        {
            superate++;
            printf("[MAIN] file '%s': %lu integri e %lu corrotti su %lu record -> verifica automatica superata (dichiarati %u)\n", fn, s->integri, s->corrotti, s->totali, s->dichiaarti);
        } 
        else 
        {
            printf("[MAIN] file '%s': %lu integri e %lu corrotti su %lu record -> verifica automatica fallita (dichiarati %u, rilevati %lu)\n", fn, s->integri, s->corrotti, s->totali, s->dichiaarti, s->integri);
        }
    }

    printf("[MAIN] verifiche superate: %u/%u\n", superate, N);
    printf("[MAIN] terminazione\n");

    queue_destroy(&shared.candidates_queue);
    queue_destroy(&shared.scartati_queue);
    pthread_mutex_destroy(&shared.stats_lock);
    free(shared.stats);

    return 0;


}