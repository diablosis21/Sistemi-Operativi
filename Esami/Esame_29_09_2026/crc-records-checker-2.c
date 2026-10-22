#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>

#define QUEUE_CAPACITY 10

void err_with_sys(char *s)
{
    perror(s);
    exit(EXIT_FAILURE);
}

void err_with_sys_msg(char *s, int e)
{
    fprintf(stderr,"%s: %s\n",s,strerror(e));
    exit(EXIT_FAILURE);
}

typedef struct {
    uint8_t data[15];
    uint8_t checksum;
    unsigned int file_id;      
    unsigned long record_num;  
} queue_item_t;


typedef struct {
    uint32_t dichiarati;
    unsigned long integri;
    unsigned long corrotti;
} file_stats_t;


typedef struct {
    queue_item_t items[QUEUE_CAPACITY];
    unsigned long head;
    unsigned long tail;
    unsigned long count;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
    unsigned int active_readers; 
} safe_queue_t;


typedef struct {
    safe_queue_t queue;
    file_stats_t *stats;        
    char **filenames;           
    unsigned int num_files;
    pthread_mutex_t stats_lock;  
} shared_context_t;

typedef struct{
    unsigned int file_idx;
    shared_context_t *shared;
} reader_arg_t;

typedef struct
{
    unsigned int verifier_id;
    shared_context_t *shared;
} verifier_arg_t;

void queue_init(safe_queue_t *q, unsigned int num_readers)
{
    assert(q != NULL);
    assert(num_readers >0);

    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->active_readers = num_readers;

    int err;


    if((err = pthread_mutex_init(&q->mutex, NULL)))
        err_with_sys_msg("pthread_mutex_init",err);

    if((err = pthread_cond_init(&q->not_full, NULL)))
        err_with_sys_msg("pthread_cond_init",err);

    if((err = pthread_cond_init(&q->not_empty, NULL)))
        err_with_sys_msg("pthread_cond_init",err);
}

void queue_destroy(safe_queue_t *q)
{
    assert(q != NULL);

    int err;

    if((err = pthread_mutex_destroy(&q->mutex)))
        err_with_sys_msg("pthread_mutex_destroy",err);

    if((err = pthread_cond_destroy(&q->not_full)))
        err_with_sys_msg("pthread_cond_destroy",err);

    if((err = pthread_cond_destroy(&q->not_empty)))
        err_with_sys_msg("pthread_cond_destroy",err);
}


void queue_push(safe_queue_t *q, queue_item_t item)
{
    assert(q != NULL);
    int err;

    if((err = pthread_mutex_lock(&q->mutex)))
        err_with_sys_msg("pthread_mutex_lock", err);

    while(q->count == QUEUE_CAPACITY)
    {
        if((err = pthread_cond_wait(&q->not_full, &q->mutex)))
            err_with_sys_msg("pthread_cond_wait", err);
    }

    q->items[q->tail] = item;
    q->tail = (q->tail + 1) % QUEUE_CAPACITY;
    q->count++;

    if(q->count == 1)
    {
        if((err = pthread_cond_signal(&q->not_empty)))
            err_with_sys_msg("pthread_cond_signal", err);
    }

    if((err = pthread_mutex_unlock(&q->mutex)))
        err_with_sys_msg("pthread_mutex_unlock", err);
    
}

bool queue_pop(safe_queue_t *q, queue_item_t *item)
{
    assert(q != NULL);
    assert(item != NULL);
    int err;
    bool estratto = false;

    if((err = pthread_mutex_lock(&q->mutex)))
        err_with_sys_msg("pthread_mutex_lock", err);

    while(q->count == 0 && q->active_readers > 0)
    {   
        if(err = pthread_cond_wait(&q->not_empty, &q->mutex))
            err_with_sys_msg("pthread_cond_wait", err);
    }

    if(q->count == 0 && q->active_readers == 0)
        estratto = false;
    else
    {
        *item = q->items[q->head];
        q->head = (q->head + 1) % QUEUE_CAPACITY;
        q->count--;
        estratto = true;

        if((err = pthread_cond_signal(&q->not_full)))
            err_with_sys_msg("pthread_cond_signal", err);
    }

    if((err = pthread_mutex_unlock(&q->mutex)))
        err_with_sys_msg("pthread_mutex_unlock", err);
    
    return estratto;
}

void queue_reader_done(safe_queue_t *q)
{
    assert(q != NULL);
    int err;

    if((err = pthread_mutex_lock(&q->mutex)))
        err_with_sys_msg("pthread_mutex_lock",err);
    
    q->active_readers--;
    
    if ((err = pthread_cond_signal(&q->not_empty)))
        err_with_sys_msg("pthread_cond_signal", err);

    if ((err = pthread_mutex_unlock(&q->mutex)))
        err_with_sys_msg("pthread_mutex_unlock", err);

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
        err_with_sys("open");

    u_int32_t dichiarati = 0;
    ssize_t bytes_read = read(fd,&dichiarati,sizeof(u_int32_t));
    if(bytes_read != sizeof(uint32_t))
    {
        fprintf(stderr,"Errore lettura header nel file %s\n",filename);
        close(fd);
        exit(EXIT_FAILURE);
    }

    shared->stats[file_idx].dichiarati = dichiarati;

    printf("[READER-%u] file '%s' : %u record dichiarati integri\n", file_idx + 1, filename, dichiarati);

    unsigned long record_num = 0;
    unsigned long offset = sizeof(u_int32_t);
    u_int8_t buffer[16];

    while((bytes_read = read(fd, buffer, 16)) == 16)
    {
        record_num++;

        printf("[READER-%u] record candidato n.%lu (offset %lu)\n",file_idx +1,record_num, offset);

        queue_item_t item;
        memcpy(item.data,buffer,15);
        item.checksum = buffer[15];
        item.file_id = file_idx;
        item.record_num = record_num;

        queue_push(&shared->queue, item);
        offset += 16;
    }

    close(fd);

    printf("[READER-%u] terminazione con %lu record letti\n", file_idx + 1, record_num);

    queue_reader_done(&shared->queue);

    return NULL;
}

void *verifier_thread(void *arg)
{
    assert(arg != NULL);
    verifier_arg_t *v_arg = (verifier_arg_t *) arg;
    shared_context_t *shared = v_arg->shared;
    unsigned int v_id = v_arg->verifier_id;

    unsigned long verified_count = 0;
    queue_item_t item;
    int err;

    while (queue_pop(&shared->queue, &item))
    {
        verified_count++;

        uint8_t calculated_cheksum = 0;

        for(int i=0; i<15; i++)
        {
            calculated_cheksum ^= item.data[i];
        }

        bool is_valid = (calculated_cheksum == item.checksum);
        const char *filename = shared->filenames[item.file_id];

        printf("[VERIF-%u] record n.%lu del file '%s': checksum dichiarato %u, calcolato %u -> %s\n", v_id, verified_count, filename, item.checksum, calculated_cheksum, is_valid ? "integro" : "corrotto");
    
        if((err = pthread_mutex_lock(&shared->stats_lock)))
            err_with_sys_msg("pthread_mutex_lock", err);

        if(is_valid)
            shared->stats[item.file_id].integri++;
        else
            shared->stats[item.file_id].corrotti++;

         if((err = pthread_mutex_unlock(&shared->stats_lock)))
            err_with_sys_msg("pthread_mutex_unlock", err);
    }


    printf("[VERIF-%u] terminazione con %lu record verificati\n",v_id, verified_count);

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

    if (M <= 0) {
        fprintf(stderr, "Il numero di verificatori deve essere maggiore di 0\n");
        exit(EXIT_FAILURE);
    }

    unsigned int N = argc - 2;
    char **file_list = &argv[2];

    printf("[MAIN] creazione di %u thread lettori e %d thread verificatori\n", N, M);

    shared_context_t shared;
    shared.num_files = N;
    shared.filenames = file_list;

    shared.stats = calloc(N, sizeof(file_stats_t));
    assert(shared.stats != NULL);

    int err;
    if((err = pthread_mutex_init(&shared.stats_lock, NULL)))
        err_with_sys_msg("pthread_mutex_init", err);

    queue_init(&shared.queue, N);

    pthread_t reader_tids[N];
    reader_arg_t reader_args[N];

    pthread_t verifier_tids[M];
    verifier_arg_t verifier_args[M];

    for(unsigned i=0; i<N; i++)
    {
        reader_args[i].file_idx = i;
        reader_args[i].shared = &shared;
        if((err = pthread_create(&reader_tids[i], NULL, reader_thread, &reader_args[i])))
            err_with_sys_msg("pthread_create reader", err);
    }

    for (int j = 0; j < M; j++) 
    {
        verifier_args[j].verifier_id = j + 1;
        verifier_args[j].shared = &shared;
        if ((err = pthread_create(&verifier_tids[j], NULL, verifier_thread, &verifier_args[j])))
            err_with_sys_msg("pthread_create verifier", err);
    }

    for(unsigned int i=0; i<N; i++)
    {
        if((err = pthread_join(reader_tids[i], NULL)))
            err_with_sys_msg("pthread_join reader", err);
    }

    for (int j = 0; j < M; j++) {
        if ((err = pthread_join(verifier_tids[j], NULL)))
            err_with_sys_msg("pthread_join verifier", err);
    }

    unsigned int verifiche_superate = 0;

    for (unsigned int i = 0; i < N; i++) {
        file_stats_t *s = &shared.stats[i];
        unsigned long totali = s->integri + s->corrotti;
        const char *filename = shared.filenames[i];

        if (s->integri == s->dichiarati) 
        {
            verifiche_superate++;
            printf("[MAIN] file '%s': %lu integri e %lu corrotti su %lu record -> verifica automatica superata (dichiarati %u)\n", filename, s->integri, s->corrotti, totali, s->dichiarati);
        } 
        else 
        {
            printf("[MAIN] file '%s': %lu integri e %lu corrotti su %lu record -> verifica automatica fallita (dichiarati %u, rilevati %lu)\n", filename, s->integri, s->corrotti, totali, s->dichiarati, s->integri);
        }
    }

    printf("[MAIN] verifiche superate: %u/%u\n", verifiche_superate, N);
    printf("[MAIN] terminazione\n");

    // 6. Pulizia delle risorse
    queue_destroy(&shared.queue);
    pthread_mutex_destroy(&shared.stats_lock);
    free(shared.stats);

    return 0;


}