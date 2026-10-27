#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <semaphore.h>
#include <pthread.h>
#include <unistd.h>

#define CANDIDATES_CAPACITY 10
#define WINNERS_CAPACITY 5

typedef enum
{
    PREMIO_NESSUNO = 0,
    PREMIO_QUARTO = 3,
    PREMIO_TERZO = 4,
    PREMIO_SECONDO = 5,
    PREMIO_JACKPOT = 6

}prize_category_t;


typedef struct 
{
    char ticket_id[24];
    int numbers[6];
    prize_category_t prize;
    const char *filename;
    int matches;
    bool is_poison_pill;

}ticket_record_t;

typedef struct 
{
    ticket_record_t *items;
    unsigned long capacity;
    unsigned long head;
    unsigned long tail;

    pthread_mutex_t mutex;
    sem_t empty_slots;
    sem_t full_slots;

}safe_queue_t;

typedef struct 
{
    safe_queue_t candidates_queue;
    safe_queue_t winners_queue;
    int winning_numbers[6];
    char **ticket_files;
    unsigned int num_files;
    unsigned int num_checkers;

    unsigned long total_verified;
    pthread_mutex_t stats_mutex;

}shared_context_t;

typedef struct 
{
    unsigned int id;
    const char *filepath;
    shared_context_t *shared;
}reader_arg_t;

typedef struct {
    unsigned int id;           
    shared_context_t *shared;
} checker_arg_t;

typedef struct {
    unsigned int N;
    pthread_t *reader_tids;
    shared_context_t *shared;
} feeder_watcher_arg_t;


void exit_with_sys(char *s)
{
    perror(s);
    exit(EXIT_FAILURE);
}

void exit_with_sys_msg(char *s, int e)
{
    fprintf(stderr,"%s :%s", s, strerror(e));
    exit(EXIT_FAILURE);
}

const char *prize_to_string(prize_category_t p) {
    switch (p) {
        case PREMIO_JACKPOT: return "jackpot!";
        case PREMIO_SECONDO: return "secondo premio";
        case PREMIO_TERZO:   return "terzo premio";
        case PREMIO_QUARTO:  return "quarto premio";
        default:             return "nessun premio";
    }
}

void queue_init(safe_queue_t *q, unsigned long capacity)
{
    assert(q != NULL);
    assert(capacity >0);
    int err;

    q->capacity = capacity;
    q->head = 0;
    q->tail = 0;

    q->items = malloc(capacity * sizeof(ticket_record_t));
    if(q->items == NULL)
        exit_with_sys("malloc queue items");

    if((err = pthread_mutex_init(&q->mutex, NULL)))
        exit_with_sys_msg("pthread mutex init", err);

    if(sem_init(&q->empty_slots, 0, capacity) == -1)
        exit_with_sys("sem_init empty_slots");

    if(sem_init(&q->full_slots, 0, 0) == -1)
        exit_with_sys("sem_init full_slots");
    
}

void queue_destroy(safe_queue_t *q)
{
    assert(q != NULL);
    int err;
    
    free(q->items);
    
    if((err = pthread_mutex_destroy(&q->mutex)))
        exit_with_sys_msg("pthread mutex destroy", err);

    if(sem_destroy(&q->empty_slots) == -1)
        exit_with_sys("sem_destroy empty_slots");

    if(sem_destroy(&q->full_slots) == -1)
        exit_with_sys("sem_destroy full_slots");

}

void queue_push(safe_queue_t *q, ticket_record_t item)
{
    assert(q != NULL);
    int err;

    if(sem_wait(&q->empty_slots) == -1)
        exit_with_sys("sem_wait empty_slots");

    if((err = pthread_mutex_lock(&q->mutex)))
         exit_with_sys_msg("pthread mutex lock", err);

    q->items[q->tail] = item;
    q->tail = (q->tail + 1) % q->capacity;

    if((err = pthread_mutex_unlock(&q->mutex)))
         exit_with_sys_msg("pthread mutex unlock", err);

    if(sem_post(&q->full_slots) == -1)
        exit_with_sys("sem_post full_slots");

    
}

void queue_pop(safe_queue_t *q, ticket_record_t *item)
{
    assert(q != NULL);
    int err;

    if(sem_wait(&q->full_slots) == -1)
        exit_with_sys("sem_wait full_slots");

    if((err = pthread_mutex_lock(&q->mutex)))
         exit_with_sys_msg("pthread mutex lock", err);

    *item = q->items[q->head];
    q->head = (q->head + 1) % q->capacity;

    if((err = pthread_mutex_unlock(&q->mutex)))
         exit_with_sys_msg("pthread mutex unlock", err);

    if(sem_post(&q->empty_slots) == -1)
        exit_with_sys("sem_post empty_slots");
}

void load_winning_numbers(const char *filepath, int win[6])
{
    FILE*fp = fopen(filepath,"r");
    if(fp == NULL)
        exit_with_sys("fopen winning-file");

    char line[128];
    if(fgets(line,sizeof(line),fp) == NULL)
    {
        fprintf(stderr,"Errore: winning-file vuoto\n");
        fclose(fp);
        exit(EXIT_FAILURE);
    }

    fclose(fp);

    int count = sscanf(line, "%d,%d,%d,%d,%d,%d", &win[0], &win[1], &win[2], &win[3], &win[4], &win[5]);

    if(count !=6)
    {
        fprintf(stderr, "Formato numeri vincenti errato nel file %s\n", filepath);
        exit(EXIT_FAILURE);
    }
}

void *reader_thread(void *arg)
{
    assert(arg != NULL);
    reader_arg_t *r_arg = (reader_arg_t*) arg;
    shared_context_t *shared = r_arg->shared;

    flockfile(stdout);
    printf("[READER-%u] file '%s'\n", r_arg->id, r_arg->filepath);
    funlockfile(stdout);

    FILE *fp = fopen(r_arg->filepath,"r");
    if(!fp)
        exit_with_sys("fopen ticket file");

    char line[256];
    unsigned long ticket_count = 0; 

    while(fgets(line,sizeof(line),fp) != NULL)
    {
        if(line[0] == '\n' || line[0] =='\r' || line[0] == '\0')
            continue;


        ticket_record_t item;
        memset(&item, 0, sizeof(item));
        item.filename = r_arg->filepath;
        item.is_poison_pill = false;

        int parsed = sscanf(line, "%20[^;];%d,%d,%d,%d,%d,%d",item.ticket_id, &item.numbers[0], &item.numbers[1], &item.numbers[2], &item.numbers[3], &item.numbers[4], &item.numbers[5]);
    
        if(parsed != 7)
            continue;

        ticket_count++;

        flockfile(stdout);
        printf("[READER-%u] biglietto candidato n.%lu: %s (%d, %d, %d, %d, %d, %d)\n",
               r_arg->id, ticket_count, item.ticket_id,
               item.numbers[0], item.numbers[1], item.numbers[2],
               item.numbers[3], item.numbers[4], item.numbers[5]);
        funlockfile(stdout);

        queue_push(&shared->candidates_queue, item);
    }

    fclose(fp);

    flockfile(stdout);
    printf("[READER-%u] terminazione con %lu biglietti letti\n", r_arg->id, ticket_count);
    funlockfile(stdout);

    return NULL;
}

void *checker_thread(void *arg)
{
    assert(arg != NULL);
    checker_arg_t *c_arg = (checker_arg_t *)arg;
    shared_context_t *shared = c_arg->shared;

    unsigned long verified_count =0;
    int err;

    while(true)
    {
        ticket_record_t item;
        queue_pop(&shared->candidates_queue, &item);

        if(item.is_poison_pill)
            break;
        
        verified_count++;

        int matches = 0;
        for(int i=0; i<6; i++)
        {
            for(int j=0; j<6; j++)
            {
                if(item.numbers[i] == shared->winning_numbers[j])
                {
                    matches++;
                    break;
                }
            }
        }

        item.matches = matches;


        if(matches == 6)
            item.prize = PREMIO_JACKPOT;
        else if (matches == 5)
            item.prize = PREMIO_SECONDO;
        else if (matches == 4)
            item.prize = PREMIO_TERZO;
        else if (matches == 3)
            item.prize = PREMIO_QUARTO;
        else
            item.prize = PREMIO_NESSUNO;


        flockfile(stdout);
        printf("[CHECK-%u] verifico biglietto %s: %d numeri indovinati su 6\t%s\n", c_arg->id, item.ticket_id, matches, prize_to_string(item.prize));
        funlockfile(stdout);

        if(matches >=3)
            queue_push(&shared->winners_queue, item);

    }

    if ((err = pthread_mutex_lock(&shared->stats_mutex)))
        exit_with_sys_msg("pthread_mutex_lock", err);

    shared->total_verified += verified_count;

    if ((err = pthread_mutex_unlock(&shared->stats_mutex)))
        exit_with_sys_msg("pthread_mutex_unlock", err);

    flockfile(stdout);
    printf("[CHECK-%u] terminazione con %lu biglietti verificati\n", c_arg->id, verified_count);
    funlockfile(stdout);
    
    ticket_record_t pill;
    memset(&pill, 0, sizeof(pill));
    pill.is_poison_pill = true;
    queue_push(&shared->winners_queue, pill);

    return NULL;
}


void *feeder_watcher_thread(void *arg) {
    feeder_watcher_arg_t *w = (feeder_watcher_arg_t *)arg;
    int err;

    
    for (unsigned int i = 0; i < w->N; i++) {
        if ((err = pthread_join(w->reader_tids[i], NULL)))
            exit_with_sys_msg("pthread_join reader in watcher", err);
    }

    
    ticket_record_t pill;
    memset(&pill, 0, sizeof(pill));
    pill.is_poison_pill = true;

    for (unsigned int j = 0; j < w->shared->num_checkers; j++) {
        queue_push(&w->shared->candidates_queue, pill);
    }

    return NULL;
}


int main(int argc, char *argv[])
{
    if(argc < 4)
    {
        fprintf(stderr, "Uso: %s <winning-file> <M-checkers> <ticket-file-1> [ticket-file-2 ...]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *winning_file = argv[1];
    int M = atoi(argv[2]);
    if(M<=0)
    {
        fprintf(stderr, "Errore: M-checkers deve essere > 0\n");
        exit(EXIT_FAILURE);
    }

    unsigned int N = argc - 3;
    char **ticket_files = &argv[3];

    shared_context_t shared;
    shared.num_files = N;
    shared.num_checkers = M;
    shared.ticket_files = ticket_files;
    shared.total_verified = 0;

    int err;
    if ((err = pthread_mutex_init(&shared.stats_mutex, NULL)))
        exit_with_sys_msg("pthread_mutex_init stats", err);

    load_winning_numbers(winning_file, shared.winning_numbers);

    queue_init(&shared.candidates_queue, CANDIDATES_CAPACITY);
    queue_init(&shared.winners_queue, WINNERS_CAPACITY);


    printf("[MAIN] creazione di %u thread lettori e %d thread verificatori\n", N, M);
    printf("[MAIN] numeri vincenti: %d, %d, %d, %d, %d, %d\n",
           shared.winning_numbers[0], shared.winning_numbers[1],
           shared.winning_numbers[2], shared.winning_numbers[3],
           shared.winning_numbers[4], shared.winning_numbers[5]);


    pthread_t reader_tids[N];
    reader_arg_t reader_args[N];
    for (unsigned int i = 0; i < N; i++) {
        reader_args[i].id = i + 1;
        reader_args[i].filepath = ticket_files[i];
        reader_args[i].shared = &shared;
        if ((err = pthread_create(&reader_tids[i], NULL, reader_thread, &reader_args[i])))
            exit_with_sys_msg("pthread_create reader", err);
    }


    pthread_t checker_tids[M];
    checker_arg_t checker_args[M];
    for (int j = 0; j < M; j++) {
        checker_args[j].id = j + 1;
        checker_args[j].shared = &shared;
        if ((err = pthread_create(&checker_tids[j], NULL, checker_thread, &checker_args[j])))
            exit_with_sys_msg("pthread_create checker", err);
    }

    pthread_t watcher_tid;
    feeder_watcher_arg_t watcher_arg = {
        .N = N,
        .reader_tids = reader_tids,
        .shared = &shared
    };
    if ((err = pthread_create(&watcher_tid, NULL, feeder_watcher_thread, &watcher_arg)))
        exit_with_sys_msg("pthread_create watcher", err);

   
    unsigned long jackpot_count = 0;
    unsigned long secondo_count = 0;
    unsigned long terzo_count = 0;
    unsigned long quarto_count = 0;
    unsigned int finished_checkers = 0;

    while (finished_checkers < (unsigned int)M) {
        ticket_record_t win_item;
        queue_pop(&shared.winners_queue, &win_item);

        if (win_item.is_poison_pill) {
            finished_checkers++;
            continue;
        }

      
        switch (win_item.prize) {
            case PREMIO_JACKPOT: jackpot_count++; break;
            case PREMIO_SECONDO: secondo_count++; break;
            case PREMIO_TERZO:   terzo_count++; break;
            case PREMIO_QUARTO:  quarto_count++; break;
            default: break;
        }

        
        flockfile(stdout);
        printf("[MAIN] biglietto vincente %s: %d numeri indovinati\t%s\n",
               win_item.ticket_id, win_item.matches, prize_to_string(win_item.prize));
        funlockfile(stdout);
    }

    
    for (int j = 0; j < M; j++) {
        if ((err = pthread_join(checker_tids[j], NULL)))
            exit_with_sys_msg("pthread_join checker", err);
    }
    if ((err = pthread_join(watcher_tid, NULL)))
        exit_with_sys_msg("pthread_join watcher", err);

    
    unsigned long total_winners = jackpot_count + secondo_count + terzo_count + quarto_count;

    printf("[MAIN] riepilogo finale:\n");
    printf("[MAIN] jackpot: %lu biglietto/i\n", jackpot_count);
    printf("[MAIN] secondo premio: %lu biglietto/i\n", secondo_count);
    printf("[MAIN] terzo premio: %lu biglietto/i\n", terzo_count);
    printf("[MAIN] quarto premio: %lu biglietto/i\n", quarto_count);
    printf("[MAIN] totale: %lu biglietti vincenti su %lu verificati\n",  total_winners, shared.total_verified);
    printf("[MAIN] terminazione\n");

    queue_destroy(&shared.candidates_queue);
    queue_destroy(&shared.winners_queue);
    pthread_mutex_destroy(&shared.stats_mutex);

    return 0;
    
}