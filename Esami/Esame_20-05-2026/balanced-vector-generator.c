#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>


#define QUEUE_CAPACITY 15
#define NUM_VERIFIERS 2
#define NUM_REPAIRERS 3


typedef struct {
    int *vec;       
    int round;      
} vector_record_t;

// 2. La coda protetta con SEMAFORI NUMERICI
typedef struct {
    vector_record_t items[QUEUE_CAPACITY];
    int head;
    int tail;
    pthread_mutex_t mutex; 
    sem_t empty_slots;     
    sem_t full_slots;     
} safe_queue_t;


typedef struct {
    safe_queue_t proposte;  
    safe_queue_t scartati;  
    
    int N;                  
    int M;                 
    
    int accepted_count;     
    pthread_mutex_t count_mutex; 
    
    bool stop_all;          
} shared_context_t;

typedef struct 
{
    shared_context_t *shared;
} generator_arg_t;

typedef struct
{
    unsigned int id;
    shared_context_t *shared;
} verifier_arg_t;


typedef struct
{
    unsigned int id;
    shared_context_t *shared;

} repairer_arg_t;

void exit_with_sys(char *s)
{
    perror(s);
    exit(EXIT_FAILURE);
}


void exit_with_sys_msg(char *s, int e)
{
    fprintf(stderr, "%s :%s",s, strerror(e));
    exit(EXIT_FAILURE);
} 

void queue_init(safe_queue_t *q) 
{
    assert(q != NULL);
    int err;

    q->head = 0;
    q->tail = 0;

    if ((err = pthread_mutex_init(&q->mutex, NULL)))
        exit_with_sys_msg("pthread_mutex_init", err);

    if (sem_init(&q->empty_slots, 0, QUEUE_CAPACITY) == -1)
        exit_with_sys("sem_init empty_slots");

    if (sem_init(&q->full_slots, 0, 0) == -1)
        exit_with_sys("sem_init full_slots");
}

void queue_destroy(safe_queue_t *q) 
{
    assert(q != NULL);
    int err;

    if ((err = pthread_mutex_destroy(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_destroy", err);

    if (sem_destroy(&q->empty_slots) == -1)
        exit_with_sys("sem_destroy empty_slots");

    if (sem_destroy(&q->full_slots) == -1)
        exit_with_sys("sem_destroy full_slots");
}

void queue_push(safe_queue_t *q, vector_record_t item) 
{

    assert(q != NULL);
    int err;

    if (sem_wait(&q->empty_slots) == -1)
        exit_with_sys("sem_wait empty_slots");

    if ((err = pthread_mutex_lock(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_lock", err);

    q->items[q->tail] = item;
    q->tail = (q->tail + 1) % QUEUE_CAPACITY;

    if ((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_unlock", err);

    if (sem_post(&q->full_slots) == -1)
        exit_with_sys("sem_post full_slots");
}

void queue_pop(safe_queue_t *q, vector_record_t *item_out) {
    assert(q != NULL);
    assert(item_out != NULL);
    int err;

    if (sem_wait(&q->full_slots) == -1)
        exit_with_sys("sem_wait full_slots");

    if ((err = pthread_mutex_lock(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_lock", err);

    *item_out = q->items[q->head];
    q->head = (q->head + 1) % QUEUE_CAPACITY;

    if ((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_sys_msg("pthread_mutex_unlock", err);

    if (sem_post(&q->empty_slots) == -1)
        exit_with_sys("sem_post empty_slots");
}


void *generator_thread(void *arg)
{
    assert(arg != NULL);

    generator_arg_t *g_arg = (generator_arg_t *) arg;
    shared_context_t *shared = g_arg->shared;

    for(int i=0; i<shared->N; i++)
    {
        int *vec= malloc(shared->M * sizeof(int));

        if(vec==NULL)
            exit_with_sys("malloc generator vec");

        for(int j=0; j<shared->M; j++)
        {
            vec[j] = rand() % 100;
        }

        vector_record_t item;
        item.vec = vec;
        item.round = 0;

        flockfile(stdout);
        printf("[GEN] vettore candidato n.%d:", i+1);
        for(int j=0; j<shared->M; j++)
        {
            printf("%d%s", item.vec[j], (j+1 < shared->M ? ", " : ""));
        }
        printf("\n");
        funlockfile(stdout);

        queue_push(&shared->proposte, item);
    }

    return NULL;
}


void *verifier_thread(void *arg)
{
    assert(arg != NULL);
    verifier_arg_t *v_arg = (verifier_arg_t*) arg;
    shared_context_t *shared = v_arg->shared;
    unsigned int id = v_arg->id;
    int err;

    while(true)
    {
        vector_record_t item;
        queue_pop(&shared->proposte, &item);

        if(item.vec == NULL)
        {
            queue_push(&shared->proposte,item);
            break;
        }

        flockfile(stdout);
        printf("[VER-%u] estratto un vettore candidato con round pari a %d: ", id, item.round);
        for(int j=0; j< shared->M; j++)
        {
            printf("%d%s", item.vec[j], (j + 1 < shared->M ? ", " : ""));
        }
        printf("\n");
        funlockfile(stdout);

        int pari = 0;
        int dispari = 0;
        for(int j=0; j< shared->M; j++)
        {
            if(item.vec[j] % 2 == 0)
                pari++;
            else
                dispari++;
        }

        if(pari == shared->M / 2 && dispari == shared->M / 2)
        {
            if((err = pthread_mutex_lock(&shared->count_mutex)))
                exit_with_sys_msg("pthread_mutex_lock count", err);

            shared->accepted_count++;
            int current_accepted = shared->accepted_count;

            printf("[VER-%u] vettore verificato e accettato dopo %d round di aggiustamenti (%d di %d)\n",id, item.round, current_accepted, shared->N);
        
            if(shared->accepted_count == shared->N)
            {
                shared->stop_all = true;

                vector_record_t pillola = { .vec= NULL, .round = -1};

                queue_push(&shared->proposte,pillola);
                queue_push(&shared->scartati, pillola);
            }

            if((err = pthread_mutex_unlock(&shared->count_mutex)))
                exit_with_sys_msg("pthread_mutex_unlock count", err);

            free(item.vec);

            if (current_accepted >= shared->N)
                break;
        }
        else
        {
            printf("[VER-%u] vettore non verificato e rigettato (%d pari vs. %d dispari)\n", id, pari, dispari);
            queue_push(&shared->scartati,item);
        }

    }

    return NULL;
}

void *repairer_thread(void *arg)
{
    assert(arg != NULL);
    repairer_arg_t *r_arg = (repairer_arg_t *) arg;
    shared_context_t *shared = r_arg->shared;
    unsigned int id = r_arg->id;

    while(true)
    {
        vector_record_t item;

        queue_pop(&shared->scartati, &item);

        if(item.vec == NULL)
        {
            queue_push(&shared->scartati, item);
            break;
        }

        flockfile(stdout);
        printf("[REP-%u] estratto un vettore da riparare: ", id);
        for (int j = 0; j < shared->M; j++) {
            printf("%d%s", item.vec[j], (j + 1 < shared->M ? ", " : ""));
        }
        printf("\n");
        funlockfile(stdout);

        item.round++;
        int idx = rand() % shared->M;
        int old_val = item.vec[idx];

        int new_val = rand() % 100;
        item.vec[idx] = new_val;

        flockfile(stdout);
        printf("[REP-%u] reinserito vettore riparato (%d->%d) con round pari a %d: ",
               id, old_val, new_val, item.round);
        for (int j = 0; j < shared->M; j++) {
            printf("%d%s", item.vec[j], (j + 1 < shared->M ? ", " : ""));
        }
        printf("\n");
        funlockfile(stdout);

        queue_push(&shared->proposte, item);
    }

    printf("[REP-%u] terminato\n", id);
    return NULL;
}


int main(int argc, char *argv[])
{
    if(argc !=3)
    {
        fprintf(stderr, "Uso: %s <N> <M>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int N = atoi(argv[1]);
    int M = atoi(argv[2]);

    if (N <= 0 || M <= 0 || M % 2 != 0) {
        fprintf(stderr, "Errore: N deve essere > 0, M deve essere > 0 e PARI!\n");
        exit(EXIT_FAILURE);
    }

    srand(time(NULL));

    printf("[MAIN] creazione di un thread generator, di %d thread verificatore e di %d thread riparatori\n", NUM_VERIFIERS, NUM_REPAIRERS);


    shared_context_t shared;
    shared.N = N;
    shared.M = M;
    shared.accepted_count = 0;
    shared.stop_all = false;

    int err;

    if((err = pthread_mutex_init(&shared.count_mutex,NULL)))
        exit_with_sys_msg("pthread_mutex_init count",err);

    queue_init(&shared.proposte);
    queue_init(&shared.scartati);

    pthread_t gen_tid;
    generator_arg_t gen_arg = { .shared = &shared};
    if((err = pthread_create(&gen_tid,NULL,generator_thread, &gen_arg)))
        exit_with_sys_msg("pthread_create generator", err);

    pthread_t ver_tids[NUM_VERIFIERS];
    verifier_arg_t ver_args[NUM_VERIFIERS];
    for(int i=0; i<NUM_VERIFIERS; i++)
    {
        ver_args[i].id = i +1;
        ver_args[i].shared = &shared;
        if((err = pthread_create(&ver_tids[i],NULL,verifier_thread,&ver_args[i])))
            exit_with_sys_msg("pthread_create verifier",err);
    }

    pthread_t rep_tids[NUM_REPAIRERS];
    repairer_arg_t rep_args[NUM_REPAIRERS];
    for (int i = 0; i < NUM_REPAIRERS; i++) {
        rep_args[i].id = i + 1;
        rep_args[i].shared = &shared;
        if ((err = pthread_create(&rep_tids[i], NULL, repairer_thread, &rep_args[i])))
            exit_with_sys_msg("pthread_create repairer", err);
    }

    if((err = pthread_join(gen_tid, NULL)))
        exit_with_sys_msg("pthread_join generator", err);

    for(int i=0; i<NUM_VERIFIERS; i++)
    {
        if ((err = pthread_join(ver_tids[i], NULL)))
            exit_with_sys_msg("pthread_join verifier", err);
    }

    for (int i = 0; i < NUM_REPAIRERS; i++) {
        if ((err = pthread_join(rep_tids[i], NULL)))
            exit_with_sys_msg("pthread_join repairer", err);
    }

    queue_destroy(&shared.proposte);
    queue_destroy(&shared.scartati);
    pthread_mutex_destroy(&shared.count_mutex);

    printf("[MAIN] terminato\n");
    return 0;

}