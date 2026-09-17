#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_LINE_LENGTH 1024

// 1. Definizioni delle Strutture Dati e della Macchina a Stati

typedef enum {
    EMPTY,
    TO_PROCESS,
    DONE
} slot_state_t;

// Slot di comunicazione (Pattern Monitor)
typedef struct {
    char phrase[MAX_LINE_LENGTH];
    int reader_id;
    int phrase_num;

    slot_state_t state;
    pthread_mutex_t mutex;
    pthread_cond_t cond_reader;
    pthread_cond_t cond_transformer;
} rendezvous_slot_t;

// Ambiente Condiviso (Dependency Injection, no variabili globali)
typedef struct {
    rendezvous_slot_t slots[4];
    int shutdown;
} shared_env_t;

// Argomenti per il thread Lettore
typedef struct {
    int id;
    char *filename;
    int processed_phrases;
    shared_env_t *env;
} reader_arg_t;

// Argomenti per il thread Trasformatore
typedef struct {
    int id;
    shared_env_t *env;
} transformer_arg_t;


// 2. Funzioni di Trasformazione (Logica di Dominio)

// Trasformazione 1: LOWER-BACK
void transform_lower_back(char *str) {
    for (int i = 0; str[i] != '\0'; i++) {
        if (str[i] >= 'a' && str[i] <= 'z') {
            if (str[i] == 'a') {
                str[i] = 'z';
            } else {
                str[i] = str[i] - 1;
            }
        }
    }
}

// Trasformazione 2: LEET
void transform_leet(char *str) {
    for (int i = 0; str[i] != '\0'; i++) {
        switch (str[i]) {
            case '0': str[i] = 'o'; break;
            case '1': str[i] = 'i'; break;
            case '3': str[i] = 'e'; break;
            case '7': str[i] = 't'; break;
            case '@': str[i] = 'a'; break;
            case '$': str[i] = 's'; break;
            default: break; 
        }
    }
}

// Trasformazione 3: UPPER-FWD
void transform_upper_fwd(char *str) {
    for(int i = 0; str[i] != '\0'; i++) {
        if(str[i] >= 'A' && str[i] <= 'Z') {
            if(str[i] == 'Z') {
                str[i] = 'A';
            } else {
                str[i] = str[i] + 1;
            }
        }
    }
}

// Trasformazione 4: TITLE-CASE
void transform_title_case(char *str) {
    int new_word = 1; 

    for(int i = 0; str[i] != '\0'; i++) {
        if(str[i] == ' ') {
            new_word = 1;
        } else {
            if(new_word) {
                if(str[i] >= 'a' && str[i] <= 'z') {
                    str[i] = str[i] - 32;
                }
                new_word = 0;
            } else {
                if(str[i] >= 'A' && str[i] <= 'Z') {
                    str[i] = str[i] + 32;
                }
            }
        }
    }
}


// 3. Routine dei Thread (Concorrenza e Sincronizzazione)

void *transformer_routine(void *arg) {
    transformer_arg_t *t_arg = (transformer_arg_t *)arg;
    int id = t_arg->id;
    shared_env_t *env = t_arg->env;

    const char *transformer_names[] = {"LOWER-BACK", "LEET", "UPPER-FWD", "TITLE-CASE"};
    int requests_served = 0;

    while(1) {
        pthread_mutex_lock(&env->slots[id].mutex);

        // Attesa passiva che ci sia lavoro e che non sia richiesto lo spegnimento
        while(env->slots[id].state != TO_PROCESS && env->shutdown == 0) {
            pthread_cond_wait(&env->slots[id].cond_transformer, &env->slots[id].mutex);
        }

        // Condizione di uscita (Graceful Shutdown)
        if(env->shutdown == 1 && env->slots[id].state != TO_PROCESS) {
            pthread_mutex_unlock(&env->slots[id].mutex);
            break; 
        }

        char old_phrase[MAX_LINE_LENGTH];
        strcpy(old_phrase, env->slots[id].phrase);

        // Routing al trasformatore specifico
        switch(id) {
            case 0: transform_lower_back(env->slots[id].phrase); break;
            case 1: transform_leet(env->slots[id].phrase); break;
            case 2: transform_upper_fwd(env->slots[id].phrase); break;
            case 3: transform_title_case(env->slots[id].phrase); break;
        }

        printf("[%s] frase n.%d di READER-%d: '%s' -> '%s'\n", 
               transformer_names[id], 
               env->slots[id].phrase_num, 
               env->slots[id].reader_id, 
               old_phrase, 
               env->slots[id].phrase);

        requests_served++;
        env->slots[id].state = DONE;

        pthread_cond_signal(&env->slots[id].cond_reader);
        pthread_mutex_unlock(&env->slots[id].mutex);
    }

    printf("[%s] terminazione con %d richieste servite\n", transformer_names[id], requests_served);
    return NULL;
}


void *reader_routine(void *arg) {
    reader_arg_t *r_arg = (reader_arg_t *) arg;
    int my_id = r_arg->id;
    char *filename = r_arg->filename;
    shared_env_t *env = r_arg->env; 

    printf("[READER-%d] apertura del file '%s'\n", my_id, filename);

    FILE* file = fopen(filename, "r");
    if(file == NULL) {
        perror("Errore nell'apertura del file");
        pthread_exit(NULL);
    }

    char line[MAX_LINE_LENGTH];
    int phrase_counter = 1;

    while (fgets(line, sizeof(line), file) != NULL) {
        line[strcspn(line,"\n")] = '\0';

        if(strlen(line) == 0) {
            continue;
        }

        char original_phrase[MAX_LINE_LENGTH];
        strcpy(original_phrase, line);
        
        printf("[READER-%d] frase n.%d: '%s'\n", my_id, phrase_counter, original_phrase);

        // Coreografia di Rendezvous attraverso i 4 slot
        for(int t = 0; t < 4; t++) {
            pthread_mutex_lock(&env->slots[t].mutex);

            while(env->slots[t].state != EMPTY) {
                pthread_cond_wait(&env->slots[t].cond_reader, &env->slots[t].mutex);
            }

            strcpy(env->slots[t].phrase, line);
            env->slots[t].reader_id = my_id;
            env->slots[t].phrase_num = phrase_counter;

            env->slots[t].state = TO_PROCESS;

            pthread_cond_signal(&env->slots[t].cond_transformer);

            while(env->slots[t].state != DONE) {
                pthread_cond_wait(&env->slots[t].cond_reader, &env->slots[t].mutex);
            }

            strcpy(line, env->slots[t].phrase);
            env->slots[t].state = EMPTY;

            pthread_cond_broadcast(&env->slots[t].cond_reader);
            pthread_mutex_unlock(&env->slots[t].mutex);
        }

        printf("[READER-%d] frase n.%d decodificata in: '%s'\n", my_id, phrase_counter, line);
        phrase_counter++;
    }

    fclose(file);
    r_arg->processed_phrases = phrase_counter - 1;

    printf("[READER-%d] terminazione con %d frasi decodificate\n", my_id, r_arg->processed_phrases);
    return NULL;
}


// 4. Entry Point (Inizializzazione e Orchestrazione)

int main(int argc, char *argv[]) {
    if(argc < 2) { 
        fprintf(stderr, "Uso: %s <file-1> ... <file-N>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int num_readers = argc - 1;
    printf("[MAIN] Creazione di %d thread lettori e 4 thread trasformatori.\n", num_readers);

    // Allocazione locale dell'ambiente condiviso
    shared_env_t env;
    env.shutdown = 0; 

    // Inizializzazione di Mutex e Condition Variables
    for(int i = 0; i < 4; i++) {
        env.slots[i].state = EMPTY;

        if(pthread_mutex_init(&env.slots[i].mutex, NULL) != 0) {
            perror("Errore inizializzazione mutex");
            exit(EXIT_FAILURE);
        }
        if(pthread_cond_init(&env.slots[i].cond_reader, NULL) != 0) {
            perror("Errore inizializzazione cond_reader");
            exit(EXIT_FAILURE);
        }
        if(pthread_cond_init(&env.slots[i].cond_transformer, NULL) != 0 ) {
            perror("Errore inizializzazione cond_transformer");
            exit(EXIT_FAILURE);
        }
    }

    // Spawning Trasformatori
    pthread_t transformers[4];
    transformer_arg_t t_args[4]; 

    for(int i = 0; i < 4; i++) {
        t_args[i].id = i;
        t_args[i].env = &env; 

        if(pthread_create(&transformers[i], NULL, transformer_routine, &t_args[i]) != 0) {
            perror("Errore nella creazione del thread trasformatore");
            exit(EXIT_FAILURE);
        }
    }   

    // Spawning Lettori
    pthread_t readers[num_readers];
    reader_arg_t reader_args[num_readers];

    for(int i = 0; i < num_readers; i++) {
        reader_args[i].id = i + 1; 
        reader_args[i].filename = argv[i + 1];
        reader_args[i].env = &env; 

        if(pthread_create(&readers[i], NULL, reader_routine, &reader_args[i]) != 0) {
            perror("Errore nella creazione del thread lettore");
            exit(EXIT_FAILURE);
        }
    }

    // Barrier Synchronization: Attesa Lettori
    for(int i = 0; i < num_readers; i++) {
        pthread_join(readers[i], NULL);
    }

    // Graceful Shutdown: Spegnimento Trasformatori
    env.shutdown = 1;
    for(int i = 0; i < 4; i++) {
        pthread_mutex_lock(&env.slots[i].mutex);
        pthread_cond_broadcast(&env.slots[i].cond_transformer);
        pthread_mutex_unlock(&env.slots[i].mutex);
    }

    // Barrier Synchronization: Attesa Trasformatori
    for(int i = 0; i < 4; i++) {
        pthread_join(transformers[i], NULL);
    }

    // Stampa Riepilogo
    printf("[MAIN] riepilogo: ");
    for(int i = 0; i < num_readers; i++) {
        printf("%s: %d frasi", reader_args[i].filename, reader_args[i].processed_phrases);
        if(i < num_readers - 1) {
            printf(", "); 
        }
    }
    printf("\n");

    printf("[MAIN] Terminazione\n");
    return EXIT_SUCCESS;
}
