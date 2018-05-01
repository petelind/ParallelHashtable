#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <sys/time.h>
#include <sched.h>
#include <unistd.h>

#define NUM_BUCKETS 5     // Buckets in hash table
#define NUM_KEYS 10000  // Number of keys inserted per thread=
// #define VERBOSE TRUE  // Comment out to disable verbose output (shall be disabled for timing!)

typedef struct _bucket_entry {
    int key;
    int val;
    struct _bucket_entry *next;
} bucket_entry;

/* Part 1. Guard insert operations with mutex.
 * Ok, the problem is in insert part. To avoid messing up with the next
 * pointer in the linked list and insert operations we have to guard sensitive
 * area with the mutex. Lets pack both tables[NUM_BUCKETS] and guarding mutex
 * into nice new structure we will call hashTable. Here we go: */

typedef struct hashTable { bucket_entry *buckets[NUM_BUCKETS];
    pthread_mutex_t lock;
    pthread_mutex_t basket_locks[NUM_BUCKETS]; };
/*
 * Part 4. Multiple buckets allow multiple threads accessing buckets, but then we should be mutually exclusive
 * on the thread level. Lets do exactly it - you cannot modify bucket N if you havent acquired lock of mutex N.
 * Please note: having multiple mutexes (table + basket level) is overkill, but assignment states "continue working
 * with parallel_mutex.c." */

// Needed for part 4 - to pass values to parallel_insert(key_value_pair)
typedef struct {
    int key;
    int value;
} key_value_pair;

struct hashTable *hashtable;
int num_threads = 1;      // Number of threads (configurable)
int keys[NUM_KEYS];


void panic(char *msg) {
    printf("%s\n", msg);
    exit(1);
}

double now() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Inserts a key-value pair into the buckets - this where our mutex will be used
void insert(int key, int val) {
    int i = key % NUM_BUCKETS;
    bucket_entry *e = (bucket_entry *) malloc(sizeof(bucket_entry));
    if (!e) panic("No memory to allocate bucket!");

    e->key = key;
    e->val = val;

    // Part 1. OK, lets keep sensitive area as small as possible:
    pthread_mutex_lock(&hashtable->lock);
    e->next = hashtable->buckets[i]; // lock should be already in place, otherwise
    // chances are we are pointing to the mem location already taken in parallel thread
    hashtable->buckets[i] = e; // insert operation, after that we will release the lock
    pthread_mutex_unlock(&hashtable->lock);
}

/* Part 4. Parallel insert. The difference is that now we would use basket-level locks. */
void* parallel_insert(key_value_pair *pair) {
    int i = pair->key % NUM_BUCKETS;
    #ifdef VERBOSE
    printf("Key %d goes into basket#%d\n", pair->key, i );
    #endif
    bucket_entry *e = (bucket_entry *) malloc(sizeof(bucket_entry));
    if (!e) panic("No memory to allocate bucket!");

    #ifdef VERBOSE
    printf("Now inserting: %d:%d\n", pair->key, pair->value );
    #endif


    e->key = pair->key;
    e->val = pair->value;

    pthread_mutex_lock(&hashtable->basket_locks[i]);
    e->next = hashtable->buckets[i]; // lock should be already in place, otherwise
    // chances are we are pointing to the mem location already taken in parallel thread
    hashtable->buckets[i] = e; // insert operation, after that we will release the lock
    pthread_mutex_unlock(&hashtable->basket_locks[i]);
    pthread_exit(NULL);
}

// Retrieves an entry from the hash buckets by key
// Returns NULL if the key isn't found in the buckets
bucket_entry * retrieve(int key) {
    bucket_entry *b;
    for (b = hashtable->buckets[key % NUM_BUCKETS]; b != NULL; b = b->next) {
        if (b->key == key) return b;
    }
    return NULL;
}

void* parallel_retrieve(int key) {
    /* PART 3. Parallel retrieve.
     * Retrieve operations do not required any specific kind of locks, at least for the main()
     * we see below. What should be changed so multiple retrieves can run in parallel?
     * 1. Returns should be replaced with pthread_exit()
     * 2. Functions returns void* so appropriate conversions should be applied.
     * 3. No references to internals shall be returned (would not exist after pthread_exit()
     * */
    bucket_entry *b;
    b = hashtable->buckets[key % NUM_BUCKETS];

    for (b = hashtable->buckets[key % NUM_BUCKETS]; b != NULL; b = b->next) {
        if (b->key == key) pthread_exit((void*)b);} // We assume receiver will handle conversion

    pthread_exit(NULL);

}

void* parallel_get_phase (void* arg) {
/* PART 3 (Continued). Here we will use parallel_retrieve to spawn 2 parallel retrieves.
 * ( Just 2 for the simplicity and because I dont have more cores, so no point in having more).
 * */
    long tid = (long) arg;
    int key1, key2 = 0;
    long lost = 0;

    // those are worker threads...
    pthread_t* worker1 = (pthread_t *) malloc(sizeof(pthread_t));
    pthread_t* worker2 = (pthread_t *) malloc(sizeof(pthread_t));

    // here we will receive our results...
    bucket_entry* first_result = (bucket_entry*)malloc(sizeof(bucket_entry));
    bucket_entry* second_result = (bucket_entry*)malloc(sizeof(bucket_entry));

    // OK, now we can access our hashtable from 2 parallel streams...
    for (key1 = tid * 2 ; key1 < NUM_KEYS; key1 = key1 + 2 * num_threads) {
        // Spawn threads and ask them to retrieve...
        key2 = key1 + 1;
        #ifdef VERBOSE
        printf("[%d] Now retrieving: key #%d:%d\n", tid, key1, keys[key1]);
        #endif
        pthread_create(&worker1, NULL, parallel_retrieve, (void *) keys[key1]);
        #ifdef VERBOSE
        printf("[%d] Now retrieving key #%d:%d\n", tid, key2, keys[key2]);
        #endif
        pthread_create(&worker2, NULL, parallel_retrieve, (void *) keys[key2]);

        // Wait by the barrier until they return...
        pthread_join(worker1, &first_result);
        #ifdef VERBOSE
        printf("[%d] Just retrieved %d\n", tid, first_result->key);
        #endif
        pthread_join(worker2, &second_result);
        #ifdef VERBOSE
        printf("[%d] Just retrieved %d\n", tid, second_result->key);
        #endif

        // See if we got anything lost
        if ((first_result == NULL) || (first_result->key!=keys[key1]) ) lost ++;
        if ((second_result == NULL) || (second_result->key!=keys[key2]) ) lost ++;
    }

    printf("[thread %ld] %ld keys lost!\n", tid, lost);

    pthread_exit((void *)lost);

}

void * put_phase(void *arg) {

    long tid = (long) arg;
    int key = 0;

    // If there are k threads, thread i inserts
    //      (i, i), (i+k, i), (i+k*2)
    for (key = tid ; key < NUM_KEYS; key += num_threads) {
        insert(keys[key], tid);
    }

    pthread_exit(NULL);
}

void * parallel_put_phase(void *arg) {
    /* Part 4. If two different inserts are running on different buckets in parallel, is lock needed? *
     * If we are talking about table-level lock - no, we dont need it. But we still need basket-level
     * locks, though - they are linked lists, its easy to put them in jeopardy.
     * */

    long tid = (long) arg;

    // those are worker threads, as many as we have baskets...
    pthread_t workers[NUM_BUCKETS];

    int key = 0;

    // If there are k threads, thread i inserts
    //      (i, i), (i+k, i), (i+k*2)
    for (key = tid * NUM_BUCKETS ; key < NUM_KEYS; key = key + num_threads * NUM_BUCKETS) {
        // OK, now we will spin off number of threads equal to the number of buckets we have...
        // Given there is enough cores - thats as fast as we can go...
        for (int i = 0; i < NUM_BUCKETS; i++) {
            key_value_pair pair;
            pair.key = keys[key + i];
            pair.value = tid;
            pthread_create(&workers[i], NULL, parallel_insert, &pair);
        }

        // now we have to wait for them to finish...
        for (int i = 0; i < NUM_BUCKETS; i++) {
            pthread_join(workers[i], NULL);
        }
    }

    pthread_exit(NULL);
}

void * get_phase(void *arg) {
    long tid = (long) arg;
    int key = 0;
    long lost = 0;

    for (key = tid ; key < NUM_KEYS; key += num_threads) {
        if (retrieve(keys[key]) == NULL) lost++;
    }
    printf("[thread %ld] %ld keys lost!\n", tid, lost);

    pthread_exit((void *)lost);
}

int main(int argc, char **argv) {
    long i;
    pthread_t *threads;
    double start, end;

    if (argc != 2) {
        panic("usage: ./parallel_hashtable <num_threads>");
    }
    if ((num_threads = atoi(argv[1])) <= 0) {
        panic("must enter a valid number of threads to run");
    }

    int cores = sysconf(_SC_NPROCESSORS_ONLN); // 1 core would be effectively reserved by OS
    if (cores - 1 < num_threads) printf("INFO: Insufficient cores (%d available) to serve %d threads, "
                                "expect slowdown because of thread mgmt overhead!\n", cores, num_threads);

    // Lets initialize our brand-new struct and its mutex:
    hashtable = (struct hashTable *) malloc(sizeof(struct hashTable));
    pthread_mutex_init(&hashtable->lock, NULL);
    // We are good to go, now sensitive part is protected

    srandom(time(NULL));
    for (i = 0; i < NUM_KEYS; i++)
        keys[i] = random();

    threads = (pthread_t *) malloc(sizeof(pthread_t)*num_threads);
    if (!threads) {
        panic("out of memory allocating thread handles");
    }

    // Insert keys in parallel
    start = now();
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, put_phase, (void *)i);
    }

    // Barrier
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    end = now();

    printf("[main] Inserted %d keys in %f seconds\n", NUM_KEYS, end - start);
    float timing1 = end - start;

    // Reset the thread array
    memset(threads, 0, sizeof(pthread_t)*num_threads);

    // Retrieve keys in parallel
    start = now();
    for (i = 0; i < num_threads; i++) {
        // Part 3. Now we are ready to call pa rallel retrieval of keys in parallel:
        pthread_create(&threads[i], NULL, parallel_get_phase, (void *)i);
        // Please note: if there is not enough cores available - it would actually
        // SLOW DOWN execution, not speed up, because thread management adds overhead.
    }


    // Collect count of lost keys
    long total_lost = 0;
    long *lost_keys = (long *) malloc(sizeof(long) * num_threads);
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], (void **)&lost_keys[i]);
        total_lost += lost_keys[i];
    }
    end = now();

    printf("[main] Retrieved %ld/%d keys in %f seconds\n", NUM_KEYS - total_lost, NUM_KEYS, end - start);
    printf("[main] Press any key to start it over again for Part 3 and 4.\n");
    getchar();

    system("clear");
    printf("[main] OK, lets start it all over again. "
           "Now see %d threads running %d inserting threads each:\n", num_threads, NUM_BUCKETS);
    memset(threads, 0, sizeof(pthread_t)*num_threads);

    start = now();
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, parallel_put_phase, (void *)i);
    }

    // Barrier
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    end = now();
    float timing2 = end - start;

    printf("[main] Inserted %d keys in %f seconds\n", NUM_KEYS, end - start);
    printf("[main] Delta between 2 approaches is %f seconds\n", NUM_KEYS, timing2 - timing1);
    printf("[main] OK, lets see if we lost anyting:\n");

    // Reset the thread array
    memset(threads, 0, sizeof(pthread_t)*num_threads);

    // TODO: Move into separate function
    // Retrieve keys in parallel
    start = now();
    for (i = 0; i < num_threads; i++) {
        // Part 3. Now we are ready to call pa rallel retrieval of keys in parallel:
        pthread_create(&threads[i], NULL, parallel_get_phase, (void *)i);
        // Please note: if there is not enough cores available - it would actually
        // SLOW DOWN execution, not speed up, because thread management adds overhead.
    }

    // Collect count of lost keys again
    total_lost = 0;
    *lost_keys = (long *) malloc(sizeof(long) * num_threads);
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], (void **)&lost_keys[i]);
        total_lost += lost_keys[i];
    }
    end = now();


    return 0;

}
