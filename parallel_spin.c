#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <sys/time.h>

#define NUM_BUCKETS 5     // Buckets in hash table
#define NUM_KEYS 100000   // Number of keys inserted per thread=

/*
 * Ok, we see huge difference with the spinlock - 0.006 sec VS 0.02 sec with the mutex.
 * It seems counter-intuitive, because in theory spinlock just burns CPU cycles.
 * In practice, though, we have really tiny sensitive area & 4 cores serving other threads, so
 * spinlock has good chances to acquire desired lock during allocated quantum, which is exactly
 * what happens. Mutex, on the contrary, puts thread to sleep until lock can be acquired, which is
 * [relatively] expensive operation - in our case, more expensive than spinning for some time.
 * Thus, spinlock in this case would be faster than mutex, given we are running multi-core CPU -
 * for the single core quite the opposite would be happen - lock would just burn through cycles
 * while nobody can release the lock because no other threads are running.
 */

typedef struct _bucket_entry {
    int key;
    int val;
    struct _bucket_entry *next;
} bucket_entry;
typedef struct hashTable { bucket_entry *table[NUM_BUCKETS]; pthread_spinlock_t lock; };

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

// Inserts a key-value pair into the table - this where our spin will be used
void insert(int key, int val) {
    int i = key % NUM_BUCKETS;
    bucket_entry *e = (bucket_entry *) malloc(sizeof(bucket_entry));
    if (!e) panic("No memory to allocate bucket!");

    e->key = key;
    e->val = val;

    // OK, lets keep sensitive area as small as possible:
    pthread_spin_lock(&hashtable->lock);
    e->next = hashtable->table[i]; // lock should be already in place, otherwise
    // chances are we are pointing to the mem location already taken in parallel thread
    hashtable->table[i] = e; // insert operation, after that we will release the lock
    pthread_spin_unlock(&hashtable->lock);
}

// Retrieves an entry from the hash table by key
// Returns NULL if the key isn't found in the table
bucket_entry * retrieve(int key) {
    bucket_entry *b;
    for (b = hashtable->table[key % NUM_BUCKETS]; b != NULL; b = b->next) {
        if (b->key == key) return b;
    }
    return NULL;
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

    // Lets initialize our brand-new struct and its spin:
    hashtable = (struct hashTable *) malloc(sizeof(struct hashTable));
    pthread_spin_init(&hashtable->lock, NULL);
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

    // Reset the thread array
    memset(threads, 0, sizeof(pthread_t)*num_threads);

    // Retrieve keys in parallel
    start = now();
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, get_phase, (void *)i);
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

    return 0;
}
