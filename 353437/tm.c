/**
 * @file   tm.c
 * @author Can Kirimca
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

#include <tm.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "macros.h"

struct transaction{
    uint64_t id;
    uint64_t version;
    bool read_only;
    struct read_set* rs;
    struct write_set* ws;
    struct segment_node* segments_to_free_head;
    struct region* region;
};

struct lock{
    _Atomic unsigned long version;
    _Atomic unsigned long holder;
    _Atomic bool locked;
};

//a struct denoting the segments in which the users write or read
struct segment_node{
    struct segment_node* next; //ptr to next segment
    uint64_t segment_id;       //id of the segment
    uint64_t segment_size;     //size of the segment
    uint64_t alignment;        //alignment = size of a word 
    shared_t allocated_address;        //pointer to the allocated space 
    struct lock* locks; //lock array for the words
    uint64_t numberOfLocks;
    bool isDeleted;
};

struct region {
    //struct shared_lock_t lock;
    void* start;     
    struct segment_node** allocs; 
    size_t size;       
    size_t align;  
    _Atomic uint64_t no_of_segments;
    _Atomic uint64_t transaction_id_counter;
    _Atomic uint64_t segment_id_counter;
    _Atomic uint64_t version;
    //struct segment_node* segmentSingle; //single segment implementation
};

//struct representing a single write node
struct write_node{
    struct write_node* next;
    struct segment_node* segment;
    uint64_t index;
    shared_t word;
    uint64_t address;
};
//struct representing the write set containing multiple write_nodes
struct write_set{
    struct write_node* head;
    uint64_t size;
};

//struct representing a single read node
struct read_node{
    struct read_node* next;
    struct segment_node* segment;
    u_int64_t index;
    shared_t word;
    uint64_t address;
};
//struct representing the read set containing multiple read_nodes
struct read_set{
    struct read_node* head;
    uint64_t size;
};

void wset_clear(struct write_set* set){
    struct write_node* head = set->head;
    while(head != NULL){
        struct write_node* temp = head;
        head = head->next;
        free(temp->word);
        free(temp);
    }
    set->head = NULL;
    set->size = 0;
}

void wset_append(struct write_set* set, struct write_node* new_node){
    struct write_node* head = set->head;
    struct write_node* prev = NULL;

    if(head == NULL){
        set->head = new_node;
        set->head->next = NULL;
        set->size++;
        return;
    }

    while(head != NULL){
        prev = head;
        head = head->next;
    }
    if(prev != NULL){
        prev->next = new_node;
    }
    set->size++;
    //printf("in size: %lu\n", set->size);

}

void rset_clear(struct read_set* set){
    struct read_node* head = set->head;
    while(head != NULL){
        struct read_node* temp = head;
        head = head->next;
        free(temp);
    }
    set->head = NULL;
    set->size = 0;
}

void rset_append(struct read_set* set, struct read_node* new_node){
    struct read_node* head = set->head;
    struct read_node* prev = NULL;

    if(head == NULL){
        set->head = new_node;
        set->head->next = NULL;
        set->size++;
        return;
    }

    while(head != NULL){
        prev = head;
        head = head->next;
    }
    if(prev != NULL){
        prev->next = new_node;
    }
    set->size++;
}

bool acquire(struct transaction* t, struct lock* l){
    bool* f = malloc(sizeof(bool));
    *f = false;
    //printf("\nbefore acquire, lock: %lu, holder: %lu\n", l->locked, l->holder);
    if(atomic_compare_exchange_strong(&(l->locked), f, true)){
        l->holder = t->id;
        free(f);
        //printf("\nafter acquire, lock: %lu, holder: %lu\n", l->locked, l->holder);
        return true;
    }
    free(f);
    return false;
}

bool release(struct transaction* t, struct lock* l){
    //printf("\nbefore release, lock: %lu, holder: %lu\n", l->locked, l->holder);
    if(l->holder == t->id) {
        l->locked = false;
        l->holder = 0;
        //printf("\nafter release, lock: %lu, holder: %lu\n", l->locked, l->holder);
        return true;
    } 
    return false;
}

void transaction_cleanup(struct transaction* t, bool only_sets, bool clear_locks, struct lock** l){
    //printf("\n-----------Transaction cleanup----------------\n");
    
    if(only_sets){
        wset_clear(t->ws);
        rset_clear(t->rs);
        free(t->ws);
        free(t->rs);
        free(t);
        return;
    }

    if(l == NULL)
        return;    

    struct lock** locks = l;
    if(clear_locks){
        for(unsigned long i = 0; i < t->ws->size; i++){
            //printf("holder: %lu\n", locks[i]->holder);
            //printf("tid: %lu\n", t->id);
            release(t, locks[i]);
        }
    }
    wset_clear(t->ws);
    rset_clear(t->rs);  
    free(locks);
    free(t->ws);
    free(t->rs);
    free(t);
}

//returns the segment whose allocated address is equal to the address passed as parameter 
struct segment_node* segment_get(struct region* r, shared_t address){
    struct region* region = r; 
    for(int i = 0; i < 70000; i++){
        if(r->allocs[i] != NULL && r->allocs[i]->allocated_address <= address && r->allocs[i]->allocated_address + r->allocs[i]->segment_size >= address){
            return r->allocs[i];
        }
    }
    return NULL;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////types_and_helpers_end//////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//Check if the given number is a power of 2
bool isPowerOfTwo(size_t n){
    while(n>1){
        if(n % 2 != 0)
            return false;
        n = n/2;    
    }
    return true;
}

uint64_t generateTransactionId(struct region* shared){
    struct region* region = shared;
    uint64_t newId = region->transaction_id_counter;
    region->transaction_id_counter++;
    return newId;
}

void addNode(struct segment_node* head, struct segment_node* node){
    if(head == NULL){
        head = node;
        node->next = NULL;
        return;
    }
    
    struct segment_node* tempHead = head;
    
    struct segment_node* tempPrev;
    while(tempHead != NULL){
        tempPrev = tempHead;
        tempHead = tempHead->next;
    }
    tempPrev->next = node;
    node->next = NULL;
}

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) {
    //printf("-------------------------------------tm_create start\n");
    if(size % align != 0)
        return invalid_shared;
    if(!isPowerOfTwo(align)) 
        return invalid_shared;

    //initialize region pointer and check if it is unlikely
    struct region* region = (struct region*)malloc(sizeof(struct region)); 
    if (unlikely(!region)) {
        return invalid_shared;
    }
    //initialize region segments pointer and check if it is unlikely
    region->allocs = (struct segment_node**) malloc(sizeof(struct segment_node*) * 70000);

    if (unlikely(!region->allocs)) {
        free(region);
        return invalid_shared;
    }


    for(int i = 0; i < 70000; i++){
        region->allocs[i] = NULL; //check for memory leak
    }    
    
    

    struct segment_node* segment = (struct segment_node*) malloc(sizeof(struct segment_node)); 
    //initialize region segments pointer and check if it is unlikely
    if (unlikely(!segment)) {
        free(region);
        return invalid_shared;
    }

    region->align = align;
    region->transaction_id_counter = 1;
    region->segment_id_counter = 0;
    segment->segment_size = size;
    segment->alignment = align;
    segment->segment_id = 0;
    segment->next = NULL;
    region->no_of_segments = 1;
    region->version = 0;
    segment->isDeleted = false;

    segment->numberOfLocks = size/align;
    segment->locks = (struct lock*) malloc(sizeof(struct lock) * size/align);
    if (unlikely(!segment->locks)) {
        free(segment);
        free(region);
        return invalid_shared;
    }    

    for(unsigned long i = 0; i < segment->numberOfLocks; i++){
        struct lock* l = (struct lock*) &(segment->locks[i]);
        l->holder = 0;
        l->version = 0;
        l->locked = false;
    }

    //allocate the first space and free the pointer if the allocation is not successful
    if (posix_memalign(&(segment->allocated_address), align, size) != 0) {
        free(segment->locks);
        free(segment);
        free(region);
        return invalid_shared;
    }

    region->allocs[0] = segment;
    region->start = segment->allocated_address;

    //initialize to zero as instructed
    memset(segment->allocated_address, 0, size);
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    //printf("destroy start\n");
    //return false;
    struct region* region = (struct region*)shared;
    for(int i = 0; i < 70000; i++){
        if(region->allocs[i] != NULL){
            free(region->allocs[i]->locks);
            free(region->allocs[i]);
        }
    }    
    free(region->allocs);
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    struct region* region = (struct region*) shared;
    return region->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    struct region* region = shared;
    return region->allocs[0]->segment_size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) {
    //printf("-------------------------------------tm_align start");
    struct region* region = (struct region*) shared;
    return region->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) {
    struct transaction* transaction = (struct transaction*) malloc(sizeof(struct transaction));
    struct region* region = shared;

    if(unlikely(!transaction)){
        return invalid_tx;
    }

    //transaction->alloc_head = NULL;

    //write set initialize
    transaction->ws = (struct write_set*) malloc(sizeof(struct write_set));
    if(unlikely(!transaction->ws)){
        free(transaction);
        return invalid_tx;
    }
    transaction->ws->head = NULL;
    transaction->ws->size = 0;
    
    //read set initialize
    transaction->rs = (struct read_set*)malloc(sizeof(struct read_set));
    if(unlikely(!transaction->rs)){
        free(transaction->ws);
        free(transaction);
        return invalid_tx;
    }
    transaction->rs->head = NULL;
    transaction->rs->size = 0;

    transaction->region = region;
    transaction->id = __sync_add_and_fetch(&(region->transaction_id_counter), 1);
    transaction->read_only = is_ro;
    transaction->version = region->version;
    transaction->segments_to_free_head = NULL;
    return (tx_t)transaction;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) {
    //printf("-------------------------------------tm_end start");
    struct transaction* transaction = (struct transaction*) tx; 
    struct region* region = shared;
    if(transaction->read_only){
        transaction_cleanup(transaction, true, false, NULL);
        return true;
    }
    else{
        //get all the locks associated with the write set of the transaction
        struct lock** locks = (struct lock**)malloc(sizeof(struct lock*) * transaction->ws->size);
        struct write_node* head = transaction->ws->head;
        u_int64_t k = 0;

        while(head != NULL){
            locks[k] = (struct lock*) &(head->segment->locks[head->index]);
            head = head->next;
            k++;
        }

        //try to lock all locks
        //if a lock cannot be locked, unlock all locked ones
        bool canCommit = true;
        for(unsigned long i = 0; i < transaction->ws->size; i++){
            if(!acquire(transaction, locks[i])){
                //printf("failed at %lu out of %lu\n", i, transaction->ws->size);
                for(unsigned long j = 0; j < i; j++)
                    release(transaction, locks[j]);
                canCommit = false;
                break;
            }
            //printf("transaction %lu acquired lock %lu\n", transaction->id, i);
        }

        if(canCommit){            //we can commit
            //printf("\ntransaction %lu can commit\n", transaction->id);
            //if another transaction incremented the version first 
            _Atomic uint64_t tempV = __sync_add_and_fetch(&(region->version), 1);

            //printf("transaction %lu\n", transaction->id);
            //printf("temp %lu\n", temp1);
            if(transaction->version + 1 != tempV){ 
                struct read_node* rn = transaction->rs->head;
                while(rn != NULL){
                    struct lock* lock = (struct lock*) &(rn->segment->locks[rn->index]);
                    bool available = true;
                    if(lock->locked){
                        available = false;
                        for(int x = 0; x < transaction->ws->size; x++){
                            if(locks[x] == lock){                       
                                available = true;
                                break;
                            }
                        }
                    }

                    if(!available ||  transaction->version < lock->version){
                        transaction_cleanup(transaction, false, true, locks);
                        return false;
                    }

                    rn = rn->next;
                }
            }
            //commit all writes
            struct write_node* wn = transaction->ws->head;
            while(wn != NULL){
                struct segment_node* s = wn->segment;
                shared_t dest = (shared_t)(s->allocated_address + wn->index * region->align);
                memcpy(dest, wn->word, region->align);
                //printf("version before %lu\n",wn->segment->locks[wn->index].version);
                struct lock* locktemp = (struct lock*)&(wn->segment->locks[wn->index]);
                locktemp->version = tempV;
                //printf("version after %lu\n", wn->segment->locks[wn->index].version);
                wn = wn->next;
            }   

            while(transaction->segments_to_free_head != NULL){
                transaction->segments_to_free_head->isDeleted = true;
                transaction->segments_to_free_head = transaction->segments_to_free_head->next;
            }

            transaction_cleanup(transaction, false, true, locks);
            return true;
        }else{
            //printf("\ntransaction %lu cannot commit\n", transaction->id);
            transaction_cleanup(transaction, false, true, locks);
            return false;
        }
        transaction_cleanup(transaction, false, true, locks);

        return true;
    }
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    //printf("-------------------------------------tm_read start\n");    
    struct transaction* transaction = (struct transaction* )tx;
    struct region* region = shared;
    //printf("read calling get\n");    
    struct segment_node* s = segment_get(region, (void*)source);

    if(s == NULL || s->isDeleted){
        //printf("read t %lu segment already deleted %lu\n",transaction->id, s->segment_id);
        transaction_cleanup(transaction, true, false, NULL);
        return false;
    }else{
        //printf("read t %lu can read from segment %lu\n",transaction->id, s->segment_id);
    }
    uint64_t noOfWords = size/(region->align);
    uint64_t index = 0;
    void* src = (void*) source;
    void* trg = (void*) target;

    while(index < noOfWords){
        struct lock* lock = (struct lock*) &(s->locks[(src - s->allocated_address)/region->align]);
        uint64_t tempV = lock->version;
        bool successful = (lock->version == tempV) && (lock->version <= transaction->version) && (!(lock->locked));
        
        if(!successful){
            transaction_cleanup(transaction, true, false, NULL);
            return false;
        }

        if(transaction->read_only){
            memcpy(trg, src, region->align);
        }
        else{
            //check if the value to be read has been written by us before
            struct write_node* existing_write = transaction->ws->head;
            while(existing_write != NULL){
                if((uint64_t)src == existing_write->address){
                    break; //break with the value set to the desired node
                }
                existing_write = existing_write->next;
            } //if no break, the value will be null

            if(existing_write != NULL){
                memcpy(trg, existing_write->word, region->align);  
            } 
            else{
                memcpy(trg, src, region->align);
            }
            struct read_node* rn = (struct read_node*) malloc(sizeof(struct read_node));
            rn->next = NULL;
            rn->segment = s;
            rn->index = ((src - s->allocated_address)/region->align);
            rset_append(transaction->rs, rn);
        }

        successful = (lock->version == tempV) && (lock->version <= transaction->version) && (!(lock->locked));
        
        if(!successful){
            transaction_cleanup(transaction, true, false, NULL);
            return false;
        }

        index += 1;
        trg += region->align;
        src += region->align;
    }
    //printf("-------------------------------------tm_read successful\n");    
    return true;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    //printf("tm_write start--------------------\n");

    struct transaction* transaction = (struct transaction*) tx;

    struct region* region = (struct region*)shared;
    //printf("read calling get\n");    
    struct segment_node* s = segment_get(region, (shared_t)target);
    if(s == NULL || s->isDeleted){
        transaction_cleanup(transaction, true, false, NULL);
        return false;
    }

    void* src = (void*) source; 
    void* trg = (void*) target;
    uint64_t noOfWords = size/(region->align);
    uint64_t index = 0;
    int wc = 1;
    while(index < noOfWords){
        struct write_node* previously_written_node = transaction->ws->head;
        while(previously_written_node != NULL){
            if(previously_written_node->address == (uint64_t) trg){
                break;
            }
            previously_written_node = previously_written_node->next;
        }

        if(previously_written_node == NULL){
            void * newSpace;
            //if failed to allocate memory, clean up and return false
            if(posix_memalign(&(newSpace), region->align, (region->align * wc)) != 0){
                transaction_cleanup(transaction, true, false, NULL);
                return false;
            }
            
            memcpy(newSpace, src, region->align);
            struct write_node* wn = (struct write_node*) malloc(sizeof(struct write_node));

            wn->next = NULL;
            wn->segment = s;
            wn->index = (uint64_t)((trg - s->allocated_address)/region->align);
            //printf("assigning %lu to wn index\n", ((trg - s->allocated_area)/region->align));

            wn->word = newSpace;
            wn->address = (uint64_t) (s->allocated_address + wn->index * region->align);
            wset_append(transaction->ws, wn);
        }
        else{
            memcpy(previously_written_node->word, src, region->align);
        }
        index += 1;
        trg += region->align;
        src += region->align;
    }
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) {
    struct transaction* transaction = (struct transaction*) tx; 
    struct region* region = (struct region*)shared;
    struct segment_node* segment = (struct segment_node*) malloc(sizeof(struct segment_node)); 

    if(unlikely(!segment)){
        return nomem_alloc;
    }
    if (posix_memalign(&(segment->allocated_address), region->align, size) != 0) {
        free(segment);
        return nomem_alloc;
    }

    if(unlikely(!segment->allocated_address)){
        free(segment);
        return nomem_alloc;
    }
    segment->segment_id = __sync_add_and_fetch(&(region->segment_id_counter), 1);
    memset(segment->allocated_address, 0, size);
    segment->segment_size = size;
    segment->alignment = region->align;
    segment->isDeleted = false;

    segment->next = NULL;
    segment->numberOfLocks = size/region->align;
    segment->locks = (struct lock*) malloc(sizeof(struct lock) * size/region->align);
    if (unlikely(!segment->locks)) {
        free(segment);
        free(segment->allocated_address);
        return nomem_alloc;
    }    
    for(unsigned long i = 0; i < segment->numberOfLocks; i++){
        struct lock* l = (struct lock*) &(segment->locks[i]);
        l->holder = 0;
        l->version = 0;
        l->locked = false;
    }

    region->no_of_segments = __sync_add_and_fetch(&(region->no_of_segments), 1);
    region->allocs[segment->segment_id] = segment;

    memcpy(target, &(segment->allocated_address), sizeof(void*));
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) {
    struct transaction* transaction = (struct transaction*) tx;
    struct region* region = (struct region*)shared;

    struct segment_node* segment = (struct segment_node*) segment_get(region, (shared_t)target);
    if(segment == NULL || segment->isDeleted){
        transaction_cleanup(transaction, true,false,NULL);
        return false;
    }

    region->no_of_segments = __sync_add_and_fetch(&(region->no_of_segments), -1);
    addNode(transaction->segments_to_free_head, segment);
    return true;
}
