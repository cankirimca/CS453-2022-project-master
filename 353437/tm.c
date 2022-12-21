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

// External headers

// Internal headers
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
    //struct segment_set allocated_segments;
    //struct segment_set free_segments;
    struct region* region;
};

struct lock{
    _Atomic unsigned long version;
    _Atomic unsigned long holder;
    _Atomic bool locked;
};

bool acquire(struct transaction* t, struct lock* l){
    bool* f = malloc(sizeof(bool));
    *f = false;
    if(atomic_compare_exchange_strong(&(l->locked), f, true)){
        l->holder = t->id;
        free(f);
        return true;
    }
    free(f);
    return false;
}

bool release(struct transaction* t, struct lock* l){
    if(l->holder == t->id) {
        l->locked = false;
        l->holder = 0;
        return true;
    } 
    return false;
}

struct lock_list{
    struct lock* list_ptr;
    unsigned long size;
};

//a struct denoting the segments in which the users write or read
struct segment_node{
    struct segment_node* prev; //ptr to next segment
    struct segment_node* next; //ptr to previous segment
    uint64_t segment_id;       //id of the segment
    uint64_t segment_size;     //size of the segment
    uint64_t alignment;        //alignment = size of a word 
    bool is_allocated;     
    shared_t allocated_area;        //pointer to the allocated space 
    struct lock* locks;
    uint64_t numberOfLocks;
};

struct segment_list{
    struct segment_node** seg_addresses;
    unsigned int number_of_segments;
    _Atomic bool* locks;
};

struct region {
    //struct shared_lock_t lock;
    void* start;     
    //struct segment_list allocated_segments; 
    size_t size;       
    size_t align;  
    unsigned int no_of_segments;
    _Atomic uint64_t id_counter;
    _Atomic uint64_t version;
    struct segment_node* segmentSingle;
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
        //set->tail = new_node;
    }
    set->size++;
    //printf("in size: %lu\n", set->size);

}


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

struct segment_set{
    struct segment_node* head;
    struct segment_node* tail;
    int size;
};

void transaction_cleanup(struct transaction* t, bool only_sets, bool clear_locks, struct lock** l){
    //TODO make some changes 
    //printf("\n-----------Transaction cleanup----------------\n");
    
    if(only_sets){
        wset_clear(t->ws);
        rset_clear(t->rs);
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
    //printf("zwei\n\n");
    wset_clear(t->ws);
    rset_clear(t->rs);  
    free(locks);
    free(t->ws);
    free(t->rs);
}
/*
void freeLocks(struct lock* locks, bool all, uint64_t index){
    for(uint64_t i = 0; i < index; i++){
        free(locks[i]);
    }
    if(all)
        free(locks);
}*/

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////types_end//////////////////////////////////////////////////
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
    uint64_t newId = region->id_counter;
    region->id_counter++;
    return newId;
}

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) {
    //printf("-------------------------------------tm_create start");
    // TODO: tm_create(size_t, size_t)
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
    //TODO change for multi segment
    //region->allocated_segments.seg_addresses = (struct segment_node**) malloc(sizeof(struct segment_node*) * region->no_of_segments); //TODO change later
    region->segmentSingle = (struct segment_node*) malloc(sizeof(struct segment_node)); //TODO change later
    if (unlikely(!region->segmentSingle)) {
        free(region);
        return invalid_shared;
    }
    //TODO change for multi segment implementation
    //region->allocated_segments.locks = malloc(sizeof(_Atomic bool) * region->no_of_segments); //TODO change later
    region->segmentSingle->numberOfLocks = size/align;
    region->segmentSingle->locks = (struct lock*) malloc(sizeof(struct lock) * size/align);
    if (unlikely(!region->segmentSingle->locks)) {
        free(region->segmentSingle);
        free(region);
        return invalid_shared;
    }    

    for(unsigned long i = 0; i < region->segmentSingle->numberOfLocks; i++){
        /*region->segmentSingle->locks[i] = (struct lock*) malloc(sizeof(struct lock));// * region->segmentSingle->numberOfLocks); //TODO change later
        if (unlikely(!region->segmentSingle->locks[i])) {
            for(unsigned long j = 0; j < i; j++){
                free(region->segmentSingle->locks[j]);
            }
            free(region->segmentSingle->locks);
            free(region->segmentSingle);
            free(region);
            return invalid_shared;
        } */
        region->segmentSingle->locks[i].holder = 0;
        region->segmentSingle->locks[i].version = 0;
        region->segmentSingle->locks[i].locked = false;
    }
    
    //TODO uncomment for multi segment
    /*struct segment_list temp = region->allocated_segments;
    for(int j = 0; j < region->no_of_segments; j++){
        temp.seg_addresses[j] = NULL;
        temp.locks[j] = false;
    }

    //TODO initialize previous two mallocs

    //allocate the starting region
    region->allocated_segments.seg_addresses[0] = malloc(sizeof(struct segment_node));
    
    struct segment_node* temp_first_node = (struct segment_node*)(region->allocated_segments.seg_addresses[0]);*/
    //allocate the first space and free the pointer if the allocation is not successful
    if (posix_memalign(&(region->segmentSingle->allocated_area), align, size) != 0) {
        free(region->segmentSingle->locks);//freeLocks(region->segmentSingle->locks, true, region->segmentSingle->numberOfLocks);
        free(region->segmentSingle);
        free(region);
        return invalid_shared;
    }
    memset(region->segmentSingle->allocated_area, 0, size);

    //TODO move to the beginning
    region->start = region->segmentSingle->allocated_area;
    region->align = align;
    region->id_counter = 1;
    region->segmentSingle->segment_size = size;
    region->segmentSingle->alignment = align;
    region->segmentSingle->segment_id = 0;
    region->segmentSingle->next = NULL;
    region->segmentSingle->prev = NULL;
    region->no_of_segments = 1;
    region->version = 0;
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    //printf("-------------------------------------tm_destroy start");

    // TODO: tm_destroy(shared_t)
    struct region* region = (struct region*)shared;
    /*for(int j = 0; j < (region->no_of_segments); j++){
        free(region->allocated_segments.seg_addresses[j]->allocated_area);
        free(region->allocated_segments.seg_addresses[j]->lock_list);
        free(region->allocated_segments.seg_addresses[j]);
    }
    free(region->allocated_segments.seg_addresses);
    free(region->allocated_segments.locks);
    free(region);*/
    free(region->segmentSingle->allocated_area);
    free(region->segmentSingle->locks);
    free(region->segmentSingle);
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    //printf("-------------------------------------tm_start start");
    struct region* region = (struct region*) shared;
    return region->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    //printf("-------------------------------------tm_size start");

    struct region* region = shared;
    //struct segment_node* first_segment = region->allocated_segments.seg_addresses[0];
    //return first_segment->segment_size;
    return region->segmentSingle->segment_size;
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
    // TODO: tm_begin(shared_t)

    struct transaction* transaction = (struct transaction*) malloc(sizeof(struct transaction));
    struct region* region = shared;

    if(unlikely(!transaction)){
        return invalid_tx;
    }

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
    transaction->id = __sync_add_and_fetch(&(region->id_counter), 1);
    transaction->read_only = is_ro;
    transaction->version = region->version;
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

    if(!transaction->read_only){
        //printf("is not read only\n");

        //get all the locks associated with the write set of the transaction
        struct lock** locks = (struct lock**)malloc(sizeof(struct lock*) * transaction->ws->size);
        struct write_node* head = transaction->ws->head;
        u_int64_t k = 0;
        //printf("size %d\n", transaction->ws->size);

        while(head != NULL){ //TODO consider changing to for loop
            locks[k] = (struct lock*) &(head->segment->locks[head->index]);
            //printf("adding %d\n", locks[k]->locked);
            head = head->next;
            k++;
        }

        //try to lock all locks
        //if a lock cannot be locked, unlock all locked ones
        bool canCommit = true;
        //printf("size: %lu\n", transaction->ws->size);

        for(unsigned long i = 0; i < transaction->ws->size; i++){
            if(!acquire(transaction, locks[i])){
                //printf("failed at %lu out of %lu\n", i, transaction->ws->size);
                for(unsigned long j = 0; j < i; j++)
                    release(transaction, locks[i]);
                canCommit = false;
                break;
            }
            //printf("transaction %lu acquired lock %lu\n", transaction->id, i);
        }
        _Atomic unsigned long wv;

        if(canCommit){            //we can commit
            //printf("\ntransaction %lu can commit\n", transaction->id);
            //if another transaction incremented the version first 
            _Atomic uint64_t temp1 = __sync_add_and_fetch(&(region->version), 1);

            //printf("transaction %lu\n", transaction->id);
            //printf("temp %lu\n", temp1);
            if(transaction->version != temp1 - 1){ 
                struct read_node* rn = transaction->rs->head;
                while(rn != NULL){
                    struct lock lock = (struct lock) rn->segment->locks[rn->index];
                    // if the node is read by another, release write locks
                    if(lock.version > transaction->version){
                        //printf("\npatladi 1\n");
                        transaction_cleanup(transaction, false, true, locks);
                        return false;
                    }

                    bool another = false;
                    if(lock.locked){
                        another = true;
                        for(int x = 0; x < transaction->ws->size; x++){
                            if((struct lock*)(locks[x]) == &(lock)){                       
                                another = false;
                                break;
                            }
                        }
                    }
                    //check if the lock was locked by another process
                    
                    if(another){
                        //printf("\nlockedbyanother 2\n");
                        transaction_cleanup(transaction, false, true, locks);
                        return false;
                    }
                    else
                        //printf("\nnolockedbyanother 2\n");

                    rn = rn->next;
                }
            }

            struct write_node* wn = transaction->ws->head;
            while(wn != NULL){
                struct segment_node* s = wn->segment;
                shared_t dest = (shared_t)(s->allocated_area + wn->index * region->align);
                memcpy(dest, wn->word, region->align);
                //printf("\nt %lu committed %lu\n", transaction->id,(wn->word));
                struct lock* locktemp = (struct lock*)&(wn->segment->locks[wn->index]);
                //printf("\nversion before %lu\n", locktemp->version);

                locktemp->version = temp1;
                //printf("\nversion after %lu\n", locktemp->version);
                //printf("\nversion after %lu\n", wn->segment->locks[wn->index].version);
                wn = wn->next;
            }            
            transaction_cleanup(transaction, false, true, locks);
            return true;
        }else{
            printf("\ntransaction %lu cannot commit\n", transaction->id);
            transaction_cleanup(transaction, false, true, locks);
            return false;
        }
        free(locks);
    }
    //printf("\nis read only \n");
    transaction_cleanup(transaction, true, false, NULL);
    //printf("-------------------------------------tm_end end\n");
    return true;
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

    //todo change for multi segment implementation
    struct segment_node* s = region->segmentSingle; //.seg_addresses[((_Atomic unsigned long)source >> 48)]; 
    if(s == NULL){
        transaction_cleanup(transaction, true, false, NULL);
        return false;
    }
    uint64_t noOfWords = size/(region->align);
    uint64_t index = 0;
    void* src = (void*) source; //TODO fake space and multi-segment modification 
    void* trg = (void*) target;

    while(index < noOfWords){
        /*for(int r = 0; r < s->numberOfLocks; r++){
            printf("%d\n", s->locks[r].locked);
        }*/
        struct lock lock = (struct lock) s->locks[(src - s->allocated_area)/region->align];
        uint64_t tempV = lock.version;

        if(transaction->read_only){
            //printf("1read before %lu\n", *((uint64_t*)trg));
            //printf("1read src %lu\n", *((uint64_t*)src));
            memcpy(trg, src, region->align);
            //printf("1read after %lu\n", *((uint64_t*)trg));
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
                //printf("2read before %lu\n", *((uint64_t*)trg));
                //printf("2read src %lu\n", *((uint64_t*)src));
                //printf("copying from existing write\n");
                memcpy(trg, existing_write->word, region->align);  
                //printf("2read after %lu\n", *((uint64_t*)trg));

            } 
            else{
                //printf("3read before %lu\n", *((uint64_t*)trg));
                //printf("3read src %lu\n", *((uint64_t*)src));
                memcpy(trg, src, region->align);
                //printf("3read after %lu\n", *((uint64_t*)trg));
            }
            struct read_node* rn = (struct read_node*) malloc(sizeof(struct read_node));
            rn->next = NULL;
            rn->segment = s;
            rn->index = ((src - s->allocated_area)/region->align);
            rset_append(transaction->rs, rn);
        }

        bool successful = (lock.version == tempV) && (lock.version <= transaction->version) && (!(lock.locked));
        
        if(!successful){
            //printf("not successful\n");
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
    struct transaction* transaction = (struct transaction*) tx;

    //printf("will write %lu/n", *((uint64_t*)source));

    //printf("-------------------------------------tm_write start");

    //printf(transaction->id);
    //return false;
    struct region* region = (struct region*)shared;
    //printf("can %d\n", transaction->id); 

    //TODO change after multi segment imp
    struct segment_node* s = region->segmentSingle;//allocated_segments.seg_addresses[((_Atomic unsigned long)source >> 48)]; //TODO 48 shift
    if(s == NULL){
        transaction_cleanup(transaction, true, false, NULL);
        return false;
    }

    void* src = (void*) source; //TODO fake space and multi-segment modification
    void* trg = (void*) target;
    uint64_t noOfWords = size/(region->align);
    uint64_t index = 0;
    int wc = 1;
    //printf(transaction);
    while(index < noOfWords){
            //printf("index: %lu\n", index);
            //printf("nw: %lu\n", noOfWords);

        //if(transaction->ws == NULL)
            //printf("\nWrite set null!");
        struct write_node* previously_written_node = transaction->ws->head;


        while(previously_written_node != NULL){
            if(previously_written_node->address == (uint64_t) trg){
                //printf("found prev written\n");
                break;
            }
            previously_written_node = previously_written_node->next;
        }

        if(previously_written_node == NULL){
            //return false;
            void * newSpace = aligned_alloc(region->align, region->align); //TODO consider changing to aligned_alloc
            //if failed to allocate memory, clean up and return false
            if(newSpace == NULL){//posix_memalign(&(newSpace), region->align, (region->align * wc)) != 0){
                transaction_cleanup(transaction, true, false, NULL);
                return false;
            }
            
            memcpy(newSpace, src, region->align);
            struct write_node* wn = (struct write_node*) malloc(sizeof(struct write_node));
            //printf("index %lu\n", index);
            //printf("wnindex %lu\n", ((void*)trg - s->allocated_area)/region->align); 

            wn->next = NULL;
            wn->segment = s;
            wn->index = (uint64_t)((trg - s->allocated_area)/region->align); //TODO review
            //printf("assigning %lu to wn index\n", ((trg - s->allocated_area)/region->align));

            wn->word = newSpace;
            wn->address = (uint64_t) (s->allocated_area + wn->index * region->align);
            wset_append(transaction->ws, wn);
            //printf("wrote %lu\n", *((uint64_t*)wn->word));

        }
        else{
            memcpy(previously_written_node->word, src, region->align);
            //printf("wrote %lu\n", *((uint64_t*)previously_written_node->word));

        }
        index += 1;
        trg += region->align;
        src += region->align;
    }
    //printf("\n-------------------------------------tm_write end\n");
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t unused(shared), tx_t unused(tx), size_t unused(size), void** unused(target)) {
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)
    return abort_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) {
    // TODO: tm_free(shared_t, tx_t, void*)
    return false;
}
