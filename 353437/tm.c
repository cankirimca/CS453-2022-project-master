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

struct lock{
    _Atomic unsigned long version;
    _Atomic unsigned long holder;
    _Atomic bool locked;
};

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
    struct lock** locks;
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
    uint64_t id_counter;
    uint64_t version;
    struct segment_node* segmentSingle;
    uint64_t latest_transaction_id; //TODO change
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
    struct write_node* tail;
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
    new_node->next = NULL;
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
    struct read_node* tail;
    uint64_t size;
};

void rset_clear(struct read_set* set){
    struct read_node* head = set->head;
    while(head != NULL){
        struct read_node* temp = head;
        head = head->next;
        free(temp);
    }

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
        set->tail = new_node;
    }
    set->size++;
}

struct segment_set{
    struct segment_node* head;
    struct segment_node* tail;
    int size;
};

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

void transaction_cleanup(struct transaction* t, bool only_sets, bool clear_locks, struct lock** l){
    //TODO make some changes 
    //printf("\n-----------Transaction cleanup----------------\n");
    wset_clear(t->ws);
    rset_clear(t->rs);

    if(only_sets)
        return;

    if(l == NULL)
        return;    

    struct lock** locks = l;
    if(clear_locks){
        for(unsigned long i = 0; i < t->ws->size; i++){
            if(locks[i]->holder == t->id){
                locks[i]->holder = 0;
                locks[i]->locked = false; 
            }                      
        }
    }
    //printf("zwei\n\n");

    free(locks);
    free(t->ws);
    free(t->rs);
}

void freeLocks(struct lock** locks, bool all, uint64_t index){
    for(uint64_t i = 0; i < index; i++){
        free(locks[i]);
    }
    if(all)
        free(locks);
}

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
    region->no_of_segments = 65536;
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
    region->segmentSingle->locks = (struct lock**) malloc(sizeof(struct lock*) * size/align);
    if (unlikely(!region->segmentSingle->locks)) {
        free(region->segmentSingle);
        free(region);
        return invalid_shared;
    }    

    for(unsigned long i = 0; i < region->segmentSingle->numberOfLocks; i++){
        region->segmentSingle->locks[i] = (struct lock*) malloc(sizeof(struct lock) * region->segmentSingle->numberOfLocks); //TODO change later
        if (unlikely(!region->segmentSingle->locks[i])) {
            for(int j = 0; j < i; j++){
                free(region->segmentSingle->locks[j]);
            }
            free(region->segmentSingle->locks);
            free(region->segmentSingle);
            free(region);
            return invalid_shared;
        } 
        region->segmentSingle->locks[i]->holder = 0;
        region->segmentSingle->locks[i]->version = 0;
        region->segmentSingle->locks[i]->locked = false;
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
    struct segment_node* temp_first_node = region->segmentSingle;
    //allocate the first space and free the pointer if the allocation is not successful
    if (posix_memalign(&(temp_first_node->allocated_area), align, size) != 0) {
        freeLocks(region->segmentSingle->locks, true, region->segmentSingle->numberOfLocks);
        free(region->segmentSingle);
        free(region);
        return invalid_shared;
    }
    memset(temp_first_node->allocated_area, 0, size);

    //TODO move to the beginning
    region->start = temp_first_node->allocated_area;
    region->align = align;
    region->id_counter = 0;
    temp_first_node->segment_size = size;
    temp_first_node->alignment = align;
    temp_first_node->segment_id = 0;
    temp_first_node->next = NULL;
    temp_first_node->prev = NULL;
    region->no_of_segments = 1;
    region->version = 0;
    region->latest_transaction_id = 1; //TODO remove
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    printf("-------------------------------------tm_destroy start");
    return;
    // TODO: tm_destroy(shared_t)
    struct region* region = shared;

    /*for(int j = 0; j < (region->no_of_segments); j++){
        free(region->allocated_segments.seg_addresses[j]->allocated_area);
        free(region->allocated_segments.seg_addresses[j]->lock_list);
        free(region->allocated_segments.seg_addresses[j]);
    }
    free(region->allocated_segments.seg_addresses);
    free(region->allocated_segments.locks);
    free(region);*/
    free(region->segmentSingle->allocated_area);
    freeLocks(region->segmentSingle->locks, true, region->segmentSingle->numberOfLocks);
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
    struct region* region = shared;
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

    transaction->ws = (struct write_set*) malloc(sizeof(struct write_set));
    if(unlikely(!transaction->ws)){
        free(transaction);
        return invalid_tx;
    }
    transaction->rs = (struct read_set*)malloc(sizeof(struct read_set));
    if(unlikely(!transaction->rs)){
        free(transaction->ws);
        free(transaction);
        return invalid_tx;
    }

    if(transaction->ws == NULL)
        printf("ws is null again\n");

    transaction->region = region;
    transaction->id = generateTransactionId(region);
    transaction->read_only = is_ro;
    transaction->version = region->version;
    transaction->rs->head = NULL;
    transaction->rs->tail = NULL;
    transaction->rs->size = 0;
    transaction->ws->head = NULL;
    transaction->ws->tail = NULL;
    transaction->ws->size = 0;

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

        struct write_node* temp = head;

        while(temp != NULL){
            //printf("%lu->",temp->index);
            temp = temp->next;
        }
        u_int64_t k = 0;

        while(head != NULL && k < transaction->rs->size){ //TODO consider changing to for loop
            break;
            printf("k: %lu", k);  
            printf("head index: %lu\n", head->index);

            printf("caaaaaaaaaaaan%lu\n\n",(head->index/1));

            locks[k] = (struct lock*) (head->segment->locks + (head->index/1));
            if(k == 10)
                return false;
            head = head->next;
            k++;
        }



        //try to lock all locks
        //if a lock cannot be locked, unlock all locked ones
        bool canCommit = true;
        //printf("size: %lu\n", transaction->ws->size);

        for(unsigned long i = 0; i < transaction->ws->size; i++){
            break;
            printf("releasing locks\n");
            bool* f = malloc(sizeof(bool));
            *f = false;
            if(atomic_compare_exchange_strong(&(locks[i]->locked), f, true)){
                locks[i]->holder = transaction->id;
            }
            else{
                for(unsigned long j = 0; j < i; j++){
                    locks[i]->holder = (locks[i]->holder == transaction->id) ? 0 : locks[i]->holder;
                    locks[i]->locked = (locks[i]->holder == transaction->id) ? false : locks[i]->locked;
                }
                canCommit = false;
                free(f);
                break;
            }
            free(f);
        }

        if(canCommit){            //we can commit
            //printf("\ncan commit\n");
            //if another transaction incremented the version first 
            if(transaction->version != region->version){
                struct read_node* rn = transaction->rs->head;
                while(rn != NULL){
                    struct lock* lock = (struct lock*)(rn->segment->locks + rn->index);
                    // if the node is read by another, release write locks
                    if(lock->version > transaction->version){
                        transaction_cleanup(transaction, false, true, locks);
                        return false;
                    }

                    //check if the lock was locked by another process
                    if(lock->locked && lock->holder != transaction->id){
                        transaction_cleanup(transaction, false, true, locks);
                        return false;
                    }
                    rn = rn->next;
                }
            }

        //TODO change for multi segment implementation
        /*struct segment_set ss = transaction->allocated_segments;
        while(ss.head != NULL){
            struct segment_node* prev = ss.head;
            ss.head = ss.head->next;
            free(prev);
        }

        ss = transaction->free_segments;
        while(ss.head != NULL){
            struct segment_node* prev = ss.head;
            ss.head = ss.head->next;
            free(prev);
        }*/

            struct write_node* wn = transaction->ws->head;
            while(wn != NULL){
                struct segment_node* s = wn->segment;
                shared_t dest = s->allocated_area + wn->index * region->align;
                memcpy(dest, wn->word, region->align);

                region->version++;
                struct lock* lock = (struct lock*)(wn->segment->locks + wn->index);
                lock->version = region->version;
                lock->holder = 0;
                lock->locked = false;

                wn = wn->next;
        }
        
        }else{
            printf("\ncannot commit\n");
            transaction_cleanup(transaction, false, true, locks);
            return false;
        }
        
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
    uint64_t noOfWords = size/region->align;
    uint64_t index = 0;
    void* src = (void*) source; //TODO fake space and multi-segment modification 
    void* trg = (void*) target;
    while(index < noOfWords){
        struct lock* lock = (struct lock*) (s->locks + ((src - s->allocated_area)/region->align));
        uint64_t tempV =lock->version;

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
                if(existing_write->address == (uint64_t)src){
                    break; //break with the value set to the desired node
                }
                existing_write = existing_write->next;
            } //if no break, the value will be null

            if(existing_write != NULL){
                //printf("2read before %lu\n", *((uint64_t*)trg));
                //printf("2read src %lu\n", *((uint64_t*)src));
                memcpy(trg, existing_write->word, region->align);  
                //printf("2read after %lu\n", *((uint64_t*)trg));

            } 
            else{
                //printf("3read before %lu\n", *((uint64_t*)trg));
                //printf("3read src %lu\n", *((uint64_t*)src));
                memcpy(trg, src, region->align);
                //printf("3read after %lu\n", *((uint64_t*)trg));
            }
        }

        /*bool successful = (lock->version == tempV) && (lock->version <= transaction->version) && (!(lock->locked));
        
        if(!successful){
            transaction_cleanup(transaction, true, false, NULL);
            return false;
        }*/
        //printf("index %lu wnindex %lu\n", index, (src - s->allocated_area)/region->align);
        if(transaction->read_only){
            struct read_node* rn = malloc(sizeof(struct read_node));
            rn->next = NULL;
            rn->segment = s;
            rn->index = ((src - s->allocated_area)/region->align);
            rset_append(transaction->rs, rn);
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
        //if(transaction->ws == NULL)
            //printf("\nWrite set null!");
        struct write_node* previously_written_node = transaction->ws->head;


        while(previously_written_node != NULL){
            if(previously_written_node->address == (uint64_t) trg){
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
            wn->index = (uint64_t)((void*)(trg - s->allocated_area))/region->align; //TODO review
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
