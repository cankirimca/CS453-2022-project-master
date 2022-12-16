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
#include "macros.h"

//a struct denoting the segments in which the users write or read
typedef struct segment_node{
    struct segment_node* prev; //ptr to next segment
    struct segment_node* next; //ptr to previous segment
    uint64_t segment_id;       //id of the segment
    uint64_t segment_size;     //size of the segment
    uint64_t alignment;        //alignment = size of a word 
    bool is_allocated;     
    shared_t allocated_area;        //pointer to the allocated space 
    struct lock_list lock_list;
};

typedef struct segment_list{
    struct segment_node** seg_addresses;
    unsigned int number_of_segments;
    _Atomic bool* locks;
};

typedef struct region {
    //struct shared_lock_t lock;
    void* start;     
    struct segment_list allocated_segments; 
    size_t size;       
    size_t align;  
    unsigned int no_of_segments;
    uint64_t id_counter;
    uint64_t version;
};

//struct representing a single write node
typedef struct write_node{
    struct write_node* next;
    struct segment_node* segment;
    u_int64_t index;
    shared_t word;
};
//struct representing the write set containing multiple write_nodes
typedef struct write_set{
    struct write_node* head;
    struct read_node* tail;
    uint64_t size;
};

void wset_clear(struct write_set* set){
    struct write_node* head = set->head;
    while(head != NULL){
        struct write_node* temp = head;
        head = head->next;
        free(temp);
        free(temp->word);
    }
}

void wset_append(struct write_set* set, struct write_node* new_node){
    struct write_node* head = set->head;
    struct write_node* prev = NULL;
    while(head != NULL){
        prev = head;
        head = head->next;
    }
    if(prev != NULL){
        prev->next = new_node;
        set->tail = new_node;
    }
}


//struct representing a single read node
typedef struct read_node{
    struct read_node* next;
    struct segment_node* segment;
    u_int64_t index;
    shared_t word;
};
//struct representing the read set containing multiple read_nodes
typedef struct read_set{
    struct read_node* head;
    struct read_node* tail;
    uint64_t size;
};

void rset_clear(struct read_set* set){
    struct read_node* head = set->head;
    while(head != NULL){
        struct write_node* temp = head;
        head = head->next;
        free(temp);
        free(temp->word);
    }
}

void rset_append(struct read_set* set, struct read_node* new_node){
    struct read_node* head = set->head;
    struct read_node* prev = NULL;
    while(head != NULL){
        prev = head;
        head = head->next;
    }
    if(prev != NULL){
        prev->next = new_node;
        set->tail = new_node;
    }
}

typedef struct segment_set{
    struct segment_node* head;
    struct segment_node* tail;
    int size;
};

typedef struct transaction{
    uint64_t id;
    uint64_t version;
    bool read_only;
    struct read_set rs;
    struct write_set ws;
    struct segment_set allocated_segments;
    struct segment_set free_segments;
    struct region* region;
};

void transaction_cleanup(struct transaction* t, bool only_sets, bool clear_locks, struct lock** l){
    //TODO make some changes 

    wset_clear(&(t->ws));
    rset_clear(&(t->rs));

    if(only_sets)
        return;

    if(l == NULL)
        return;    

    struct lock** locks = l;
    if(clear_locks){
        for(int i = 0; i < (t->ws).size; i++){
            locks[i]->holder = (locks[i]->holder == t->id) ? 0 : locks[i]->holder;
            locks[i]->locked = (locks[i]->holder == t->id) ? false : locks[i]->locked;                       
        }
    } 
    free(locks);
}

typedef struct lock{
    _Atomic unsigned long version;
    _Atomic unsigned long holder;
    _Atomic bool locked;
};

typedef struct lock_list{
    struct lock* list_ptr;
    unsigned long size;
};

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
    // TODO: tm_create(size_t, size_t)
    if(size % align != 0)
        return invalid_shared;
    if(!isPowerOfTwo(align)) 
        return invalid_shared;

    //initialize region pointer and check if it is unlikely
    struct region* region = malloc(sizeOf(region)); 
    if (unlikely(!region)) {
        return invalid_shared;
    }
    region->no_of_segments = 65536;
    //initialize region segments pointer and check if it is unlikely
    region->allocated_segments.seg_addresses = (struct segment_node**) malloc(sizeof(struct segment_node*) * region->no_of_segments); //TODO change later
    region->allocated_segments.locks = malloc(sizeof(_Atomic bool) * region->no_of_segments); //TODO change later
    if (unlikely(!region->allocated_segments.seg_addresses)) {
        return invalid_shared;
    }
    else if (unlikely(!region->allocated_segments.locks)) {
        return invalid_shared;
    }

    struct segment_list temp = region->allocated_segments;
    for(int j = 0; j < region->no_of_segments; j++){
        temp.seg_addresses[j] = NULL;
        temp.locks[j] = false;
    }

    //TODO initialize previous two mallocs

    //allocate the starting region
    region->allocated_segments.seg_addresses[0] = malloc(sizeof(struct segment_node));

    struct segment_node* temp_first_node = (struct segment_node*)(region->allocated_segments.seg_addresses[0]);

    //allocate the first space and free the pointer if the allocation is not successful
    if (posix_memalign(&(temp_first_node->allocated_area), align, size) != 0) {
        free(temp_first_node);
        free(region);
        return invalid_shared;
    }

    memset(temp_first_node->allocated_area, 0, size);
    temp_first_node->lock_list.size = size/align; //lock size is equal to the number of words
    temp_first_node->lock_list.list_ptr = (struct lock*) malloc(sizeof(struct lock)*(size/align));

    if(unlikely(!temp_first_node->lock_list.list_ptr)){
        free(temp_first_node->allocated_area);
        free(temp_first_node);
        free(region);
        return invalid_shared;
    }

    for(int j = 0; j < size/align; j++){
        temp_first_node->lock_list.list_ptr[j].holder = 0;
        temp_first_node->lock_list.list_ptr[j].version = 0;
        temp_first_node->lock_list.list_ptr[j].locked = false;
    }

    region->start = temp_first_node;
    region->align = align;
    region->id_counter = 0;
    temp_first_node->segment_size = size;
    temp_first_node->alignment = align;
    temp_first_node->segment_id = 0;
    return invalid_shared;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    // TODO: tm_destroy(shared_t)
    struct region* region = shared;
    for(int j = 0; j < (region->no_of_segments); j++){
        free(region->allocated_segments.seg_addresses[j]->allocated_area);
        free(region->allocated_segments.seg_addresses[j]->lock_list);
        free(region->allocated_segments.seg_addresses[j]);
    }
    free(region->allocated_segments.seg_addresses);
    free(region->allocated_segments.locks);
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t unused(shared)) {
    // TODO: tm_start(shared_t)
    return NULL;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    struct region* region = shared;
    struct segment_node* first_segment = region->allocated_segments.seg_addresses[0];
    return first_segment->segment_size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t unused(shared)) {
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
    struct transaction* transaction = malloc(sizeof(transaction));
    struct region* region = shared;

    if(unlikely(!transaction))
        return invalid_tx;

    transaction->region = region;
    transaction->id = generateTransactionId(region);
    transaction->read_only = is_ro;
    transaction->version = region->version;
    transaction->rs.head = NULL;
    transaction->rs.tail = NULL;
    transaction->rs.size = 0;
    transaction->ws.head = NULL;
    transaction->ws.tail = NULL;
    transaction->ws.size = 0;
    transaction->allocated_segments.head = NULL;
    transaction->allocated_segments.tail = NULL;
    transaction->allocated_segments.size = 0;
    transaction->free_segments.head = NULL;
    transaction->free_segments.tail = NULL;
    transaction->free_segments.size = 0;

    return transaction->id;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) {
    struct transaction* transaction = tx; 
    if(!transaction->read_only){
        //get all the locks associated with the write set of the transaction
        struct lock** locks = malloc(sizeof(struct lock) * transaction->ws.size);
        struct write_node* head = transaction->ws.head;
        u_int64_t k = 0;
        while(head != NULL){ //TODO consider changing to for loop
            locks[k] = (head->segment + (head->index/1))->lock_list.list_ptr;
            head = head->next;
            k++;
        }
        //try to lock all locks
        //if a lock cannot be locked, unlock all locked ones
        bool canCommit = true;
        for(int i = 0; i < transaction->ws.size; i++){
            bool* f = malloc(sizeof(bool));
            *f = false;
            if(atomic_compare_exchange_strong(&(locks[i]->locked), f, true)){
                locks[i]->holder = transaction->id;
            }
            else{
                for(int j = 0; j < i; j++){
                    locks[i]->holder = (locks[i]->holder == transaction->id) ? 0 : locks[i]->holder;
                    locks[i]->locked = (locks[i]->holder == transaction->id) ? false : locks[i]->locked;
                }
                canCommit = false;
                free(f);
                break;
            }
            free(f);
        }

        struct region* region = shared;
        if(canCommit){            //we can commit
            //if another transaction incremented the version first 
            if(transaction->version != region->version){
                struct read_node* rn = transaction->rs.head;
                while(rn != NULL){
                    struct lock* lock = (rn->segment->lock_list).list_ptr + (rn->index);
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

        struct segment_set ss = transaction->allocated_segments;
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
        }

        struct write_node* wn = transaction->ws.head;
        while(wn != NULL){
            struct segment_node* s = wn->segment;
            shared_t dest = s->allocated_area + wn->index * region->align;
            memcpy(dest, wn->word, region->align);

            region->version++;
            struct lock* lock = (wn->segment->lock_list).list_ptr + (wn->index);
            lock->version = region->version;
            lock->holder = 0;
            lock->locked = false;

            wn = wn->next;
        }
        
        }else{
            transaction_cleanup(transaction, false, true, locks);
            return false;
        }
        
    }
    transaction_cleanup(transaction, true, false, NULL);
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
    struct transaction* transaction = tx;
    struct region* region = shared;

    struct segment_node* s = region->allocated_segments.seg_addresses[((_Atomic unsigned long)source >> 48)]; 
    if(s == NULL){
        transaction_cleanup(transaction, true, false, NULL);
    }
    uint64_t noOfWords = size/region->align;
    uint64_t index = 0;
    void* src = s->allocated_area + ; //TODO fake space 
    void* trg = target;
    while(index < noOfWords){
        struct lock* lock = (s->lock_list).list_ptr + ((src - s->allocated_area)/region->align);
        index += 1;
        trg += region->align;
        src += region->align;
    }
    return false;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t unused(shared), tx_t unused(tx), void const* unused(source), size_t unused(size), void* unused(target)) {
    // TODO: tm_write(shared_t, tx_t, void const*, size_t, void*)
    return false;
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
