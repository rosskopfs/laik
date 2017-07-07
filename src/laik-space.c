/* 
 * This file is part of the LAIK parallel container library.
 * Copyright (c) 2017 Josef Weidendorfer
 */

#include "laik-internal.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

    #include <unistd.h>

// counter for space ID, just for debugging
static int space_id = 0;

// counter for partitioning ID, just for debugging
static int part_id = 0;

// helpers

void setIndex(Laik_Index* i, uint64_t i1, uint64_t i2, uint64_t i3)
{
    i->i[0] = i1;
    i->i[1] = i2;
    i->i[2] = i3;
}

static
int getSpaceStr(char* s, Laik_Space* spc)
{
    switch(spc->dims) {
    case 1:
        return sprintf(s, "[0-%lu]", spc->size[0]-1);
    case 2:
        return sprintf(s, "[0-%lu/0-%lu]",
                       spc->size[0]-1, spc->size[1]-1);
    case 3:
        return sprintf(s, "[0-%lu/0-%lu/0-%lu]",
                       spc->size[0]-1, spc->size[1]-1, spc->size[2]-1);
    }
    return 0;
}


int laik_getIndexStr(char* s, int dims, Laik_Index* idx, bool minus1)
{
    uint64_t i1 = idx->i[0];
    uint64_t i2 = idx->i[1];
    uint64_t i3 = idx->i[2];
    if (minus1) {
        i1--;
        i2--;
        i3--;
    }

    switch(dims) {
    case 1:
        return sprintf(s, "%lu", i1);
    case 2:
        return sprintf(s, "%lu/%lu", i1, i2);
    case 3:
        return sprintf(s, "%lu/%lu/%lu", i1, i2, i3);
    }
    return 0;
}

// is the given slice empty?
bool laik_slice_isEmpty(int dims, Laik_Slice* slc)
{
    if (slc->from.i[0] >= slc->to.i[0])
        return true;

    if (dims>1) {
        if (slc->from.i[1] >= slc->to.i[1])
            return true;

        if (dims>2) {
            if (slc->from.i[2] >= slc->to.i[2])
                return true;
        }
    }
    return false;
}


// returns false if intersection of ranges is empty
static
bool intersectRange(uint64_t from1, uint64_t to1, uint64_t from2, uint64_t to2,
                    uint64_t* resFrom, uint64_t* resTo)
{
    if (from1 >= to2) return false;
    if (from2 >= to1) return false;
    *resFrom = (from1 > from2) ? from1 : from2;
    *resTo = (to1 > to2) ? to2 : to1;
    return true;
}

// get the intersection of 2 slices; return 0 if intersection is empty
Laik_Slice* laik_slice_intersect(int dims, Laik_Slice* s1, Laik_Slice* s2)
{
    static Laik_Slice s;

    if (!intersectRange(s1->from.i[0], s1->to.i[0],
                        s2->from.i[0], s2->to.i[0],
                        &(s.from.i[0]), &(s.to.i[0])) ) return 0;
    if (dims>1) {
        if (!intersectRange(s1->from.i[1], s1->to.i[1],
                            s2->from.i[1], s2->to.i[1],
                            &(s.from.i[1]), &(s.to.i[1])) ) return 0;
        if (dims>2) {
            if (!intersectRange(s1->from.i[2], s1->to.i[2],
                                s2->from.i[2], s2->to.i[2],
                                &(s.from.i[2]), &(s.to.i[2])) ) return 0;
        }
    }
    return &s;
}

static
void laik_slice_sub(int dims, Laik_Slice* s, Laik_Index* from)
{
    s->from.i[0] -= from->i[0];
    s->to.i[0] -= from->i[0];
    if (dims > 1) {
        s->from.i[1] -= from->i[1];
        s->to.i[1] -= from->i[1];
        if (dims > 2) {
            s->from.i[2] -= from->i[2];
            s->to.i[2] -= from->i[2];
        }
    }
}

static
Laik_Slice* sliceFromSpace(Laik_Space* s)
{
    static Laik_Slice slc;

    slc.from.i[0] = 0;
    slc.from.i[1] = 0;
    slc.from.i[2] = 0;
    slc.to.i[0] = s->size[0];
    slc.to.i[1] = s->size[1];
    slc.to.i[2] = s->size[2];

    return &slc;
}

static
int getSliceStr(char* s, int dims, Laik_Slice* slc)
{
    if (laik_slice_isEmpty(dims, slc))
        return sprintf(s, "(empty)");

    int off;
    off  = sprintf(s, "[");
    off += laik_getIndexStr(s+off, dims, &(slc->from), false);
    off += sprintf(s+off, "-");
    off += laik_getIndexStr(s+off, dims, &(slc->to), true);
    off += sprintf(s+off, "]");
    return off;
}


static
int getPartitioningTypeStr(char* s, Laik_PartitionType type)
{
    switch(type) {
    case LAIK_PT_All:    return sprintf(s, "all");
    case LAIK_PT_Block:  return sprintf(s, "block");
    case LAIK_PT_Master: return sprintf(s, "master");
    case LAIK_PT_Copy:   return sprintf(s, "copy");
    default: assert(0);
    }
    return 0;
}

static
int getReductionStr(char* s, Laik_ReductionOperation op)
{
    switch(op) {
    case LAIK_RO_None: return sprintf(s, "none");
    case LAIK_RO_Sum:  return sprintf(s, "sum");
    default: assert(0);
    }
    return 0;
}


static
int getDataFlowStr(char* s, Laik_DataFlow flow)
{
    switch(flow) {
    case LAIK_DF_None:                return sprintf(s, "none");
    case LAIK_DF_CopyIn_NoOut:        return sprintf(s, "copyin");
    case LAIK_DF_NoIn_CopyOut:        return sprintf(s, "copyout");
    case LAIK_DF_CopyIn_CopyOut:      return sprintf(s, "copyinout");
    case LAIK_DF_NoIn_SumReduceOut:   return sprintf(s, "sumout");
    case LAIK_DF_InitIn_SumReduceOut: return sprintf(s, "init-sumout");
    default: assert(0);
    }
    return 0;
}


static
int getTransitionStr(char* s, Laik_Transition* t)
{
    int off = 0;

    if (t->localCount>0) {
        off += sprintf(s+off, "  local: ");
        for(int i=0; i<t->localCount; i++) {
            if (i>0) off += sprintf(s+off, ", ");
            off += getSliceStr(s+off, t->dims, &(t->local[i].slc));
        }
        off += sprintf(s+off, "\n");
    }

    if (t->initCount>0) {
        off += sprintf(s+off, "  init: ");
        for(int i=0; i<t->initCount; i++) {
            if (i>0) off += sprintf(s+off, ", ");
            off += getReductionStr(s+off, t->init[i].redOp);
            off += getSliceStr(s+off, t->dims, &(t->init[i].slc));
        }
        off += sprintf(s+off, "\n");
    }

    if (t->sendCount>0) {
        off += sprintf(s+off, "  send: ");
        for(int i=0; i<t->sendCount; i++) {
            if (i>0) off += sprintf(s+off, ", ");
            off += getSliceStr(s+off, t->dims, &(t->send[i].slc));
            off += sprintf(s+off, " => T%d", t->send[i].toTask);
        }
        off += sprintf(s+off, "\n");
    }

    if (t->recvCount>0) {
        off += sprintf(s+off, "  recv: ");
        for(int i=0; i<t->recvCount; i++) {
            if (i>0) off += sprintf(s+off, ", ");
            off += sprintf(s+off, "T%d => ", t->recv[i].fromTask);
            off += getSliceStr(s+off, t->dims, &(t->recv[i].slc));
        }
        off += sprintf(s+off, "\n");
    }

    if (t->redCount>0) {
        off += sprintf(s+off, "  reduction: ");
        for(int i=0; i<t->redCount; i++) {
            if (i>0) off += sprintf(s+off, ", ");
            off += getReductionStr(s+off, t->red[i].redOp);
            off += getSliceStr(s+off, t->dims, &(t->red[i].slc));
            off += sprintf(s+off, " => %s (%d)",
                           (t->red[i].rootTask == -1) ? "all":"master",
                           t->red[i].rootTask);
        }
        off += sprintf(s+off, "\n");
    }

    if (off == 0) s[0] = 0;
    return off;
}

// is this a reduction?
bool laik_is_reduction(Laik_DataFlow flow)
{
    switch(flow) {
    case LAIK_DF_NoIn_SumReduceOut:
    case LAIK_DF_InitIn_SumReduceOut:
        return true;
    default:
        break;
    }
    return false;
}

// return the reduction operation from data flow behavior
Laik_ReductionOperation laik_get_reduction(Laik_DataFlow flow)
{
    switch(flow) {
    case LAIK_DF_NoIn_SumReduceOut:
    case LAIK_DF_InitIn_SumReduceOut:
        return LAIK_RO_Sum;
    default:
        break;
    }
    return LAIK_RO_None;
}

// do we need to copy values in?
bool laik_do_copyin(Laik_DataFlow flow)
{
    switch(flow) {
    case LAIK_DF_CopyIn_NoOut:
    case LAIK_DF_CopyIn_CopyOut:
        return true;
    default:
        break;
    }
    return false;
}

// do we need to copy values out?
bool laik_do_copyout(Laik_DataFlow flow)
{
    switch(flow) {
    case LAIK_DF_NoIn_CopyOut:
    case LAIK_DF_CopyIn_CopyOut:
        return true;
    default:
        break;
    }
    return false;
}

// do we need to init values?
bool laik_do_init(Laik_DataFlow flow)
{
    switch(flow) {
    case LAIK_DF_InitIn_SumReduceOut:
        return true;
    default:
        break;
    }
    return false;
}



//-----------------------
// Laik_Space

// create a new index space object (initially invalid)
Laik_Space* laik_new_space(Laik_Instance* i)
{
    Laik_Space* space = (Laik_Space*) malloc(sizeof(Laik_Space));

    space->id = space_id++;
    space->name = strdup("space-0     ");
    sprintf(space->name, "space-%d", space->id);

    space->inst = i;
    space->dims = 0; // invalid
    space->first_partitioning = 0;

    // append this space to list of spaces used by LAIK instance
    space->next = i->firstspace;
    i->firstspace = space;

    return space;
}

// create a new index space object with an initial size
Laik_Space* laik_new_space_1d(Laik_Instance* i, uint64_t s1)
{
    Laik_Space* space = laik_new_space(i);
    space->dims = 1;
    space->size[0] = s1;

    if (laik_logshown(1)) {
        char s[100];
        getSpaceStr(s, space);
        laik_log(1, "new 1d space '%s': %s\n", space->name, s);
    }

    return space;
}

Laik_Space* laik_new_space_2d(Laik_Instance* i,
                              uint64_t s1, uint64_t s2)
{
    Laik_Space* space = laik_new_space(i);
    space->dims = 2;
    space->size[0] = s1;
    space->size[1] = s2;

    if (laik_logshown(1)) {
        char s[100];
        getSpaceStr(s, space);
        laik_log(1, "new 2d space '%s': %s\n", space->name, s);
    }

    return space;
}

Laik_Space* laik_new_space_3d(Laik_Instance* i,
                              uint64_t s1, uint64_t s2, uint64_t s3)
{
    Laik_Space* space = laik_new_space(i);
    space->dims = 3;
    space->size[0] = s1;
    space->size[1] = s2;
    space->size[2] = s3;

    if (laik_logshown(1)) {
        char s[100];
        getSpaceStr(s, space);
        laik_log(1, "new 3d space '%s': %s\n", space->name, s);
    }

    return space;
}

// free a space with all resources depending on it (e.g. paritionings)
void laik_free_space(Laik_Space* s)
{
    free(s->name);

    // TODO
}

// set a space a name, for debug output
void laik_set_space_name(Laik_Space* s, char* n)
{
    s->name = strdup(n);
}

// change the size of an index space, eventually triggering a repartitiong
void laik_change_space_1d(Laik_Space* s, uint64_t s1)
{
    assert(s->dims == 1);
    if (s->size[0] == s1) return;

    s->size[0] = s1;

    // TODO: notify partitionings about space change
}

void laik_change_space_2d(Laik_Space* s,
                          uint64_t s1, uint64_t s2)
{
    assert(0); // TODO
}

void laik_change_space_3d(Laik_Space* s,
                          uint64_t s1, uint64_t s2, uint64_t s3)
{
    assert(0); // TODO
}


//-----------------------
// Laik_BorderArray

Laik_BorderArray* allocBorders(int tasks, int capacity)
{
    Laik_BorderArray* a;

    // allocate struct with offset and slice arrays afterwards
    a = (Laik_BorderArray*) malloc(sizeof(Laik_BorderArray) +
                                   (tasks + 1) * sizeof(int) +
                                   capacity * sizeof(Laik_TaskSlice));
    a->off = (int*) ((char*) a + sizeof(Laik_BorderArray));
    a->tslice = (Laik_TaskSlice*) ((char*) a->off + (tasks + 1) * sizeof(int));
    a->tasks = tasks;
    a->capacity = capacity;
    a->count = 0;

    return a;
}

void appendSlice(Laik_BorderArray* a, int task, Laik_Slice* s)
{
    assert(a->count < a->capacity);
    a->tslice[a->count].task = task;
    a->tslice[a->count].s = *s;
    a->count++;
}

static
int ts_cmp(const void *p1, const void *p2)
{
    const Laik_TaskSlice* ts1 = (const Laik_TaskSlice*) p1;
    const Laik_TaskSlice* ts2 = (const Laik_TaskSlice*) p2;
    return ts1->task - ts2->task;
}

void sortBorderArray(Laik_BorderArray* a)
{
    qsort( &(a->tslice[0]), a->count, sizeof(Laik_TaskSlice), ts_cmp);

    int task, off = 0;
    for(task = 0; task < a->tasks; task++) {
        a->off[task] = off;
        while((off < a->count) && (a->tslice[off].task <= task))
            off++;
    }
    a->off[task] = off;
    assert(off == a->count);
}

void clearBorderArray(Laik_BorderArray* a)
{
    // to remove all entries, it's enough to set count to 0
    a->count = 0;
}


//-----------------------
// Laik_Partitioning


// create a new partitioning on a space
Laik_Partitioning* laik_new_partitioning(Laik_Space* s)
{
    Laik_Partitioning* p;
    p = (Laik_Partitioning*) malloc(sizeof(Laik_Partitioning));

    p->id = part_id++;
    p->name = strdup("partng-0     ");
    sprintf(p->name, "partng-%d", p->id);

    p->group = laik_world(s->inst);
    p->space = s;
    p->pdim = 0;
    p->next = s->first_partitioning;
    s->first_partitioning = p;

    p->flow = LAIK_DF_Invalid;
    p->type = LAIK_PT_None;
    p->copyIn = false;
    p->copyOut = false;
    p->redOp = LAIK_RO_None;

    p->partitioner = 0;
    p->base = 0;
    
    p->excluded_tasks = NULL;
    p->n_excluded_tasks = 0;

    p->bordersValid = false;
    p->borders = 0;
    
    p->usedOnCount = 0;
    memset(p->usedOn, 0, sizeof(Laik_Data*)*PARTITIONING_USED_ON_MAX);

    return p;
}

static
void set_flow(Laik_Partitioning* p, Laik_DataFlow flow)
{
    p->flow = flow;
    p->copyIn = laik_do_copyin(flow);
    p->copyOut = laik_do_copyout(flow);
    p->redOp = laik_get_reduction(flow);
}

Laik_Partitioning*
laik_new_base_partitioning(Laik_Space* space,
                           Laik_PartitionType pt,
                           Laik_DataFlow flow)
{
    Laik_Partitioning* p;
    p = laik_new_partitioning(space);
    p->type = pt;
    set_flow(p, flow);

    if (laik_logshown(1)) {
        char s[100];
        getPartitioningTypeStr(s, p->type);
        getDataFlowStr(s+50, p->flow);
        laik_log(1, "new partitioning '%s': type %s, data flow %s, group %d\n",
                 p->name, s, s+50, p->group->gid);
    }

    return p;
}

Laik_Partitioner* laik_get_partitioner(Laik_Partitioning* p)
{
    if (!p->partitioner) {
        switch(p->type) {
        case LAIK_PT_Block:
            p->partitioner = laik_newBlockPartitioner(p);
            break;
        }
    }
    return p->partitioner;
}

void laik_set_partitioner(Laik_Partitioning* p, Laik_Partitioner* pr)
{
    assert(pr->type != LAIK_PT_None);
    p->type = pr->type;
    p->partitioner = pr;
}



// for multiple-dimensional spaces, set dimension to partition (default is 0)
void laik_set_partitioning_dimension(Laik_Partitioning* p, int d)
{
    assert((d >= 0) && (d < p->space->dims));
    p->pdim = d;
}


// create a new partitioning based on another one on the same space
Laik_Partitioning*
laik_new_coupled_partitioning(Laik_Partitioning* base,
                              Laik_PartitionType pt,
                              Laik_DataFlow flow)
{
    Laik_Partitioning* p;
    p = laik_new_partitioning(base->space);
    p->type = pt;
    p->base = base;
    set_flow(p, flow);

    return p;
}

// create a new partitioning based on another one on a different space
// this also needs to know which dimensions should be coupled
Laik_Partitioning*
laik_new_spacecoupled_partitioning(Laik_Partitioning* base,
                                   Laik_Space* s, int from, int to,
                                   Laik_PartitionType pt,
                                   Laik_DataFlow flow)
{
    Laik_Partitioning* p;
    p = laik_new_partitioning(p->space);
    p->type = pt;
    p->base = base;
    set_flow(p, flow);

    assert(0); // TODO

    return p;
}

Laik_Partitioning*
laik_clone_partitioning(const Laik_Partitioning* from){
    Laik_Partitioning* p = (Laik_Partitioning*) 
        malloc (sizeof(Laik_Partitioning));
    p->id = part_id++;
    p->name = strdup("partng-0     ");
    sprintf(p->name, "partng-%d", p->id);

    p->group = from->group;;
    p->space = from->space;
    p->pdim = from->pdim;
    p->next = from->space->first_partitioning;
    p->space->first_partitioning = p;

    p->flow = from->flow;
    p->type = from->type;
    p->copyIn = from->copyIn;
    p->copyOut = from->copyOut;
    p->redOp = from->redOp;

    p->partitioner = malloc(from->partitioner->size);
    memcpy(p->partitioner, from->partitioner, from->partitioner->size);
    p->partitioner->partitioning = p;
    p->base = NULL;
    
    p->excluded_tasks = NULL;
    p->n_excluded_tasks = 0;

    p->bordersValid = false;
    p->borders = 0;
    
    p->usedOnCount = from->usedOnCount;
    memcpy(p->usedOn, from->usedOn, sizeof(Laik_Data*)*PARTITIONING_USED_ON_MAX);
        
    return p;
}

// free a partitioning with related resources
void laik_free_partitioning(Laik_Partitioning* p)
{
    // FIXME: we need some kind of reference counting/GC here

    // TODO
    //free(p->name);
    //free(b->borders);
}

// get number of slices of this task
int laik_my_slicecount(Laik_Partitioning* p)
{
    laik_update_partitioning(p);

    int myid = p->group->myid;
    return p->borders->off[myid+1] - p->borders->off[myid];
}

// get slice number <n> from the slices of this task
Laik_Slice* laik_my_slice(Laik_Partitioning* p, int n)
{
    laik_update_partitioning(p);

    int myid = p->group->myid;
    int o = p->borders->off[myid] + n;
    if (o >= p->borders->off[myid+1]) {
        // slice <n> is invalid
        return 0;
    }
    assert(p->borders->tslice[o].task == myid);
    return &(p->borders->tslice[o].s);
}


// give a partitioning a name, for debug output
void laik_set_partitioning_name(Laik_Partitioning* p, char* n)
{
    p->name = strdup(n);
}


// make sure partitioning borders are up to date
// returns true on changes (if borders had to be updated)
bool laik_update_partitioning(Laik_Partitioning* p)
{
    Laik_BorderArray* baseBorders = 0;
    Laik_Space* s = p->space;
    int pdim = p->pdim;
    int basepdim;

    if (p->base) {
        if (laik_update_partitioning(p->base))
            p->bordersValid = false;

        baseBorders = p->base->borders;
        basepdim = p->base->pdim;
        // sizes of coupled dimensions should be equal
        assert(s->size[pdim] == p->base->space->size[basepdim]);
    }

    if (p->bordersValid)
        return false;

    int count = p->group->size;
    if (!p->borders)
        p->borders = allocBorders(count, 2 * count);
    else
        clearBorderArray(p->borders);
    Laik_BorderArray* ba = p->borders;

    // may trigger creation of partitioner object
    Laik_Partitioner* pr = laik_get_partitioner(p);
    if (pr)
        (pr->run)(pr, ba);
    else {
        Laik_Slice slc;

        switch(p->type) {
        case LAIK_PT_All:
            for(int task = 0; task < ba->tasks; task++) {
                setIndex(&(slc.from), 0, 0, 0);
                setIndex(&(slc.to), s->size[0], s->size[1], s->size[2]);
                appendSlice(ba, task, &slc);
            }
            break;

        case LAIK_PT_Master:
            // only full slice for master
            setIndex(&(slc.from), 0, 0, 0);
            setIndex(&(slc.to), s->size[0], s->size[1], s->size[2]);
            appendSlice(ba, 0, &slc);
            break;

        case LAIK_PT_Copy:
            assert(baseBorders);
            for(int i = 0; i < baseBorders->count; i++) {
                setIndex(&(slc.from), 0, 0, 0);
                setIndex(&(slc.to), s->size[0], s->size[1], s->size[2]);
                slc.from.i[pdim] = baseBorders->tslice[i].s.from.i[basepdim];
                slc.to.i[pdim] = baseBorders->tslice[i].s.to.i[basepdim];
                appendSlice(ba, baseBorders->tslice[i].task, &slc);
            }
            break;

        default:
            assert(0); // TODO
            break;
        }
    }

    sortBorderArray(ba);
    p->bordersValid = true;

    if (laik_logshown(1)) {
        char str[1000];
        int off;
        off = sprintf(str, "partitioning '%s' (group %d) updated: ",
                      p->name, p->group->gid);
        for(int i = 0; i < ba->count; i++) {
            if (i>0)
                off += sprintf(str+off, ", ");
            off += sprintf(str+off, "%d:", p->borders->tslice[i].task);
            off += getSliceStr(str+off, p->space->dims,
                               &(p->borders->tslice[i].s));
        }
        laik_log(1, "%s\n", str);
    }

    return true;
}



// append a partitioning to a partioning group whose consistency should
// be enforced at the same point in time
void laik_append_partitioning(Laik_PartGroup* g, Laik_Partitioning* p)
{
    assert(0); // TODO
}

// Calculate communication required for transitioning between partitionings
Laik_Transition* laik_calc_transitionP(Laik_Partitioning* from,
                                       Laik_Partitioning* to)
{
    Laik_Slice* slc;
    Laik_Transition* t;

    t = (Laik_Transition*) malloc(sizeof(Laik_Transition));
    t->localCount = 0;
    t->initCount = 0;
    t->sendCount = 0;
    t->recvCount = 0;
    t->redCount = 0;


    // either one of <from> and <to> has to be valid; same space, same group
    Laik_Space* space = 0;
    Laik_Group* group = 0;

    // make sure requested data flow is consistent
    if (from == 0) {
        // start: we come from nothing, go to initial partitioning
        assert(to != 0);
        assert(!laik_do_copyin(to->flow));

        space = to->space;
        group = to->group;
    }
    else if (to == 0) {
        // end: go to nothing
        assert(from != 0);
        assert(!laik_do_copyout(from->flow));

        space = from->space;
        group = from->group;
    }
    else {
        // to and from set
        if (laik_do_copyin(to->flow)) {
            // values must come from something
            assert(laik_do_copyout(from->flow) ||
                   laik_is_reduction(from->flow));
        }
        assert(from->space == to->space);
        assert(from->group == to->group);
        space = from->space;
        group = from->group;
    }

    int dims = space->dims;
    int myid = group->inst->myid;
    int count = group->size;
    t->dims = dims;

    Laik_BorderArray* fromBA = from ? from->borders : 0;
    Laik_BorderArray* toBA = to ? to->borders : 0;
    
    if (from) assert(from->bordersValid);
    if (to) assert(to->bordersValid);

    // init values as next phase does a reduction?
    if ((to != 0) && laik_is_reduction(to->flow)) {

        for(int o = toBA->off[myid]; o < toBA->off[myid+1]; o++) {
            if (laik_slice_isEmpty(dims, &(toBA->tslice[o].s))) continue;
            assert(t->initCount < TRANSSLICES_MAX);
            assert(to->redOp != LAIK_RO_None);
            struct initTOp* op = &(t->init[t->initCount]);
            op->slc = toBA->tslice[o].s;
            op->sliceNo = o - toBA->off[myid];
            op->redOp = to->redOp;
            t->initCount++;
        }
    }

    if ((from != 0) && (to != 0)) {

        // determine local slices to keep
        // (may need local copy if from/to mappings are different).
        // reductions are not handled here, but by backend
        if (laik_do_copyout(from->flow) && laik_do_copyin(to->flow)) {
            for(int o1 = fromBA->off[myid]; o1 < fromBA->off[myid+1]; o1++) {
                for(int o2 = toBA->off[myid]; o2 < toBA->off[myid+1]; o2++) {
                    slc = laik_slice_intersect(dims,
                                               &(fromBA->tslice[o1].s),
                                               &(toBA->tslice[o2].s));
                    if (slc == 0) continue;

                    assert(t->localCount < TRANSSLICES_MAX);
                    struct localTOp* op = &(t->local[t->localCount]);
                    op->slc = *slc;
                    op->fromSliceNo = o1 - fromBA->off[myid];
                    op->toSliceNo = o2 - toBA->off[myid];

                    t->localCount++;
                }
            }
        }

        // something to reduce?
        if (laik_is_reduction(from->flow)) {
            // reductions always should involve everyone
            assert(from->type == LAIK_PT_All);
            if (laik_do_copyin(to->flow)) {
                assert(t->redCount < TRANSSLICES_MAX);
                assert((to->type == LAIK_PT_Master) ||
                       (to->type == LAIK_PT_All));

                struct redTOp* op = &(t->red[t->redCount]);
                op->slc = *sliceFromSpace(from->space); // complete space
                op->redOp = from->redOp;
                op->rootTask = (to->type == LAIK_PT_All) ? -1 : 0;

                t->redCount++;
            }
        }

        // something to send?
        if (laik_do_copyout(from->flow)) {
            for(int o1 = fromBA->off[myid]; o1 < fromBA->off[myid+1]; o1++) {
                for(int task = 0; task < count; task++) {
                    if (task == myid) continue;
                    // we may send multiple messages to same task
                    for(int o2 = toBA->off[task]; o2 < toBA->off[task+1]; o2++) {

                        slc = laik_slice_intersect(dims,
                                                   &(fromBA->tslice[o1].s),
                                                   &(toBA->tslice[o2].s));
                        if (slc == 0) continue;

                        assert(t->sendCount < TRANSSLICES_MAX);
                        struct sendTOp* op = &(t->send[t->sendCount]);
                        op->slc = *slc;
                        op->sliceNo = o1 - fromBA->off[myid];
                        op->toTask = task;
                        t->sendCount++;
                    }
                }
            }
        }

        // something to receive not coming from a reduction?
        if (!laik_is_reduction(from->flow) && laik_do_copyin(to->flow)) {
            for(int task = 0; task < count; task++) {
                if (task == myid) continue;
                for(int o1 = fromBA->off[task]; o1 < fromBA->off[task+1]; o1++) {
                    for(int o2 = toBA->off[myid]; o2 < toBA->off[myid+1]; o2++) {

                        slc = laik_slice_intersect(dims,
                                                   &(fromBA->tslice[o1].s),
                                                   &(toBA->tslice[o2].s));
                        if (slc == 0) continue;

                        assert(t->recvCount < TRANSSLICES_MAX);
                        struct recvTOp* op = &(t->recv[t->recvCount]);
                        op->slc = *slc;
                        op->sliceNo = o2 - toBA->off[myid];
                        op->fromTask = task;
                        t->recvCount++;
                    }
                }
            }
        }
    }

    if (laik_logshown(1)) {
        char s[1000];
        int len = getTransitionStr(s, t);
        if (len == 0)
            laik_log(1, "transition %s => %s: (nothing)\n",
                     from ? from->name : "(none)", to ? to->name : "(none)");
        else
            laik_log(1, "transition %s => %s:\n%s",
                     from ? from->name : "(none)", to ? to->name : "(none)",
                     s);
    }

    return t;
}

// Calculate communication for transitioning between partitioning groups
Laik_Transition* laik_calc_transitionG(Laik_PartGroup* from,
                                       Laik_PartGroup* to)
{
    Laik_Transition* t;

    assert(0); // TODO
}

// enforce consistency for the partitioning group, depending on previous
void laik_enforce_consistency(Laik_Instance* i, Laik_PartGroup* g)
{
    assert(0); // TODO
}

// set a weight for each participating task in a partitioning, to be
//  used when a repartitioning is requested
void laik_set_partition_weights(Laik_Partitioning* p, int* w)
{
    assert(0); // TODO
}


// change an existing base partitioning
void laik_repartition(Laik_Partitioning* p, Laik_PartitionType pt)
{
    assert(0); // TODO
}


// couple different LAIK instances via spaces:
// one partition of calling task in outer space is mapped to inner space
void laik_couple_nested(Laik_Space* outer, Laik_Space* inner)
{
    assert(0); // TODO
}


//----------------------------------
// Predefined Partitioners

// Block partitioner

// forward decl
void runBlockPartitioner(Laik_Partitioner* bp, Laik_BorderArray* ba);
void runIncrementalPartitioner(Laik_Partitioner* bp, Laik_BorderArray* ba);

Laik_Partitioner* laik_newBlockPartitioner(Laik_Partitioning* p)
{
    Laik_BlockPartitioner* bp;
    bp = (Laik_BlockPartitioner*) malloc(sizeof(Laik_BlockPartitioner));

    bp->base.type = LAIK_PT_Block;
    bp->base.partitioning = p;
    bp->base.run = runBlockPartitioner;
    bp->base.size = sizeof(Laik_BlockPartitioner);

    bp->cycles = 1;
    bp->getIdxW = 0;
    bp->idxUserData = 0;
    bp->getTaskW = 0;
    bp->taskUserData = 0;
}

Laik_Partitioner* laik_newIncrementalPartitioner(Laik_Partitioning* p)
{
    Laik_IncrementalPartitioner* ip;
    ip = (Laik_IncrementalPartitioner*) malloc(sizeof(Laik_IncrementalPartitioner));

    ip->base.type = LAIK_PT_Block;
    ip->base.partitioning = p;
    ip->base.run = runIncrementalPartitioner;
    ip->base.size = sizeof(Laik_IncrementalPartitioner);

    ip->cycles = 1;
    ip->getIdxW = 0;
    ip->idxUserData = 0;
    ip->getTaskW = 0;
    ip->taskUserData = 0;
    
    ip->prev = p->borders;
}


void laik_set_index_weight(Laik_Partitioning* p, Laik_GetIdxWeight_t f,
                           void* userData)
{
    assert(p->type == LAIK_PT_Block);
    Laik_BlockPartitioner* bp;
    // may create block partitioner object if not existing yet
    bp = (Laik_BlockPartitioner*) laik_get_partitioner(p);

    bp->getIdxW = f;
    bp->idxUserData = userData;

    // borders have to be recalculated
    p->bordersValid = false;
}

void laik_set_task_weight(Laik_Partitioning* p, Laik_GetTaskWeight_t f,
                          void* userData)
{
    assert(p->type == LAIK_PT_Block);
    Laik_BlockPartitioner* bp;
    // may create block partitioner object if not existing yet
    bp = (Laik_BlockPartitioner*) laik_get_partitioner(p);

    bp->getTaskW = f;
    bp->taskUserData = userData;

    // borders have to be recalculated
    p->bordersValid = false;
}

void laik_set_cycle_count(Laik_Partitioning* p, int cycles)
{
    assert(p->type == LAIK_PT_Block);
    Laik_BlockPartitioner* bp;
    // may create block partitioner object if not existing yet
    bp = (Laik_BlockPartitioner*) laik_get_partitioner(p);

    if ((cycles < 0) || (cycles>10)) cycles = 1;
    bp->cycles = cycles;

    // borders have to be recalculated
    p->bordersValid = false;
}

int isExcluded(int t, Laik_Partitioning* p){
    int i = 0;
    for(i=0; i<p->n_excluded_tasks; i++){
        if(p->excluded_tasks[i]->rank == t){
            return 1;
        }
    }
    return 0;
}

int nextIncluded(int t, Laik_Partitioning* p){
    int ret = t;
    while(isExcluded(ret, p))
      ret++;
      
    return ret;
}

void runBlockPartitioner(Laik_Partitioner* pr, Laik_BorderArray* ba)
{
    Laik_BlockPartitioner* bp = (Laik_BlockPartitioner*) pr;
    Laik_Partitioning* p = bp->base.partitioning;
    assert(p->borders != 0);
    assert(p->type == LAIK_PT_Block);

    Laik_Space* s = p->space;
    Laik_Slice slc;
    setIndex(&(slc.from), 0, 0, 0);
    setIndex(&(slc.to), s->size[0], s->size[1], s->size[2]);

    int count = p->group->size;
    int activeCount = count - p->n_excluded_tasks;
    int pdim = p->pdim;
    uint64_t size = s->size[pdim];

    Laik_Index idx;
    double totalW;
    if (bp->getIdxW) {
        // element-wise weighting
        totalW = 0.0;
        setIndex(&idx, 0, 0, 0);
        for(uint64_t i = 0; i < size; i++) {
            idx.i[pdim] = i;
            totalW += (bp->getIdxW)(&idx, bp->idxUserData);
        }
    }
    else {
        // without weighting function, use weight 1 for every index
        totalW = (double) size;
    }

    double totalTW = 0.0;
    if (bp->getTaskW) {
        // task-wise weighting
        totalTW = 0.0;
        for(int task = 0; task < activeCount; task++)
            totalTW += (bp->getTaskW)(task, bp->taskUserData);
    }
    else {
        // without task weighting function, use weight 1 for every task
        totalTW = (double) activeCount;
    }

    double perPart = totalW / activeCount / bp->cycles;
    double w = -0.5;
    int task = 0;
    int cycle = 0;

    // taskW is a correction factor, which is 1.0 without task weights
    // TODO: Move into loop?
    double taskW;
    if (bp->getTaskW)
        taskW = (bp->getTaskW)(task, bp->taskUserData)
                * ((double) activeCount) / totalTW;
    else
        taskW = 1.0;

    slc.from.i[pdim] = 0;
    for(uint64_t i = 0; i < size; i++) {
        if (bp->getIdxW) {
            idx.i[pdim] = i;
            w += (bp->getIdxW)(&idx, bp->idxUserData);
        }
        else
            w += 1.0;

        while (w >= perPart * taskW) {
            w = w - (perPart * taskW);
            
            if ((nextIncluded(task+1, p) == count) && (cycle+1 == bp->cycles)) break;
            slc.to.i[pdim] = i;
            if (slc.from.i[pdim] < slc.to.i[pdim])
                appendSlice(ba, task, &slc);
            task++;
            while(isExcluded(task, p)){
                setIndex(&(slc.from), 0, 0, 0);
                setIndex(&(slc.to), 0, 0, 0);
                appendSlice(ba, task, &slc);
                task++;
            }
            
            if (task == count) {
                task = nextIncluded(0, p);
                cycle++;
            }
            // update taskW
            if (bp->getTaskW)
                taskW = (bp->getTaskW)(task, bp->taskUserData)
                        * ((double) activeCount) / totalTW;
            else
                taskW = 1.0;

            // start new slice
            slc.from.i[pdim] = i;
        }
        if ((nextIncluded(task+1, p) == count) && (cycle+1 == bp->cycles)) break;
    }
    assert(nextIncluded(task+1, p) == count);
    assert(cycle+1 == bp->cycles);
    slc.to.i[pdim] = size;
    appendSlice(ba, task, &slc);
    
    if (laik_myid(p->group) ==1) {
        laik_log(2, "TEST: \n");
        
        int num_borders = ba->count;
        for(int i = 0; i < num_borders; i++)
        {
            laik_log(2, "Border %i(for %i): %i %i\n", i,
                ba->tslice[i].task,
                ba->tslice[i].s.from.i[0],
                ba->tslice[i].s	.to.i[0]);
        }
    }
}

void runIncrementalPartitioner(Laik_Partitioner* pr, Laik_BorderArray* ba)
{
    Laik_IncrementalPartitioner* ip = (Laik_IncrementalPartitioner*) pr;
    // If no previous borders given, just do a fresh block partitioning
    if(!ip->prev)
        runBlockPartitioner(pr, ba);
    
    Laik_Partitioning* p = ip->base.partitioning;
    assert(p->borders != 0);
    assert(p->type == LAIK_PT_Block);
    
    Laik_Space* s = p->space;
    Laik_Slice slc;
    setIndex(&(slc.from), 0, 0, 0);
    setIndex(&(slc.to), s->size[0], s->size[1], s->size[2]);
    
    int count = p->group->size;
    int activeCount = count - p->n_excluded_tasks;
    int pdim = p->pdim;
    uint64_t size = s->size[pdim];

    
    // Compute total weight to be redistirbuted, create border array of all 
    // slices to be freed
    Laik_BorderArray* fba = allocBorders(ba->tasks, ba->capacity);
    
    Laik_Index idx;
    double totalW = 0.0;
    setIndex(&idx, 0, 0, 0);
    for(int sliceNum = 0; sliceNum < ba->count; sliceNum++)
    {
        // If this slice is one to be redistributed
        if(isExcluded(ba->tslice[sliceNum].task, p))
        {
            appendSlice(fba, ba->tslice[sliceNum].task, &ba->tslice[sliceNum].s);
            for(int i = ba->tslice[sliceNum].s.from.i[pdim]; 
                i < ba->tslice[sliceNum].s.to.i[pdim]; i++)
            {
                // Only use index weithitng here. Task weighting is done 
                // on redistribution
                if (ip->getIdxW)
                {
                    idx.i[pdim] = i;
                    totalW += (ip->getIdxW)(&idx, ip->idxUserData);
                }
                else
                {
                    totalW += 1;
                }
            }
        }
    }
    sortBorderArray(fba);
    
    // Compute total task weigth on still remaining tasks
     double totalTW = 0.0;
    if (ip->getTaskW) {
        // task-wise weighting
        totalTW = 0.0;
        for(int task = 0; task < count; task++)
            if(!isExcluded(task, p))
                totalTW += (ip->getTaskW)(task, ip->taskUserData);
    }
    else {
        // without task weighting function, use weight 1 for every task
        totalTW = (double) activeCount;
    }
    
    // Walk over to be free slices, distribute them on still running tasks
    double perPart = totalW / activeCount;
    double w = -0.5;
    int task = 0;
    
    // taskW is a correction factor, which is 1.0 without task weights
    // TODO: Move into loop?
    double taskW;
    if (ip->getTaskW)
        taskW = (ip->getTaskW)(task, ip->taskUserData)
                * ((double) activeCount) / totalTW;
    else
        taskW = 1.0;
       
    uint64_t start = fba->tslice[0].s.from.i[pdim];
    // Iterate over the entire memory to be redistributed
    for(int sliceNum = 0; sliceNum < fba->count; sliceNum++)
    {
        slc.from.i[pdim] = fba->tslice[sliceNum].s.from.i[pdim];
        for(uint64_t i = fba->tslice[sliceNum].s.from.i[pdim];
            i < fba->tslice[sliceNum].s.to.i[pdim]; i++)
        {
            // At weigth for this element
            if (ip->getIdxW) {
                idx.i[pdim] = i;
                w += (ip->getIdxW)(&idx, ip->idxUserData);
            }
            else
                w += 1.0;
        
            while (w >= perPart * taskW) {
                w = w - (perPart * taskW);
                
                if (nextIncluded(task+1, p) == count) break;
                slc.to.i[pdim] = i;
                if (slc.from.i[pdim] < slc.to.i[pdim])
                    appendSlice(ba, task, &slc);
                task++;
                while(isExcluded(task, p)){
                    task++;
                    setIndex(&(slc.from), 0, 0, 0);
                    setIndex(&(slc.to), 0, 0, 0);
                    appendSlice(ba, task, &slc);
                }
                
                // update taskW
                if (ip->getTaskW)
                    taskW = (ip->getTaskW)(task, ip->taskUserData)
                            * ((double) activeCount) / totalTW;
                else
                    taskW = 1.0;

                // start new slice
                slc.from.i[pdim] = i;
            }
            if (nextIncluded(task+1, p) == count) break;
                
            assert(nextIncluded(task+1, p) == count);
            slc.to.i[pdim] = size;
            appendSlice(ba, task, &slc);
        }
        // Switching slice, so adding to current task
        appendSlice(ba, task, &slc);
    }
       
       
}

