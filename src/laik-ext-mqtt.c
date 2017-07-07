
#include "laik/ext-mqtt.h"
#include "laik.h"
#include "laik-internal.h"

#include "../external/MQTT/laik_intf.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>

static LaikExtMsg* ext_message = NULL;

int mqtt_get_message(LaikExtMsg* list)
{
  if(ext_message)
    return 0;
  
  //Save stuff, RACES!!!
  for(int i = 0; i < list->n_failing_nodes; i++)
  {
    printf("I got a message: %s\n", list->failing_nodes[i]);
  }
  
  ext_message = list; 
  return 0;
}

int mqtt_cleanup_()
{
    // Nothing too do
    return 9;
}

void mqtt_init_(Laik_Instance* inst)
{
    //Register with MQTT
    //For now all parameters hardcoded, replace by config file/Enviroment variables later.
    //init_ext_com(&mqtt_get_message, &mqtt_cleanup_,
    //  "127.0.0.1", 1883, 60, NULL, NULL);
        
	//Todo
}

void mqtt_finalize_(Laik_Instance* inst)
{
    // Nothing to do
}

int newId(int id, int* failing)
{
    int new_id = 0;
    if(!failing[id])
      for(int i = 0; i < id; i++)
        if(!failing[i]) new_id++;
    return new_id;
}
        

//Called by application to signal that the partitioning can be changed now
void mqtt_allowRepartitioning_(Laik_Group* current_group)
{
    // Get message
    LaikExtMsg* msg = ext_message;
    
    //Debug, remove later
    LaikExtMsg dbgmsg;
    dbgmsg.n_failing_nodes = 1;
    char *test = "1";
    dbgmsg.failing_nodes = &test;
    msg = &dbgmsg;
    
    //Check if msg is useful
    if(msg == NULL)
        return;
    if(msg->n_failing_nodes == 0)
        return;
        
    // Parse incoming message TODO: Here a more general solution is necessary
    int numMsgNodes = msg->n_failing_nodes;
    int* msgNodes = (int*)malloc(current_group->size*sizeof(int));
    
    for(int i = 0; i < numMsgNodes; i++)
        msgNodes[i] = strtol(msg->failing_nodes[i], NULL, 10);
    
    // Check if this task should switch itself off
    bool killMyself = false;
    for(int i = 0; i < numMsgNodes; i++)
    {
        if(msgNodes[i] == current_group->myid)
        {
            killMyself = true;
            break;
        }
    }
    
    // Exchange information on which nodes kill themselves
    int* failing;
    failing = malloc(current_group->size * sizeof(int));
    current_group->inst->backend->gatherInts(killMyself, failing);
    
    // Compute how many nodes are failing
    int num_failing = 0;
    for(int i = 0; i < current_group->size; i++)
      if(failing[i])
        num_failing++;

    if(!num_failing) return;
    
    // Convert to array of failing Like_Task s
    Laik_Task** failing_tasks = (Laik_Task**) malloc(num_failing*sizeof(Laik_Task*));
    int task_num = 0;
    for(int i = 0; i < current_group->size; i++)
    {
        if(failing[i])
        {
            failing_tasks[task_num] = (Laik_Task*) malloc(sizeof(Laik_Task));
            failing_tasks[task_num]->rank = i;
            task_num++;
        }
    }
    assert(task_num == num_failing);
    
    // Repartition all available partitionings
    Laik_Space* spaceIter = current_group->inst->firstspace;
    while(spaceIter)
    {
        Laik_Partitioning* iter = spaceIter->first_partitioning;
        while(iter)
        {
            // Repartition all BLOCK partitions
            if(iter->type == LAIK_PT_Block)
            {
                if (laik_myid(current_group) == 0) {
                    laik_log(2, "BEFORE: \n");
                    
                    int num_borders = iter->borders->count;
                    for(int i = 0; i < num_borders; i++)
                    {
                        laik_log(2, "Border %i(for %i): %i %i\n", i,
                            iter->borders->tslice[i].task,
                            iter->borders->tslice[i].s.from.i[0],
                            iter->borders->tslice[i].s	.to.i[0]);
                    }
                }
                Laik_Partitioning* clone = laik_clone_partitioning(iter);
                
                clone->flow = LAIK_DF_CopyIn_CopyOut;
                clone->excluded_tasks = failing_tasks;
                clone->n_excluded_tasks = num_failing;
                
                // Check for all data this partitioning is active on
                for(int i = 0; i < PARTITIONING_USED_ON_MAX; i++)
                {
                    if(clone->usedOn[i] 
                        && clone->usedOn[i]->activePartitioning == iter)
                    {
                        laik_set_partitioning(clone->usedOn[i], clone);
                    }
                }
            }
            iter = iter->next;
        } 
        spaceIter = spaceIter->next;
    }
    
    int id = current_group->myid;
    int old_size = current_group->size;
    current_group->size -= num_failing;
    current_group->inst->size -= num_failing;
    
    current_group->myid = newId(id, failing);
    current_group->inst->myid = newId(id, failing);
    
    laik_log(2, "DONE\n\n\n");
    // Perform backup specific changes.
    current_group->inst->backend->switchOffNodes(failing, id);
    laik_log(2, "After backend\n");
    
    // Partitionings have changed, invalidate all borders
    spaceIter = current_group->inst->firstspace;
    while(spaceIter)
    {
        Laik_Partitioning* iter = spaceIter->first_partitioning;
        while(iter)
        {
            if(iter->bordersValid)
                clearBorderArray(iter->borders);
            iter->bordersValid = false;
            // Also reset excluded tasks
            if(iter->n_excluded_tasks > 0)
            {    
                iter->n_excluded_tasks = 0;
                iter->excluded_tasks = NULL;
            }
            
            iter = iter->next;
        }
        spaceIter = spaceIter->next;
    }    
    
    // New borders will be calculated when the partitionings are used next time
    
    // cleanup
    
    free(msgNodes);
    for(int i = 0; i < num_failing; i++)
    {
        free(failing_tasks[i]);
    }
    free(failing_tasks);
}

static Laik_RepartitionControl laik_repartitioningcontrol_mqtt =
{
    mqtt_init_,
    mqtt_finalize_,
    mqtt_allowRepartitioning_
};

Laik_RepartitionControl* init_ext_mqtt(Laik_Instance* inst)
{
  laik_repartitioningcontrol_mqtt.init(inst);
  return &laik_repartitioningcontrol_mqtt;
}

