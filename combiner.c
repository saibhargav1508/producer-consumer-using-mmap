// Sai Bhargav Mandavilli
// University of Florida

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <semaphore.h>
#include <unistd.h>

// defining function to handle errors
void errExit(char *errorStr){
	printf("%s", errorStr);
	exit(EXIT_FAILURE);
}

// defining struct for processing tuples
struct my_tuple
{
	char userID[5];
	int action;
	char topic[15];
}**buf_slots;

// initialize done to indicate if thread for a userID has completed
int done[50];

// variables for cmd args
int slots_n, reducer_n;

// initializing semaphores following producer consumer type
sem_t sem_m[50], full[50], empty[50];
int count, *t_count, *slots_in, *slots_out;

int main(int argc, char *argv[])
{	
	// main process is the mapper
	// and child processes after fork() are the reducers
	
	// cmd arguments to read number of slots and number
	// of reducer threads
	slots_n = atoi(argv[1]);
	reducer_n = atoi(argv[2]);
		
	// check to ensure there are enough number of slots
	if(slots_n <= reducer_n)
		slots_n += reducer_n;
		
	// defining address for mapped region
	// we will define a shared and anonymous mapped region 
	// with no backing file
	buf_slots = (struct my_tuple **) mmap (NULL, sizeof(struct my_tuple *) *reducer_n * slots_n, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
	if(buf_slots == MAP_FAILED)
		errExit("mmap1 error");
	
	*buf_slots = (struct my_tuple *) mmap (NULL, sizeof(struct my_tuple) *reducer_n * slots_n, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
	if(*buf_slots == MAP_FAILED)
		errExit("mmap2 error");
	
	// initialize dynamic semaphores
	// create as many semaphores as reducer threads
	for(int i=0; i < reducer_n; i++)
	{
		// pshared flag is set to 1 because semaphores
		// are shared between processes
		
		// init sem_m to 1
		if(sem_init(&sem_m[i], 1, 1) == -1)
			errExit("sem_m err");
		// init full and empty sems to 0
		if(sem_init(&full[i], 1, 0) == -1)
			errExit("sem full err");
			
		if(sem_init(&empty[i], 1, 0) == -1)
			errExit("sem empty err");	
	}
		
	// assigning slots for buffers
	for(int i=0; i < reducer_n; i++)
		buf_slots[i] = (*buf_slots + (i * slots_n));
		
	// allocate memory for slots in buffers
	slots_in = (int *) malloc(reducer_n * sizeof(int));
	if(slots_in == NULL)
		errExit("slots in malloc");
		
	slots_out = (int *) malloc(reducer_n * sizeof(int));
	if(slots_out == NULL)
		errExit("slots out malloc");
	
	t_count = (int *) malloc(reducer_n * sizeof(int));
	if(t_count == NULL)
		errExit("t_count malloc");

	// zero off internal counter and slots between buffers
	for(int i=0; i<reducer_n; i++)
	{
		t_count[i] = 0;
		slots_in[i] = 0;
		slots_out[i] = 0;
	}	
				
	int arr_ct = 0, i = 0, j, k = 0;
	// variable for slot in buffer to allocate memory
	char *buf;
	
	buf = (char *) malloc(2000);
	if(buf == NULL)
		errExit("buf malloc");
	
	// copy tuples from input redirected file to buf
	while (fgets(buf, 2000, stdin) != NULL);
			 
	// count total number of tuples
	while (buf[i] != '\0')
	{
		if (buf[i] == ')') // since each tuple has ')' character, it is used to increment count
	    		count++;
		i++;
	}
			
	// define variables to store tuple arrays
	char *ptr, *arr[count], *arr_cp[count];
	struct my_tuple tup[count];	    

	// initialize tuple array and copy userIDs to tuple structure
	i = 0;
	ptr = strtok(&buf[1], ",");
	strcpy(tup[i].userID, ptr);
				
	// similarly define logic to load scores and topics into tuple structure
	while (ptr != NULL)
	{
		// check till second , to load action character
		ptr = strtok(NULL, ",");
		
		// convert action to integer value
		int a_val = 0;
		switch(*ptr)
		{
			case 'P' :
				a_val = 50;
				break;
			case 'L' :
				a_val = 20;
				break;
			case 'D':
				a_val = -10;
				break;
			case 'C':
				a_val = 30;
				break;
			case 'S':
				a_val = 40;
				break;
		}
		tup[i].action = a_val;	

		// now load topic for each tuple
		ptr = strtok(NULL,")");
		strcpy(tup[i].topic,ptr);

		// terminate after reaching end of tuple list
		if (i == (count-1) ) // we do it count-1 times because index starts with 0
			break;

		// load the next tuple userID
		ptr = strtok(NULL, "(");
		ptr = strtok(NULL, ",");
		strcpy(tup[i+1].userID, ptr);	
			 
		i++;       
	}

	// logic to load all tuples with same userID
	for(i=0; i<count; i++)
	{
		arr[arr_ct] = tup[i].userID;
		arr_ct++;
	}
	
	// copy the tuples into different array
	// this will be used in mapped region
	arr_ct = 0;
	for(i=0; i<count; i++)
	{
		// check all tuples to find matching one
		for(j=0; j<i; j++)
		{
			if((strcmp(arr[i], arr[j])) == 0)
				break;
		}
		// load matching tuple onto arr_cp
		if(i == j)
		{
			arr_cp[arr_ct] = arr[i];
			arr_ct++;			
		}
	}

	// add tuples to slots of buffers in first process, i.e., mapper
	for(i=0; i<reducer_n; i++)
	{
		k = 0; // intialize to zero to load new userID tuple
		// built on model similar to producer consumer using semaphores
		for(j=0; j<count; j++)
		{
			// use the loaded arr_cp for the tuples
			// do not use arr as it will dump the core
			if((strcmp(arr_cp[i], tup[j].userID)) == 0)
			{
				// lock the tuple resource
				if(sem_wait(&sem_m[i]) == -1)
					errExit("sem wait sem_m");
				
				if(slots_in[i] == slots_n)
				{					
					// indicate the other process on empty since a match is found
					if(sem_post(&empty[i]) == -1)
						errExit("sem post empty");

					// match found, unlock the tuple resource so other process can work on it
					if(sem_post(&sem_m[i]) == -1)
						errExit("sem post sem_m");
						
					// wait for other process to indicate it has removed items, then we can lock again and work on next tuple
					if(sem_wait(&full[i]) == -1)
						errExit("sem wait full");
						
					// lock again to check on next tuple
					if(sem_wait(&sem_m[i]) == -1)
						errExit("sem wait sem_m");
					
					k = 0;
					slots_in[i] = 0;
				}
				
				// logic if matching not found and there are more tuples to check
				else if(slots_in[i] < slots_n)
				{
					// load the userId, topic and score into next slot in same buffer
					// HERE WE ARE LOADING INTO THE MAPPED REGION
					strcpy(buf_slots[i][k].userID, tup[j].userID);
					strcpy(buf_slots[i][k].topic, tup[j].topic);
					buf_slots[i][k].action = tup[j].action;
					// increment slot count in same buffer
					slots_in[i]++;
					t_count[i]++;
					k++;	
				}
				
				if(sem_post(&sem_m[i]) == -1)
					errExit("sem post sem_m");
			}
		}
		// set done to indicate that all tuples related to this userID have been loaded onto buffer
		done[i] = 1;
		// indicate for this buffer
		if(sem_post(&empty[i]) == -1)
			errExit("sem post empty");		
	}

	// create reducer_n number of child processes
	// each child process corresponds to one reducer
	for (i=0; i<reducer_n; i++)
	{
		// create child processes using fork
		switch(fork())
		{
			case -1:
				errExit("fork");
			
			// ith child process -> reducer_n times
			case 0:
			{	
				// define struct for processing buffers in child process
				struct my_tuple ch_buf[100];
				
				// defining process local variables
				int slot_count[reducer_n], update, idx = 0;
				// count number of slots for a buffer	
				for(int i=0; i<reducer_n; i++)
					slot_count[i] = 0;
					
				while(1)
				{
					// lock when processing on reducer thread
					if(sem_wait(&sem_m[i]) == -1)
						errExit("sem wait sem_m");
					
					while(slots_in[i]-slots_out[i]==0 && done[i]==0)
					{
						// similar to logic used in mapper process above	
						if(sem_post(&sem_m[i]) == -1)
							errExit("sem post sem_m");
						if(sem_wait(&empty[i]) == -1)
							errExit("sem wait empty");
						if(sem_wait(&sem_m[i]) == -1)
							errExit("sem wait sem_m");
					}
						
					// enter processing condition only when there are slots filled inside buffer
					while(slots_in[i] - slots_out[i] > 0)
					{
						slot_count[i]++;
	
						// loop in all slots to check for loading tuples and checking for similar topics
						for(int j=0; j<slots_in[i]; j++)
						{	
							slots_out[i]++;
							if((j == 0) && (t_count[i] > slots_n))
							{
								// load the userID, topic and action score onto buffer
								// HERE WE ARE LOADING FROM THE MAPPED REGION						
								strcpy(ch_buf[idx].userID, buf_slots[i][j].userID);
								strcpy(ch_buf[idx].topic, buf_slots[i][j].topic);
								ch_buf[idx].action = buf_slots[i][j].action;
								// increment index to point to next slot in buffer
								idx++;
							}
							
							else
							{
								// check for each slot in buffer for matching topics 
								// this is to find total score
								for(int l=0; l<idx; l++)
								{
									if(((strcmp(buf_slots[i][j].topic, ch_buf[l].topic)) == 0))
									{
										// if matching topic found, update score
										ch_buf[l].action += buf_slots[i][j].action;
										// set flag to show that updation operation is done
										update = 1;
										break;	
									}
								}
									
								// if updation is not done, new tuple is copied onto buffer
								if (update != 1)
								{
									strcpy(ch_buf[idx].userID, buf_slots[i][j].userID);
									strcpy(ch_buf[idx].topic, buf_slots[i][j].topic);
									ch_buf[idx].action = buf_slots[i][j].action;
									idx++;
								}	
								// reset update flag for next iteration
								update = 0;
							}
						}
					}
					
					// print the updated tuples in buffer slots
					// do this only if done is set from mapper
					if((done[i] == 1) && (slots_in[i] == slots_out[i]))
					{
						for(int k=0; k<idx; k++)
							printf("(%s, %s, %d)\n", ch_buf[k].userID, ch_buf[k].topic, ch_buf[k].action);
						
						// unlock semaphore
						if(sem_post(&sem_m[i]) == -1)
							errExit("sem post sem_m");
						
						break;
					}
					// zero out slots count for next topic
					slots_out[i] = 0;
					slots_in[i] = 0;
					
					// indicate mapper process on completion
					if(sem_post(&full[i]) == -1)
						errExit("sem post full");
					
					if(sem_post(&sem_m[i]) == -1)
						errExit("sem post sem_m");
				}
				
				// terminate the process because parent will be waiting on child
				exit(0);
			// closing case 0
			}
		
			// mapper process (main) accessed in default
			default:
				break;
		}
	}

	// wait until all reducer process childs terminate
	while(wait(NULL) > 0){;}

	exit(EXIT_SUCCESS);
}
