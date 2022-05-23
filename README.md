# producer-consumer-using-mmap
Repository implements multiple producer consumer model using mmap between multiple processes

You are going to implement the Combiner program using multiple processes. You will be using shared memory and synchronization primitives (mutexes, condition variables, or semaphores). However, in this assignment you need
to use the mmap system call to set up the shared region (for the synchronization primitives as well as for the data) as the mapper and the reducers will be implemented as separate processes. Note
that this requires setting of the synchronization primitives as process shared. 

We also require that anonymous mapping (no backing file) is used with mmap. Your solution must be free of data races and deadlocks.
