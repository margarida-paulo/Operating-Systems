#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>
#include <pthread.h>
#include "constants.h"
#include <dirent.h>


// @brief Estrutura que guarda os file descriptors e outras informações necessárias para cada tarefa.
typedef struct fds{
  int input; // File descriptor of the file that we are reading from
  int output; // File descriptor of the output file
  int max_backups; // Máximo de backups que podem acontecer em simultâneo
  int backupNum;
  char *fileName;
  DIR *dir;
  pthread_t *threads;
  pthread_rwlock_t *indiv_locks;
} in_out_fds;

typedef struct generalInfo{
  struct dirent *fileDir;
  int max_backups;
  DIR *dir;
  pthread_t *threads;
} info;

/// Initializes the KVS state.
/// @return 0 if the KVS state was initialized successfully, 1 otherwise.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 if the KVS state was terminated successfully, 1 otherwise.
int kvs_terminate();

/// Writes a key value pair to the KVS. If key already exists it is updated.
/// @param num_pairs Number of pairs being written.
/// @param keys Array of keys' strings.
/// @param values Array of values' strings.
/// @return 0 if the pairs were written successfully, 1 otherwise.
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], in_out_fds *outputFd);

/// Reads values from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write the (successful) output.
/// @return 0 if the key reading, 1 otherwise.
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], in_out_fds *fd);

/// Deletes key value pairs from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], in_out_fds *fd);

/// Writes the state of the KVS.
/// @param fd File descriptor to write the output.
void kvs_show(int outputFd);

/// Creates a backup of the KVS state and stores it in the correspondent
/// backup file
/// @return 0 if the backup was successful, 1 otherwise.
void kvs_backup(const char *fileName, pthread_mutex_t *backup_mutex, int *backup_counter, int *backupNum, DIR *directory, in_out_fds *fd);

/// Waits for the last backup to be called.
void kvs_wait_backup();

/// Waits for a given amount of time.
/// @param delay_us Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

void *tableOperations(void *fd_info);


#endif  // KVS_OPERATIONS_H
