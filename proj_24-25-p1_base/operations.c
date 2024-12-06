#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

void write_lock_kvs_mutex(){
  pthread_rwlock_wrlock(kvs_table->table_mutex);
}

void read_lock_kvs_mutex(){
  pthread_rwlock_rdlock(kvs_table->table_mutex);
}

void unlock_kvs_mutex(){
  pthread_rwlock_unlock(kvs_table->table_mutex);
}

int kvs_init() {
  if (kvs_table != NULL) {
    //fprintf(stderr, "KVS state has already been initialized\n");
    write(STDERR_FILENO, "KVS state has already been initialized\n", strlen("KVS state has already been initialized\n"));
    return 1;
  }
  //Inicializamos a mutex que protege as leituras e escritas na tabela
  kvs_table = create_hash_table();
  kvs_table->table_mutex = malloc(sizeof(pthread_rwlock_t));
  pthread_rwlock_init(kvs_table->table_mutex, NULL);
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    write(STDERR_FILENO, "KVS state must be initialized\n", strlen("KVS state must be initialized\n"));
    return 1;
  }
  pthread_rwlock_destroy(kvs_table->table_mutex);
  free(kvs_table->table_mutex);

  free_table(kvs_table);
  kvs_table = NULL;
  //Destruímos a mutex que protege as leituras e escritas na tabela
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], int outputFd) {
  if (kvs_table == NULL) {
    write(STDERR_FILENO, "KVS state must be initialized\n", strlen("KVS state must be initialized\n"));
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      write(outputFd, "Failed to write keypair (", strlen("Failed to write keypair ("));
      write(outputFd, keys[i], strlen(keys[i]));
      write(outputFd, ",", 1);
      write(outputFd, values[i], strlen(values[i]));
      write(outputFd, ")\n", 2);
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int outputFd) {
  if (kvs_table == NULL) {
    write(STDERR_FILENO, "KVS state must be initialized\n", strlen("KVS state must be initialized\n"));
    return 1;
  }
  pthread_rwlock_wrlock(kvs_table->table_mutex);
  write(outputFd, "[", 1);
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      write(outputFd, "(", 1);
      write(outputFd, keys[i], strlen(keys[i]));
      write(outputFd, ",KVSERROR)", strlen(",KVSERROR)"));
    } else {
      write(outputFd,"(", 1);
      write(outputFd, keys[i], strlen(keys[i]));
      write(outputFd,",", 1);
      write(outputFd, result, strlen(keys[i]));
      write(outputFd,")", 1);
    }
    free(result);
  }
  write(outputFd, "]\n", 2);
  pthread_rwlock_unlock(kvs_table->table_mutex);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int outputFd) {
  if (kvs_table == NULL) {
    write(STDERR_FILENO, "KVS state must be initialized\n", strlen("KVS state must be initialized\n"));
    return 1;
  }

  int aux = 0;

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write(outputFd, "[", 1);
        aux = 1;
      }
      write(outputFd, "(", 1);
      write(outputFd, keys[i], strlen(keys[i]));
      write(outputFd, ",KVSMISSING)", strlen(",KVSMISSING)"));    }
  }
  if (aux) {
    write(outputFd, "]\n", 2);
  }

  return 0;
}

void kvs_show(int outputFd) {
  read_lock_kvs_mutex();
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      write(outputFd,"(", 1);
      write(outputFd, keyNode->key, strlen(keyNode->key));
      write(outputFd,",", 1);
      write(outputFd, keyNode->value, strlen(keyNode->value));
      write(outputFd,")\n", 2);
      //printf("(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
    }
  }
  unlock_kvs_mutex();
}

int kvs_backup() {
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}