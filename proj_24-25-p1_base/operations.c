#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "kvs.h"
#include "constants.h"
#include "operations.h"
#include "parser.h"
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <dirent.h>
#include <errno.h>
#include <sys/wait.h>


/* pthread_rwlock_t *table_mutex_for_fork;

void prepare() {
    pthread_rwlock_wrlock(table_mutex_for_fork);
}

void parent() {
    pthread_rwlock_unlock(table_mutex_for_fork);
}

void child() {
    pthread_rwlock_unlock(table_mutex_for_fork);
} */

static struct HashTable* kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    //fprintf(stderr, "KVS state has already been initialized\n");
    write(STDERR_FILENO, "KVS state has already been initialized\n", strlen("KVS state has already been initialized\n"));
    return 1;
  }
  //Inicializamos a mutex que protege as leituras e escritas na tabela
  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    write(STDERR_FILENO, "KVS state must be initialized\n", strlen("KVS state must be initialized\n"));
    return 1;
  }

  free_table(kvs_table);
  kvs_table = NULL;
  //Destruímos a mutex que protege as leituras e escritas na tabela
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], in_out_fds *fd) {
  if (kvs_table == NULL) {
    write(STDERR_FILENO, "KVS state must be initialized\n", strlen("KVS state must be initialized\n"));
    return 1;
  }

  for (size_t i = 1; i < num_pairs; i++) { //ordenar as keys antes de procurá-las na hashtable
    char temp[MAX_STRING_SIZE];
    strcpy(temp, keys[i]);
    size_t j = i;
    while (j > 0 && strcmp(keys[j - 1], temp) > 0) {
      strcpy(keys[j], keys[j - 1]);
      j--;
    }
    strcpy(keys[j], temp);
  }

  int already_locked[LOCKS_AMOUNT] = {0};
  for(size_t i = 0; i < num_pairs; i++){
    int index;
    if (keys[i][0] >= 'A' && keys[i][0] <= 'Z')
      index = keys[i][0] - 'A';
    else
      index = keys[i][0] - 'a';
    if (!already_locked[index]){
      pthread_rwlock_wrlock(&(fd->indiv_locks[index]));
      already_locked[index] = 1;
    }
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      write(fd->output, "Failed to write keypair (", strlen("Failed to write keypair ("));
      write(fd->output, keys[i], strlen(keys[i]));
      write(fd->output, ",", 1);
      write(fd->output, values[i], strlen(values[i]));
      write(fd->output, ")\n", 2);
    }
  }

  for(size_t i = 0; i < num_pairs; i++){
    int index;
    if (keys[i][0] >= 'A' && keys[i][0] <= 'Z')
      index = keys[i][0] - 'A';
    else
      index = keys[i][0] - 'a';
    if (already_locked[index]){
      pthread_rwlock_unlock(&(fd->indiv_locks[index]));
      already_locked[index] = 0;
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], in_out_fds *fd) {
  if (kvs_table == NULL) {
    write(STDERR_FILENO, "KVS state must be initialized\n", strlen("KVS state must be initialized\n"));
    return 1;
  }
  for (size_t i = 1; i < num_pairs; i++) { //ordenar as keys antes de procurá-las na hashtable
    char temp[MAX_STRING_SIZE];
    strcpy(temp, keys[i]);
    size_t j = i;
    while (j > 0 && strcmp(keys[j - 1], temp) > 0) {
      strcpy(keys[j], keys[j - 1]);
      j--;
    }
    strcpy(keys[j], temp);
  }

  int already_locked[LOCKS_AMOUNT] = {0};
  write(fd->output, "[", 1);
  for(size_t i = 0; i < num_pairs; i++){
    int index;
    if (keys[i][0] >= 'A' && keys[i][0] <= 'Z')
      index = keys[i][0] - 'A';
    else
      index = keys[i][0] - 'a';
    if (!already_locked[index]){
      pthread_rwlock_rdlock(&(fd->indiv_locks[index]));
      already_locked[index] = 1;
    }
  }



  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      write(fd->output, "(", 1);
      write(fd->output, keys[i], strlen(keys[i]));
      write(fd->output, ",KVSERROR)", strlen(",KVSERROR)"));
    } else {
      write(fd->output,"(", 1);
      write(fd->output, keys[i], strlen(keys[i]));
      write(fd->output,",", 1);
      write(fd->output, result, strlen(result));
      write(fd->output,")", 1);
    }
    free(result);

  }

  for(size_t i = 0; i < num_pairs; i++){
    int index;
    if (keys[i][0] >= 'A' && keys[i][0] <= 'Z')
      index = keys[i][0] - 'A';
    else
      index = keys[i][0] - 'a';
    if (already_locked[index]){
      pthread_rwlock_unlock(&(fd->indiv_locks[index]));
      already_locked[index] = 0;
    }
  }

  write(fd->output, "]\n", 2);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], in_out_fds *fd) {
  if (kvs_table == NULL) {
    write(STDERR_FILENO, "KVS state must be initialized\n", strlen("KVS state must be initialized\n"));
    return 1;
  }

  int aux = 0;

    for (size_t i = 1; i < num_pairs; i++) { //ordenar as keys antes de procurá-las na hashtable
      char temp[MAX_STRING_SIZE];
      strcpy(temp, keys[i]);
      size_t j = i;
      while (j > 0 && strcmp(keys[j - 1], temp) > 0) {
        strcpy(keys[j], keys[j - 1]);
        j--;
      }
      strcpy(keys[j], temp);
    }

  int already_locked[LOCKS_AMOUNT] = {0};
  write(fd->output, "[", 1);
  for(size_t i = 0; i < num_pairs; i++){
    int index;
    if (keys[i][0] >= 'A' && keys[i][0] <= 'Z')
      index = keys[i][0] - 'A';
    else
      index = keys[i][0] - 'a';
    if (!already_locked[index]){
      pthread_rwlock_wrlock(&(fd->indiv_locks[index]));
      already_locked[index] = 1;
    }
  }


  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write(fd->output, "[", 1);
        aux = 1;
      }
      write(fd->output, "(", 1);
      write(fd->output, keys[i], strlen(keys[i]));
      write(fd->output, ",KVSMISSING)", strlen(",KVSMISSING)"));
    }
  }
  if (aux) {
    write(fd->output, "]\n", 2);
  }

  for(size_t i = 0; i < num_pairs; i++){
    int index;
    if (keys[i][0] >= 'A' && keys[i][0] <= 'Z')
      index = keys[i][0] - 'A';
    else
      index = keys[i][0] - 'a';
    if (already_locked[index]){
      pthread_rwlock_unlock(&(fd->indiv_locks[index]));
      already_locked[index] = 0;
    }
  }

  return 0;
}

void kvs_show(int outputFd) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      write(outputFd,"(", 1);
      write(outputFd, keyNode->key, strlen(keyNode->key));
      write(outputFd,", ", 2);
      write(outputFd, keyNode->value, strlen(keyNode->value));
      write(outputFd,")\n", 2);
      //printf("(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

/*Função para criar o file para colocar o backup: */
int createBackupFile(const char *fileName, int backupNum)
{ // função para criar o backup file
  size_t fileNameLen = strlen(fileName);
  // criar buffer para o nome do arquivo de backup
  char backupFileName[MAX_BACKUP_FILE_NAME_SIZE];
  // copiar nome do file que estamos a copiar
  snprintf(backupFileName, MAX_BACKUP_FILE_NAME_SIZE, "%.*s-%d.bck", (int)(fileNameLen - 4), fileName, backupNum);
  // abrir/criar o arquivo de backup
  int backupFd = open(backupFileName, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
  if (backupFd == -1)
  {
    perror("Couldn't create backup file");
  }

  return backupFd; // return fd ou -1 em caso de erro
}

void kvs_backup(const char *fileName, pthread_mutex_t *backup_mutex, int *backup_counter, int *backupNum, DIR *directory, in_out_fds *fd) {

    //table_mutex_for_fork = fd->table_mutex;
    // Register fork handlers
    //pthread_atfork(prepare, parent, child);
    pid_t pid = fork();  // Cria o processo filho
    if (pid == -1) {  // Erro no fork
        pthread_mutex_unlock(backup_mutex);
        perror("Failed to fork\n");
        (*backup_counter)--;  // Decrementa o contador de backups em caso de erro
        pthread_mutex_lock(backup_mutex);
        return;
    }
    if (pid == 0) {  // Processo filho
        int backupFd = createBackupFile(fileName, *backupNum);
        int exit_code = EXIT_SUCCESS;
        if (backupFd == -1) {
            //pthread_rwlock_rdlock(fd->table_mutex);
            kvs_show(backupFd);
            //pthread_rwlock_unlock(fd->table_mutex);
            exit_code = EXIT_FAILURE;
        }
        close(backupFd);
        closedir(directory);
        //if (fd->threads != NULL)
          //free(fd->threads);
        //fd->threads = NULL;
        cleanFds(fd->input, fd->output);
        //kvs_terminate();
        //pthread_rwlock_destroy(fd->table_mutex);
        free(fd);
        exit(exit_code);
    } else{
      pthread_mutex_lock(backup_mutex);
      (*backupNum)++;
      pthread_mutex_unlock(backup_mutex);
    }

/*     // Wait for all child processes to finish
    while (1) {
    pid_t processId = waitpid(-1, NULL, 0);
      if (processId == -1) {
        if (errno == ECHILD) {
          // No more child processes
          break;
        } else if (errno == EINTR) {
          // Interrupted by a signal, continue waiting
          continue;
        } else {
          perror("Error waiting for child process");
          break;
        }
      }
    } */
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

