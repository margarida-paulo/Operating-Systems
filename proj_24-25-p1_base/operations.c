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
      write(outputFd, result, strlen(result));
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
      write(outputFd,", ", 2);
      write(outputFd, keyNode->value, strlen(keyNode->value));
      write(outputFd,")\n", 2);
      //printf("(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
    }
  }
  unlock_kvs_mutex();
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
    pid_t pid = fork();  // Cria o processo filho
    pthread_mutex_lock(backup_mutex);
    (*backupNum)++;  
    pthread_mutex_unlock(backup_mutex);
    if (pid == -1) {  // Erro no fork
        perror("Failed to fork\n");
        pthread_mutex_lock(backup_mutex);
        (*backup_counter)--;  // Decrementa o contador de backups em caso de erro
        pthread_mutex_unlock(backup_mutex);
        return;
    }
    if (pid == 0) {  // Processo filho
        int backupFd = createBackupFile(fileName, *backupNum); 
        if (backupFd == -1) {
            close(backupFd);
            closedir(directory);
            cleanFds(fd->input, fd->output);     
            //free(fd->threads); 
            free(fd);
            free(kvs_table->table_mutex);
            free_table(kvs_table);
            exit(EXIT_FAILURE);  
        }
        kvs_show(backupFd);  
        close(backupFd);
        closedir(directory);
        //free(fd->threads);
        cleanFds(fd->input, fd->output);
        free(fd);
        free(kvs_table->table_mutex);
        free_table(kvs_table);
        exit(EXIT_SUCCESS);  
    } 
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

