
#define _DEFAULT_SOURCE

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <pthread.h>
#include <sys/wait.h>
#include <semaphore.h>
#include <errno.h>

pthread_mutex_t backup_mutex = PTHREAD_MUTEX_INITIALIZER; // o mutex pros backups é inicializado
int backup_counter = 0;                                   // counter para o numero de backups em simultaneo

/* Para o exercicio 3, temos de criar tarefas para o programa conseguir tratar de vários ficheiros
 .job em simultâneo. Para isso, vamos usar threads, com mutexes de leitura e escrita para proteger
 a manipulação da tabela. Para não ultrapassar o número máximo de threads, usaremos semáforos.
 */

// Semaforo que garante que não se ultrapassa max_threads em simultâneo
sem_t semaforo_max_threads;

/* pthread_mutex_t active_threads_mutex = PTHREAD_MUTEX_INITIALIZER; // Mutex para controlar que threads estão ativos é inicializado
 */


// Esta é a função que vai fazer as operações na tabela, que vai ser chamada em threads.
void *tableOperations(void *fd_info){
        //printf("THREAD CREATED\n");
        in_out_fds *fd = fd_info;
        enum Command fileOver = 0;
        while (fileOver != EOC)
        {
          char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
          char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
          unsigned int delay;
          size_t num_pairs;

          switch (fileOver = get_next(fd->input))
          {
          case CMD_WRITE:
            num_pairs = parse_write(fd->input, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
            if (num_pairs == 0)
            {
              write(fd->output, "Invalid command. See HELP for usage\n", strlen("Invalid command. See HELP for usage\n"));
              continue;
            }

            if (kvs_write(num_pairs, keys, values, fd->output))
            {
              write(fd->output, "Failed to write pair\n", strlen("Failed to write pair\n"));
            }

            break;

          case CMD_READ:
            num_pairs = parse_read_delete(fd->input, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

            if (num_pairs == 0)
            {
              write(fd->output, "Invalid command. See HELP for usage\n", strlen("Invalid command. See HELP for usage\n"));
              continue;
            }

            if (kvs_read(num_pairs, keys, fd->output))
            {
              write(fd->output, "Failed to read pair\n", strlen("Failed to read pair\n"));
            }
            break;

          case CMD_DELETE:
            num_pairs = parse_read_delete(fd->input, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

            if (num_pairs == 0)
            {
              write(fd->output, "Invalid command. See HELP for usage\n", strlen("Invalid command. See HELP for usage\n"));
              continue;
            }

            if (kvs_delete(num_pairs, keys, fd->output))
            {
              write(fd->output, "Failed to delete pair\n", strlen("Failed to delete pair\n"));
            }
            break;

          case CMD_SHOW:

            kvs_show(fd->output);
            break;

          case CMD_WAIT:
            if (parse_wait(fd->input, &delay, NULL) == -1)
            {
              write(fd->output, "Invalid command. See HELP for usage\n", strlen("Invalid command. See HELP for usage\n"));
              continue;
            }

            if (delay > 0)
            {
              write(fd->output, "Waiting...\n", strlen("Waiting...\n"));
              kvs_wait(delay);
            }
            break;

          case CMD_BACKUP:
            if (backup_counter >= fd->max_backups)
            {
              // Espera até que algum processo filho termine
              pid_t finished_pid = wait(NULL);
              if (finished_pid == -1)
              {
                perror("Erro ao esperar pelo processo filho\n");
              }
              else
              {
                printf("Processo filho %d terminou\n", finished_pid); // Debug
                kvs_backup(fd->fileName, &backup_mutex, &backup_counter, &(fd->backupNum), fd->dir, fd);
              }
            }
            else
            {
              pthread_mutex_lock(&backup_mutex);
              backup_counter++;
              pthread_mutex_unlock(&backup_mutex);
              kvs_backup(fd->fileName, &backup_mutex, &backup_counter, &(fd->backupNum), fd->dir, fd);
            }
            break;

          case CMD_INVALID:
            write(fd->output, "Invalid command. See HELP for usage\n", strlen("Invalid command. See HELP for usage\n"));
            break;

          case CMD_HELP:
            write(fd->output,
              "Available commands:\n"
              "  WRITE [(key,value)(key2,value2),...]\n"
              "  READ [key,key2,...]\n"
              "  DELETE [key,key2,...]\n"
              "  SHOW\n"
              "  WAIT <delay_ms>\n"
              "  BACKUP\n" // Not implemented
              "  HELP\n", strlen("Available commands:\n"
              "  WRITE [(key,value)(key2,value2),...]\n"
              "  READ [key,key2,...]\n"
              "  DELETE [key,key2,...]\n"
              "  SHOW\n"
              "  WAIT <delay_ms>\n"
              "  BACKUP\n" // Not implemented
              "  HELP\n"));

            break;

          case CMD_EMPTY:
            break;

          case EOC:
            cleanFds(fd->input, fd->output);
            break;
          }
        }
        cleanFds(fd->input, fd->output);
        //free(fd);
        //printf("THREAD FINISHED\n");
        sem_post(&semaforo_max_threads);
        return NULL;
}

int main(int argc, char *argv[])
{
  if(argc!=4){
    write(STDERR_FILENO, "Wrong arguments.\n", strlen("Wrong arguments.\n")); //perguntar ao stor
    write(STDERR_FILENO, "Usage: ./kvs [FOLDER_NAME] [max_threads(>0)]\n", strlen("Usage: ./kvs [FOLDER_NAME] [max_threads]\n")); //perguntar ao stor  
    return (EXIT_FAILURE);
  }

  //O número máximo de backups em simultaneo é dado pelo input do user
  int max_backups = atoi(argv[2]);
  //Definimos o número máximo de threads que podemos ter, a partir do input do utilizador
  int max_threads = atoi(argv[3]);

  // Verificamos se o atoi retornou 0, o que acontece quando há algum erro. Neste caso, consideramos também que se o utilizador escolher
  // 0 como max_threads ou como max_backups, o programa também não corre. Verificamos também se não foram colocados números negativos.
  if ((max_threads <= 0) | (max_backups <= 0)){
    write(STDERR_FILENO, "Wrong arguments.\n", strlen("Wrong arguments.\n")); //perguntar ao stor
    write(STDERR_FILENO, "Usage: ./kvs [FOLDER_NAME] [max_threads(>0)]\n", strlen("Usage: ./kvs [FOLDER_NAME] [max_threads]\n")); //perguntar ao stor
    return (EXIT_FAILURE);
  }

  // aqui na main eu abro a diretoria e vejo se ela existe (!= NULL)
  char *dirPath = argv[1];
  DIR *dir = opendir(dirPath);
  if (dir == NULL)
  {
    perror("Couldn't open directory");
    return(EXIT_FAILURE);
  }
  if (chdir(dirPath) == -1){
    perror("Couldn't go inside directory");
    closedir(dir);
    return(EXIT_FAILURE);
  }
  // A inicialização da tabela estava a ser feita dentro do while, mas tem de ser fora porque
  // a tabela é única, todos os ficheiros escrevem para o mesmo sítio.
  if (kvs_init())
  {
    //fprintf(stderr, "Failed to initialize KVS\n");
    write(STDERR_FILENO, "Failed to initialize KVS\n", strlen("Failed to initialize KVS\n"));
    closedir(dir);
    return 1;
  }

  struct dirent *fileDir;
  sem_init(&semaforo_max_threads, 0, (unsigned int)max_threads);
  pthread_t *threads = malloc(0); //Array para guardar as threads, para, no final, podermos fazer join.
  size_t countThreads = 0;
  while ((fileDir = readdir(dir)) != NULL)
  { // leio a diretoria e dentro deste while tenho de fazer open_file para os ficheiros do tipo ".job"
    struct stat fileStat;
    if (stat(fileDir->d_name, &fileStat) == -1)
    {
        perror("Couldn't stat file");
        continue;
    }

    if (S_ISREG(fileStat.st_mode))
    {
      // verifica se é um regular file
      const char *fileName = fileDir->d_name;
      size_t fileNameLength = strlen(fileName);
      // só leio ficheiros do tipo ".job":
      if (MAX_JOB_FILE_NAME_SIZE > fileNameLength && fileNameLength > 4 && strcmp(fileName + (fileNameLength - 4), ".job") == 0) {
        int fd = open(fileDir->d_name, O_RDONLY);
        //O backupNum é um counter para o numero de backups de cada file NO TOTAL e é inicializado quando abrimos um ficheiro, porque cada ficheiro tem o seu backupNum
        int backupNum = 0;
        int outputFd;
        if(fd==-1){
          perror("Couldn't open file");
          continue;
        }
        if ((outputFd = outputFile(fileName)) == -1)
          continue;
        // Temos de criar esta estrutura para guardar os fd's para conseguirmos enviar à função da thread.
        threads = realloc(threads, (countThreads + 1) * sizeof(threads));
        //in_out_fds *fds = malloc(sizeof(in_out_fds));
        in_out_fds fds;
        fds.input = fd;
        fds.output = outputFd;
        fds.max_backups = max_backups;
        fds.backupNum = backupNum;
        fds.fileName = fileName;
        fds.dir = dir;
        fds.threads = threads;
        sem_wait(&semaforo_max_threads);
        if (pthread_create(&(threads[countThreads]), NULL, &tableOperations, &fds) != 0){
          write(STDERR_FILENO, "Error in creating thread\n", strlen("Error in creating thread\n"));
          sem_post(&semaforo_max_threads);
          cleanFds(fds.input, fds.output);
          //free (fds);
        } else
            countThreads++;
      }
    }
  }
  for (size_t i = 0; i < countThreads; i++){
      pthread_join(threads[i], NULL);
  }

    // Wait for all child processes to finish
  while (1) {
    pid_t pid = waitpid(-1, NULL, 0);
      if (pid == -1) {
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
    }
  sem_destroy(&semaforo_max_threads);
  free(threads);
  closedir(dir);
  if (kvs_terminate())
  {
    write(STDERR_FILENO, "Failed to terminate KVS\n", strlen("Failed to terminate KVS\n"));
    return 1;
  }
}