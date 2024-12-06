
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



/* Para o exercicio 3, temos de criar tarefas para o programa conseguir tratar de vários ficheiros
 .job em simultâneo. Para isso, vamos usar threads, com mutexes de leitura e escrita para proteger
 a manipulação da tabela. Para não ultrapassar o número máximo de threads, usaremos semáforos.
 */


// @brief Estrutura que guarda os file descriptors necessários para cada tarefa.
typedef struct fds{
  int input; // File descriptor of the file that we are reading from
  int output; // File descriptor of the output file
} in_out_fds;


// Esta é a função que vai fazer as operações na tabela, que vai ser chamada em threads.
void *tableOperations(void *fd_info){
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

            if (kvs_backup(fd->output))
            {
              write(fd->output, "Failed to perform backup.\n", strlen("Failed to perform backup.\n"));
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
        free(fd);
        return NULL;
}

int main(int argc, char *argv[])
{
  if(argc!=3){
    write(STDERR_FILENO, "Wrong arguments.\n", strlen("Wrong arguments.\n")); //perguntar ao stor
    write(STDERR_FILENO, "Usage: ./kvs [FOLDER_NAME] [MAX_THREADS(>0)]\n", strlen("Usage: ./kvs [FOLDER_NAME] [MAX_THREADS]\n")); //perguntar ao stor  
    return (EXIT_FAILURE);
  }

  //Definimos o número máximo de threads que podemos ter, a partir do input do utilizador
  int MAX_THREADS = atoi(argv[2]);

  if (!MAX_THREADS){
    write(STDERR_FILENO, "Wrong arguments.\n", strlen("Wrong arguments.\n")); //perguntar ao stor
    write(STDERR_FILENO, "Usage: ./kvs [FOLDER_NAME] [MAX_THREADS(>0)]\n", strlen("Usage: ./kvs [FOLDER_NAME] [MAX_THREADS]\n")); //perguntar ao stor
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
    return(EXIT_FAILURE);
  }
  // A inicialização da tabela estava a ser feita dentro do while, mas tem de ser fora porque
  // a tabela é única, todos os ficheiros escrevem para o mesmo sítio.
  if (kvs_init())
  {
    //fprintf(stderr, "Failed to initialize KVS\n");
    write(STDERR_FILENO, "Failed to initialize KVS\n", strlen("Failed to initialize KVS\n"));
    return 1;
  }

  struct dirent *fileDir;
  pthread_t threads[MAX_THREADS]; //Array para guardar as threads, para, no final, podermos fazer join.
  int i = 0; // Variável para iterar pelas threads
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
        int outputFd;
        if(fd==-1){
          perror("Couldn't open file");
          continue;
        }
        if ((outputFd = outputFile(fileName)) == -1)
          continue;
        // Temos de criar esta estrutura para guardar os fd's para conseguirmos enviar à função da thread.
        in_out_fds *fds = malloc(sizeof(in_out_fds));
        fds->input = fd;
        fds->output = outputFd;
        if (pthread_create(&(threads[i]), NULL, &tableOperations, fds) != 0){
          write(STDERR_FILENO, "Error in creating thread\n", strlen("Error in creating thread\n"));
          free (fds);
        } else
            i++;
      }
    }
  }
  for (i = 0; i < MAX_THREADS; i++){
    pthread_join(threads[i], NULL);
  }
  if (kvs_terminate())
  {
    write(STDERR_FILENO, "Failed to terminate KVS\n", strlen("Failed to terminate KVS\n"));
    return 1;
  }
  closedir(dir);
}
