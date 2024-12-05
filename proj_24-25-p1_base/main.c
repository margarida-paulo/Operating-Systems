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

int main(int argc, char *argv[])
{
  if(argc!=2){
    write(STDERR_FILENO, "wrong argument count\n", strlen("wrong argument count\n")); //perguntar ao stor
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

  struct dirent *fileDir;
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
      int fileNameLength = strlen(fileName);
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
        enum Command fileOver = 0;
        if (kvs_init())
        {
          //fprintf(stderr, "Failed to initialize KVS\n");
          write(STDERR_FILENO, "Failed to initialize KVS\n", strlen("Failed to initialize KVS\n"));
          return 1;
        }
        while (fileOver != EOC)
        {
          char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
          char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
          unsigned int delay;
          size_t num_pairs;

          switch (fileOver = get_next(fd))
          {
          case CMD_WRITE:
            num_pairs = parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
            if (num_pairs == 0)
            {
              write(outputFd, "Invalid command. See HELP for usage\n", strlen("Invalid command. See HELP for usage\n"));
              continue;
            }

            if (kvs_write(num_pairs, keys, values, outputFd))
            {
              write(outputFd, "Failed to write pair\n", strlen("Failed to write pair\n"));
            }

            break;

          case CMD_READ:
            num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

            if (num_pairs == 0)
            {
              write(outputFd, "Invalid command. See HELP for usage\n", strlen("Invalid command. See HELP for usage\n"));
              continue;
            }

            if (kvs_read(num_pairs, keys, outputFd))
            {
              write(outputFd, "Failed to read pair\n", strlen("Failed to read pair\n"));
            }
            break;

          case CMD_DELETE:
            num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

            if (num_pairs == 0)
            {
              write(outputFd, "Invalid command. See HELP for usage\n", strlen("Invalid command. See HELP for usage\n"));
              continue;
            }

            if (kvs_delete(num_pairs, keys, outputFd))
            {
              write(outputFd, "Failed to delete pair\n", strlen("Failed to delete pair\n"));
            }
            break;

          case CMD_SHOW:

            kvs_show(outputFd);
            break;

          case CMD_WAIT:
            if (parse_wait(fd, &delay, NULL) == -1)
            {
              write(outputFd, "Invalid command. See HELP for usage\n", strlen("Invalid command. See HELP for usage\n"));
              continue;
            }

            if (delay > 0)
            {
              write(outputFd, "Waiting...\n", strlen("Waiting...\n"));
              kvs_wait(delay);
            }
            break;

          case CMD_BACKUP:

            if (kvs_backup(outputFd))
            {
              write(outputFd, "Failed to perform backup.\n", strlen("Failed to perform backup.\n"));
            }
            break;

          case CMD_INVALID:
            write(outputFd, "Invalid command. See HELP for usage\n", strlen("Invalid command. See HELP for usage\n"));
            break;

          case CMD_HELP:
            write(outputFd,
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
            close(fd);
            close(outputFd);
            if (kvs_terminate())
            {
              write(STDERR_FILENO, "Failed to terminate KVS\n", strlen("Failed to terminate KVS\n"));
              return 1;
            }
            break;
          }
        }
      }
    }
  }
  closedir(dir);
}