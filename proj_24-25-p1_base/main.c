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

int main(int argc, char *argv[])
{
  if(argc!=2){
    fprintf(stderr, "wrong argument count\n"); //perguntar ao stor
  }
  if (kvs_init())
  {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  // aqui na main eu abro a diretoria e vejo se ela existe (!= NULL)
  char *dirPath = argv[1];
  DIR *dir = opendir(dirPath);
  if (dir == NULL)
  {
    perror("Couldn't open directory");
    exit(EXIT_FAILURE);
  }

  struct dirent *fileDir;
  while ((fileDir = readdir(dir)) != NULL)
  { // leio a diretora e dentro deste while tenho de fazer open_file para os ficheiros do tipo ".job"
    if (fileDir->d_type == DT_REG)
    { // verifica se é um regular file
      const char *fileName = fileDir->d_name;
      const char *fileExtension = strrchr(fileName, '.'); // fileextension será depois do ponto
      if (fileExtension != NULL && strcmp(fileExtension, ".job") == 0)
      { // só leio ficheiros do tipo ".job"
        char fullPath[PATH_MAX];
        int fd = open(fullPath, O_RDONLY);
        if(fd==-1){
          perror("Couldn't open file");
          continue;
        }
        while (1)
        {
          char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
          char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
          unsigned int delay;
          size_t num_pairs;

          switch (get_next(fd))
          {
          case CMD_WRITE:
            num_pairs = parse_write(STDIN_FILENO, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
            if (num_pairs == 0)
            {
              fprintf(stderr, "Invalid command. See HELP for usage\n");
              continue;
            }

            if (kvs_write(num_pairs, keys, values))
            {
              fprintf(stderr, "Failed to write pair\n");
            }

            break;

          case CMD_READ:
            num_pairs = parse_read_delete(STDIN_FILENO, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

            if (num_pairs == 0)
            {
              fprintf(stderr, "Invalid command. See HELP for usage\n");
              continue;
            }

            if (kvs_read(num_pairs, keys))
            {
              fprintf(stderr, "Failed to read pair\n");
            }
            break;

          case CMD_DELETE:
            num_pairs = parse_read_delete(STDIN_FILENO, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

            if (num_pairs == 0)
            {
              fprintf(stderr, "Invalid command. See HELP for usage\n");
              continue;
            }

            if (kvs_delete(num_pairs, keys))
            {
              fprintf(stderr, "Failed to delete pair\n");
            }
            break;

          case CMD_SHOW:

            kvs_show();
            break;

          case CMD_WAIT:
            if (parse_wait(STDIN_FILENO, &delay, NULL) == -1)
            {
              fprintf(stderr, "Invalid command. See HELP for usage\n");
              continue;
            }

            if (delay > 0)
            {
              printf("Waiting...\n");
              kvs_wait(delay);
            }
            break;

          case CMD_BACKUP:

            if (kvs_backup())
            {
              fprintf(stderr, "Failed to perform backup.\n");
            }
            break;

          case CMD_INVALID:
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            break;

          case CMD_HELP:
            printf(
                "Available commands:\n"
                "  WRITE [(key,value),(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

            break;

          case CMD_EMPTY:
            break;

          case EOC:
            close(fd);
            continue;
          }
        }
      }
    }
  }
  closedir(dirPath);
}