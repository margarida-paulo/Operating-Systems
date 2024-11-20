#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "fileOperations.h"

int main(int argc, char **argv) {

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  (void)argc;

  int outFd = outputFile(argv[1]);
  if (outFd == -1){
    return 1;
  }

  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    printf("> ");
    fflush(stdout);

    switch (get_next(STDIN_FILENO)) {
      case CMD_WRITE:
        num_pairs = parse_write(STDIN_FILENO, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write(outFd, "Invalid command. See HELP for usage\n", 37);
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write(outFd, "Failed to write pair\n", 22);
        }

        break;

      case CMD_READ:
        num_pairs = parse_read_delete(STDIN_FILENO, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(STDIN_FILENO, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:

        kvs_show();
        break;

      case CMD_WAIT:
        if (parse_wait(STDIN_FILENO, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          printf("Waiting...\n");
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:

        if (kvs_backup()) {
          fprintf(stderr, "Failed to perform backup.\n");
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        write(outFd, "Available commands:\n"
            "  WRITE [(key,value),(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n", 148);
        break;
        
      case CMD_EMPTY:
        break;

      case EOC:
        close(outFd);
        kvs_terminate();
        return 0;
    }
  }
}
