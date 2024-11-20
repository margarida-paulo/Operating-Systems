#include "fileOperations.h"

int outputFile(char *diretoria){
	char ficheiro[strlen(diretoria) + 5];
	strcpy(ficheiro, diretoria);
	strcat(ficheiro,"/out");
	int fd = open(ficheiro, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
	if (fd == -1){
		write(STDERR_FILENO, "Couldn't open output file", 26);
	}
	return fd;
}