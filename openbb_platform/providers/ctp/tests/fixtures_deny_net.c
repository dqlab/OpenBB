#define _GNU_SOURCE
#include <sys/socket.h>
#include <dlfcn.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
static void hit(const char *s) { const char *p=getenv("CTP_NET_LOG"); if(p){int f=open(p,O_WRONLY|O_CREAT|O_APPEND,0600); if(f>=0){write(f,s,strlen(s));close(f);}} }
int socket(int domain,int type,int protocol) { if(domain==AF_INET||domain==AF_INET6){hit("socket\n");errno=EACCES;return -1;} int (*f)(int,int,int)=dlsym(RTLD_NEXT,"socket"); return f(domain,type,protocol); }
int connect(int fd,const struct sockaddr *a,socklen_t n) { if(a && (a->sa_family==AF_INET||a->sa_family==AF_INET6)){hit("connect\n");errno=EACCES;return -1;} int (*f)(int,const struct sockaddr*,socklen_t)=dlsym(RTLD_NEXT,"connect");return f(fd,a,n); }
ssize_t sendto(int fd,const void *b,size_t n,int flags,const struct sockaddr *a,socklen_t al) { if(a && (a->sa_family==AF_INET||a->sa_family==AF_INET6)){hit("sendto\n");errno=EACCES;return -1;} ssize_t (*f)(int,const void*,size_t,int,const struct sockaddr*,socklen_t)=dlsym(RTLD_NEXT,"sendto");return f(fd,b,n,flags,a,al); }
