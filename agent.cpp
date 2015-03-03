#include <iostream>
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/inotify.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <arpa/inet.h>


#include "myInotify.h"
#include "sockConstants.h"


using namespace std;



#define IP_LEN 16
#define HOST_LEN 128
#define ETH_NAME "eth0"



#define LOG_running

#ifdef  LOG_running

FILE *fp_log = NULL;
FILE *message_log = NULL;
char Log_Message[256] = "";

char * getCurrentTime()
{
    time_t rawtime;
    struct tm * timeinfo;

    time ( &rawtime );
    timeinfo = localtime ( &rawtime );

    return asctime(timeinfo);
}


void writeLogMessage_INFO(FILE *fp_log, char *log)
{
    time_t rawtime;
    struct tm * timeinfo;

    time ( &rawtime );
    timeinfo = localtime ( &rawtime );

    fprintf(fp_log, "\n%s[INFO]\n%s\n", asctime(timeinfo), log);

}

void writeLogMessage_WARN(FILE *fp_log, char *warnLog)
{
    time_t rawtime;
    struct tm * timeinfo;

    time ( &rawtime );
    timeinfo = localtime ( &rawtime );

    fprintf(fp_log, "\n%s[WARN]\n%s\n", asctime(timeinfo), warnLog);

}


#endif




#define NETWORK

#ifdef NETWORK

int fd_socket;
char message[LEN_message] = "";
unsigned int send_len = 0;

int sendMessageToServer(int FLAG, char *Message, int fd_socket)
{
    unsigned int send_len = 0;

    char tmp_message[10 + LEN_message] = "";
    sprintf(tmp_message,"%d;%s", FLAG, Message);
    send_len = send(fd_socket, tmp_message, sizeof(tmp_message), 0);
    if(send_len < 0)
    {
        perror("send ");
    }

#ifdef LOG_running

    sprintf(Log_Message, "[sendMessageToServer]\nmessage : \n%s", tmp_message);
    writeLogMessage_INFO(fp_log, Log_Message);
    writeLogMessage_INFO(message_log, Log_Message);

#endif

    return send_len;

}

bool dealWithMessage(char * message, inotify_appStruct &app_ino, inotify_logStruct &log_ino,
                     inotify_logStruct_modify &modi_ino)
{

    void removeFromAppTable(inotify_appStruct &app_ino, char * appName,
                            inotify_logStruct &log_ino, inotify_logStruct_modify &modi_ino);

    if(NULL == message || 0 == strcmp("", message))
    {
        cout<<"[WARN] Wrong message.\n"<<endl;
        return false;
    }
    char copy_message[LEN_message];
    strcpy(copy_message, message);

    /// get flag
    char *p = strtok(copy_message, ";");
    int flag_num = -1;

    if(NULL == p)
    {
        cout<<"[WARN] wrong message.\n"<<endl;
        return false;
    }

    flag_num = atoi(p);

    switch(flag_num)
    {

    case app_OVER:
    {
        p = strtok(NULL, ";");
        removeFromAppTable(app_ino, p, log_ino, modi_ino);
        break;

    }

    default:
    {
        cout<<"error Flag in message.\n"<<endl;
        break;
    }
    }

#ifdef LOG_running

    sprintf(Log_Message, "[dealWithMessage]\nmessage : \n%s", message);
    writeLogMessage_INFO(fp_log, Log_Message);
    writeLogMessage_INFO(message_log, Log_Message);

#endif


    return true;

}

#endif



/// Global variables
char HOST_IpAddr[IP_LEN] = "";
char HOST_Name[HOST_LEN] = "";


int modify_num = 0;
/// Global variables

void getCurrentTime_Print()
{
    time_t rawtime;
    struct tm * timeinfo;

    time ( &rawtime );
    timeinfo = localtime ( &rawtime );
    printf ( "The current time is: %s", asctime(timeinfo) );
}



bool ipFormatCheck(char * ip)
{
    if(NULL == ip)
        return false;
    if('.' == ip[0])
        return false;
    char * ptr = ip;
    char * ptr_dot = NULL;
    int dotNum = 0;
    char tmp[32] = "";
    unsigned int i;
    for(; NULL != (ptr_dot = index(ptr, '.')); ptr = ptr_dot + 1)
    {
        ++dotNum;
        strncpy(tmp, ptr, ptr_dot - ptr);
        tmp[ptr_dot - ptr] = '\0';
        for(i = 0; i < strlen(tmp); ++i)
        {
            if(*(ptr + i) < '0' || *(ptr + i) > '9')
            {
                return false;
            }

        }
        int decimal = atoi(tmp);
        if(decimal > 255 || decimal < 0)
        {
            return false;
        }

    }
    if(3 != dotNum)
    {
        return false;
    }

    strcpy(tmp, ptr);
    for(i = 0; i < strlen(tmp); ++i)
    {
        if(*(ptr + i) < '0' || *(ptr + i) > '9')
        {
            return false;
        }

    }
    return true;
}

bool MY_getHostName(char * const hostname, int len)
{
    gethostname(hostname, len);
    //fprintf(stdout, "host : %s\n", hostName);
    return true;
}

bool MY_getHostIpAddress(char * const ipaddr, int len)
{
    if(len < 16)
    {
        perror("string not long enough ");
        return false;
    }
    int sock;
    struct sockaddr_in sin;
    struct ifreq ifr;

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock == -1)
    {
        perror("socket");
        return false;
    }
    strncpy(ifr.ifr_name, ETH_NAME, IFNAMSIZ);
    ifr.ifr_name[IFNAMSIZ - 1] = 0;

    if (ioctl(sock, SIOCGIFADDR, &ifr) < 0)
    {
        perror("ioctl");
        return false;
    }

    memcpy(&sin, &ifr.ifr_addr, sizeof(sin));
    strcpy(ipaddr, inet_ntoa(sin.sin_addr));
    return true;
}


bool Init_inotifyTable_root(inotify_rootStruct &ino, const char * rootPath)
{
    if(NULL == rootPath)
    {
        cout<<"NULL Pointer in Init_inotifyTable_root\n"<<endl;
        return false;
    }
    strcpy(ino.filePath, rootPath);

    ino.fd = inotify_init1(IN_NONBLOCK);
    if (-1 == ino.fd)
    {
        cout<<"Error fd in Init_inotifyTable_root\n"<<endl;
        return false;
    }

    /// Add watcher to rootfd to watch app CREATE events.
    ino.wd = inotify_add_watch(ino.fd, ino.filePath, IN_CREATE);
    if(-1 ==ino.wd)
    {
        cout<<"Error add_watch in Init_inotifyTable_root\n"<<endl;
        return false;
    }

#ifdef LOG_running

    sprintf(Log_Message, "[Init_inotifyTable_root]\nrootPath : %s", rootPath);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return true;
}

bool Init_inotifyTable_app(inotify_appStruct &ino, int num)
{
    if(num <= 0)
    {
        cout<<"Wrong num in Init_inotifyTable_app\n"<<endl;
        return false;
    }

    ino.totalNum = num;
    ino.cnt = 0;
    ino.fd = inotify_init1(IN_NONBLOCK);
    if(-1 == ino.fd)
    {
        cout<<"Error fd in Init_inotifyTable_app\n"<<endl;
        return false;
    }
    ino.wd = new int[num];
    if(NULL == ino.wd)
    {
        cout<<"NULL Pointer in Init_inotifyTable_app\n"<<endl;
        return false;
    }
    ino.names = new iString[num];
    if(NULL == ino.names)
    {
        cout<<"NULL Pointer in Init_inotifyTable_app\n"<<endl;
        return false;
    }
    int i;
    for(i = 0; i < num ; ++i)
    {
        //ino->names[i][0] = '/';
        ino.names[i][0] = '\0';
    }

#ifdef LOG_running

    sprintf(Log_Message, "[Init_inotifyTable_app]\ntotalNum : %d\ncnt : %d",
            ino.totalNum, ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return true;
}

/*
bool Init_inotifyTable_con(inotify_conStruct * const ino, int num)
{
    if(NULL == ino && num <= 0)
    return false;
    ino->totalNum = num;
    ino->cnt = 0;
    //ino->fd = inotify_init1(IN_NONBLOCK);
    //if(-1 == ino->fd)
    //return false;
    //ino->wd = new int[num];
    //if(NULL == ino->wd)
    //return false;
    ino->names = new iString[num];
    if(NULL == ino->names)
        return false;
    int i;
    for(i = 0; i < num ; ++i)
    {
        ino->names[i][0] = '/';
        ino->names[i][1] = '\0';
    }
    ino->parentNum = new int[num];
    if(NULL == ino->parentNum)
        return false;
    return true;
}
*/

bool Init_inotifyTable_log(inotify_logStruct &ino, int num)
{
    if(num <= 0)
    {
        cout<<"Wrong num in Init_inotifyTable_log\n"<<endl;
        return false;
    }
    ino.totalNum = num;
    ino.cnt = 0;
    ino.fd = inotify_init1(IN_NONBLOCK);
    if(-1 == ino.fd)
    {
        cout<<"Error fd in Init_inotifyTable_log\n"<<endl;
        return false;
    }

    ino.wd = new int[num];
    if(NULL == ino.wd)
    {
        cout<<"NULL Pointer in Init_inotifyTable_log\n"<<endl;
        return false;
    }
    ino.parentNum = new int[num];
    if(NULL == ino.parentNum)
    {
        cout<<"NULL Pointer in Init_inotifyTable_log\n"<<endl;
        return false;
    }
    ino.names = new iString[num];
    if(NULL == ino.names)
    {
        cout<<"NULL Pointer in Init_inotifyTable_log\n"<<endl;
        return false;
    }
    ino.offset = new int[num];
    if(NULL == ino.offset)
    {
        cout<<"NULL Pointer in Init_inotifyTable_log\n"<<endl;
        return false;
    }
    int i;
    for(i = 0; i < num ; ++i)
    {
        //ino->names[i][0] = '/';
        ino.names[i][0] = '\0';
        ino.offset[i] = 0;
    }

#ifdef LOG_running

    sprintf(Log_Message, "[Init_inotifyTable_log]\ntotalNum : %d\ncnt : %d",
            ino.totalNum, ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return true;
}

bool Init_inotifyTable_log_modify(inotify_logStruct_modify &ino, int num)
{
    if(num <= 0)
    {
        cout<<"Wrong num in Init_inotifyTable_log_modify\n"<<endl;
        return false;
    }
    ino.totalNum = num;
    ino.cnt = 0;
    ino.fd = inotify_init1(IN_NONBLOCK);
    if(-1 == ino.fd)
    {
        cout<<"Error fd in Init_inotifyTable_log_modify\n"<<endl;
        return false;
    }

    ino.wd = new int[num];
    if(NULL == ino.wd)
    {
        cout<<"NULL Pointer in Init_inotifyTable_log_modify\n"<<endl;
        return false;
    }
    ino.parentNum = new int[num];
    if(NULL == ino.parentNum)
    {
        cout<<"NULL Pointer in Init_inotifyTable_log_modify\n"<<endl;
        return false;
    }
    ino.offset = new int[num];
    if(NULL == ino.offset)
    {
        cout<<"NULL Pointer in Init_inotifyTable_log_modify\n"<<endl;
        return false;
    }
    ino.fpArray = new FILEPointer[num];
    if(NULL == ino.fpArray )
    {
        cout<<"NULL Pointer in Init_inotifyTable_log_modify\n"<<endl;
        return false;
    }
    ino.names = new iString[num];
    if(NULL == ino.names)
    {
        cout<<"NULL Pointer in Init_inotifyTable_log_modify\n"<<endl;
        return false;
    }
    int i;
    for(i = 0; i < num ; ++i)
    {
        ino.offset[i] = 0;
        ino.fpArray[i] = NULL;
    }

#ifdef LOG_running

    sprintf(Log_Message, "[Init_inotifyTable_log_modify]\ntotalNum : %d\ncnt : %d",
            ino.totalNum, ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return true;

}




bool release_inotify_appStruct(inotify_appStruct &ino)
{

    delete[] ino.wd;
    ino.wd = NULL;
    delete[] ino.names;
    ino.names = NULL;
    ino.totalNum = ino.cnt = 0;
    close(ino.fd);

#ifdef LOG_running

    sprintf(Log_Message, "[release_inotify_appStruct]\ntotalNum : %d\ncnt : %d",
            ino.totalNum, ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return true;
}

/*
bool release_inotify_conStruct(inotify_conStruct * const ino)
{
    if(NULL == ino)
        return false;
    free(ino->parentNum);
    free(ino->names);
    ino->totalNum = ino->cnt = 0;
    //close(ino->fd);
    return true;
}
*/

bool release_inotify_logStruct(inotify_logStruct &ino)
{

    delete[] ino.offset;
    ino.offset = NULL;
    delete[] ino.parentNum;
    ino.parentNum = NULL;
    delete[] ino.names;
    ino.names = NULL;
    delete[] ino.wd;
    ino.wd = NULL;
    ino.totalNum = ino.cnt = 0;
    close(ino.fd);

#ifdef LOG_running

    sprintf(Log_Message, "[release_inotify_logStruct]\ntotalNum : %d\ncnt : %d",
            ino.totalNum, ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return true;
}

bool release_inotify_logStruct_modify(inotify_logStruct_modify &ino)
{

    delete[] ino.wd;
    ino.wd = NULL;
    delete[] ino.parentNum;
    ino.parentNum = NULL;
    int i;
    for(i = 0; i < ino.cnt; ++i)
    {
        fclose(ino.fpArray[i]);
    }
    delete[] ino.fpArray;
    ino.fpArray = NULL;
    ino.totalNum = ino.cnt = 0;
    close(ino.fd);

#ifdef LOG_running

    sprintf(Log_Message, "[release_inotify_logStruct_modify]\ntotalNum : %d\ncnt : %d",
            ino.totalNum, ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return true;
}



/// Realloc

bool realloc_inotify_appStruct(inotify_appStruct &ino, int addNum)
{
    //printf("go into realloc_inotify_appStruct.\n");
    int New_totalNum = ino.totalNum + addNum;
    int * temp_wd = new int[New_totalNum];
    if(NULL == temp_wd)
    {
        cout<<"NULL Pointer in realloc_inotify_appStruct\n"<<endl;
        return false;
    }
    iString * temp_names = new iString[New_totalNum];
    if(NULL == temp_names)
    {
        cout<<"NULL Pointer in realloc_inotify_appStruct\n"<<endl;
        return false;
    }
    int i;
    for(i = 0; i < ino.cnt; ++i)
    {
        temp_wd[i] = ino.wd[i];
        strcpy(temp_names[i], ino.names[i]);
    }
    for(i = ino.cnt; i < New_totalNum; ++i)
    {
        //temp_names[i][0] = '/';
        temp_names[i][0] = '\0';
    }
    free(ino.names);
    free(ino.wd);
    ino.wd = temp_wd;
    ino.names = temp_names;
    ino.totalNum = New_totalNum;
    //printf("%d\t%d\n",ino->totalNum, ino->cnt);

#ifdef LOG_running

    sprintf(Log_Message, "[realloc_inotify_appStruct]\ntotalNum : %d\ncnt : %d",
            ino.totalNum, ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return true;
}

/*
bool realloc_inotify_conStruct(inotify_conStruct * const ino)
{
    if(NULL == ino)
        return false;
    inotify_conStruct * temp;
    int Ex_totalNum = ino->totalNum;
    release_inotify_conStruct(ino);
    if(!Init_inotifyTable_con(temp, 2 * Ex_totalNum))
        return false;
    *ino = *temp;

    return true;
}
*/

bool realloc_inotify_logStruct(inotify_logStruct &ino, int addNum)
{
    //printf("go into realloc_inotify_logStruct.\n");

    int New_totalNum = ino.totalNum + addNum;
    int * temp_wd = new int[New_totalNum];
    if(NULL == temp_wd)
    {
        cout<<"NULL Pointer in realloc_inotify_logStruct\n"<<endl;
        return false;
    }
    iString * temp_names = new iString[New_totalNum];
    if(NULL == temp_names)
    {
        cout<<"NULL Pointer in realloc_inotify_logStruct\n"<<endl;
        return false;
    }
    int * temp_parentNum = new int[New_totalNum];
    if(NULL == temp_parentNum)
    {
        cout<<"NULL Pointer in realloc_inotify_logStruct\n"<<endl;
        return false;
    }
    int * temp_offset = new int[New_totalNum];
    if(NULL == temp_offset)
    {
        cout<<"NULL Pointer in realloc_inotify_logStruct\n"<<endl;
        return false;
    }
    int i;
    for(i = 0; i < ino.cnt; ++i)
    {
        temp_wd[i] = ino.wd[i];
        strcpy(temp_names[i], ino.names[i]);
        temp_parentNum[i] = ino.parentNum[i];
        temp_offset[i] = ino.offset[i];
    }
    for(i = ino.cnt; i < New_totalNum; ++i)
    {
        //temp_names[i][0] = '/';
        temp_names[i][0] = '\0';
        temp_offset[i] = 0;
    }
    free(ino.names);
    free(ino.wd);
    free(ino.parentNum);
    free(ino.offset);
    ino.wd = temp_wd;
    ino.names = temp_names;
    ino.parentNum = temp_parentNum;
    ino.totalNum = New_totalNum;
    ino.offset = temp_offset;

#ifdef LOG_running

    sprintf(Log_Message, "[realloc_inotify_logStruct]\ntotalNum : %d\ncnt : %d",
            ino.totalNum, ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return true;
}

bool realloc_inotify_logStruct_modify(inotify_logStruct_modify &ino, int addNum)
{
    //printf("go into realloc_inotify_logStruct.\n");
    int New_totalNum = ino.totalNum + addNum;
    int * temp_wd = new int[New_totalNum];
    if(NULL == temp_wd)
    {
        cout<<"NULL Pointer in realloc_inotify_logStruct_modify\n"<<endl;
        return false;
    }
    FILEPointer * temp_fpArray = new FILEPointer[New_totalNum];
    if(NULL == temp_fpArray)
    {
        cout<<"NULL Pointer in realloc_inotify_logStruct_modify\n"<<endl;
        return false;
    }
    int * temp_parentNum = new int[New_totalNum];
    if(NULL == temp_parentNum)
    {
        cout<<"NULL Pointer in realloc_inotify_logStruct_modify\n"<<endl;
        return false;
    }
    int * temp_offset = new int[New_totalNum];
    if(NULL == temp_offset)
    {
        cout<<"NULL Pointer in realloc_inotify_logStruct_modify\n"<<endl;
        return false;
    }
    iString * temp_names = new iString[New_totalNum];
    if(NULL == temp_names)
    {
        cout<<"NULL Pointer in realloc_inotify_logStruct_modify\n"<<endl;
        return false;
    }
    int i;
    for(i = 0; i < ino.cnt; ++i)
    {
        temp_wd[i] = ino.wd[i];
        temp_fpArray[i] = ino.fpArray[i];
        temp_parentNum[i] = ino.parentNum[i];
        temp_offset[i] = ino.offset[i];
        strcpy(temp_names[i], ino.names[i]);
    }
    for(i = ino.cnt; i < New_totalNum; ++i)
    {
        temp_names[i][0] = '\0';
        temp_fpArray[i] = NULL;
        temp_offset[i] = 0;
    }
    free(ino.fpArray);
    free(ino.wd);
    free(ino.parentNum);
    free(ino.offset);
    free(ino.names);
    ino.wd = temp_wd;
    ino.fpArray = temp_fpArray;
    ino.parentNum = temp_parentNum;
    ino.totalNum = New_totalNum;
    ino.offset = temp_offset;
    ino.names = temp_names;

#ifdef LOG_running

    sprintf(Log_Message, "[realloc_inotify_logStruct_modify]\ntotalNum : %d\ncnt : %d",
            ino.totalNum, ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return true;
}



/* Read all available inotify events from the file descriptor 'fd'.
   wd is the table of watch descriptors for the directories in argv.
   argc is the length of wd and argv.
   argv is the list of watched directories.
   Entry 0 of wd and argv is unused. */

/// Watch rootPath and receive appliaction CREATE events.
void handle_app_events(inotify_rootStruct &root_ino, inotify_appStruct &app_ino)
{

    char buffer[256 * (EVENT_SIZE)];
    int len;
    char * ptr;
    const struct inotify_event * event;
    while(1)
    {
        len = read(root_ino.fd, buffer, sizeof(buffer));
        if (len == -1 && errno != EAGAIN)
        {
            perror("read() in handle_app_events()");
            exit(EXIT_FAILURE);
        }

        /* If the nonblocking read() found no events to read, then
           it returns -1 with errno set to EAGAIN. In that case,
           we exit the loop. */

        if (len <= 0)
            break;
        /// Loop over all events in the buffer
        for (ptr = buffer; ptr < buffer + len;
                ptr += sizeof(struct inotify_event) + event->len)
        {
            event = (const struct inotify_event *) ptr;
            if(event->mask & IN_CREATE)
            {
                if (event->mask & IN_ISDIR)
                {
                    /// Deal with application(DIR) CREATE events.
                    if(app_ino.cnt >= app_ino.totalNum)
                    {
                        if(!realloc_inotify_appStruct(app_ino, ADD_appStruct_NUM))
                        {
                            perror("realloc_inotify_appStruct.");
                            exit(EXIT_FAILURE);
                        }
                    }
                    int seq = app_ino.cnt;
                    app_ino.names[seq][0] = '/';
                    app_ino.names[seq][1] = '\0';
                    strcat(app_ino.names[seq], event->name);
                    char temp_filename[256] = "";
                    temp_filename[0] = '\0';
                    strcpy(temp_filename, root_ino.filePath);
                    strcat(temp_filename, app_ino.names[seq]);
                    int wd = inotify_add_watch(app_ino.fd, temp_filename, IN_CREATE);
                    if(-1 == wd)
                    {
                        cout<<temp_filename<<endl;
                        perror("handle_app_events, add_watch ");
                        exit(EXIT_FAILURE);
                    }
                    app_ino.wd[seq] = wd;
                    ++app_ino.cnt;
                    printf("**APP EVENT**\n[directory] %s was created.\n", event->name);
                    printf("[handle_app_events]\naddName : %s\npath : %s\n\n",
                           app_ino.names[seq], temp_filename);
#ifdef LOG_running

                    sprintf(Log_Message, "[handle_app_events]\ntotalNum : %d\t cnt : %d\naddName : %s\nadd_watch : %d\npath : %s",
                            app_ino.totalNum, app_ino.cnt, app_ino.names[seq],
                            app_ino.wd[seq], temp_filename);
                    writeLogMessage_INFO(fp_log, Log_Message);

#endif


                }
            }
        }
    }

}


/// Watch appPath in appTable and receive container CREATE events.
void handle_con_events(inotify_rootStruct &root_ino, inotify_appStruct &app_ino,
                       inotify_logStruct &log_ino)
{

    char buffer[256 * (EVENT_SIZE)];
    int len;
    char * ptr;
    const struct inotify_event * event;
    while(1)
    {
        len = read(app_ino.fd, buffer, sizeof(buffer));
        if (len == -1 && errno != EAGAIN)
        {
            perror("read() in handle_con_events()");
            exit(EXIT_FAILURE);
        }

        /// If the nonblocking read() found no events to read, then
        ///   it returns -1 with errno set to EAGAIN. In that case,
        ///   we exit the loop.

        if (len <= 0)
            break;
        /// Loop over all events in the buffer
        for (ptr = buffer; ptr < buffer + len;
                ptr += sizeof(struct inotify_event) + event->len)
        {
            event = (const struct inotify_event *) ptr;
            if(event->mask & IN_CREATE)
            {
                int i;
                if (event->mask & IN_ISDIR)
                {
                    /// Deal with container(DIR) CREATE events.
                    //printf("[directory] %s was created.\n", event->name);
                    //printf("**Container EVENT**\n[directory] %s was created.\n", event->name);
                    if(log_ino.cnt >= log_ino.totalNum)
                    {
                        if(!realloc_inotify_logStruct(log_ino, ADD_logStruct_NUM))
                        {
                            perror("realloc_inotify_logStruct.");
                            exit(EXIT_FAILURE);
                        }
                    }
                    int parent_num = -1;
                    for(i = 0 ; i < app_ino.cnt; ++i)
                    {
                        if(event->wd == app_ino.wd[i])
                        {
                            parent_num = i;
                            break;
                        }
                    }
                    if(-1 == parent_num)
                    {
                        perror("error in search wd on handle_con_events.");
                        exit(EXIT_FAILURE);
                    }
                    int seq = log_ino.cnt;
                    log_ino.parentNum[seq] = parent_num;
                    log_ino.names[seq][0] = '/';
                    log_ino.names[seq][1] = '\0';
                    strcat(log_ino.names[seq], event->name);
                    char temp_filename[256] = "";
                    //temp_filename[0] = '\0';
                    strcpy(temp_filename, root_ino.filePath);
                    strcat(temp_filename, app_ino.names[log_ino.parentNum[seq]]);
                    strcat(temp_filename, log_ino.names[seq]);
                    //strcat(temp_filename, "/syslog");
                    ///watch syslog create event in container's dir.
                    int wd = inotify_add_watch(log_ino.fd, temp_filename, IN_CREATE);
                    if(-1 == wd)
                    {
                        cout<<temp_filename<<endl;
                        perror("handle_con_events, add_watch");
                        exit(EXIT_FAILURE);
                    }
                    log_ino.wd[seq] = wd;
                    ++log_ino.cnt;
                    printf("**Container EVENT**\n[directory] %s was created.\n", event->name);
                    printf("[handle_con_events]\naddName : %s\npath : %s\n\n",
                           log_ino.names[seq], temp_filename);

#ifdef LOG_running

                    sprintf(Log_Message, "[handle_con_events]\ntotalNum : %d\t cnt : %d\naddName : %s\nadd_watch : %d\npath : %s",
                            log_ino.totalNum, log_ino.cnt, log_ino.names[seq],
                            log_ino.wd[seq],temp_filename);
                    writeLogMessage_INFO(fp_log, Log_Message);

#endif

                }
            }
        }
    }
}


/// Watch conPath in conTable and receive log MODIFY events.
/// network communication
void handle_log_events(inotify_rootStruct &root_ino, inotify_appStruct &app_ino,
                       inotify_logStruct &log_ino, inotify_logStruct_modify &modi_log_ino)
{

    void removeFromLogTable_seq(inotify_logStruct &log_ino, int seq);
    void addToModi_logTable(inotify_logStruct_modify &modi_ino,
                            char *fileName, int fileOffset,
                            int parentNum, char *names);

    char buffer[256 * (EVENT_SIZE)];
    int len;
    char * ptr;
    const struct inotify_event * event;
    while(1)
    {
        len = read(log_ino.fd, buffer, sizeof(buffer));
        if (len == -1 && errno != EAGAIN)
        {
            perror("read() in handle_log_events()");
            exit(EXIT_FAILURE);
        }

        /* If the nonblocking read() found no events to read, then
           it returns -1 with errno set to EAGAIN. In that case,
           we exit the loop. */

        if (len <= 0)
            break;
        /// Loop over all events in the buffer
        for (ptr = buffer; ptr < buffer + len;
                ptr += sizeof(struct inotify_event) + event->len)
        {
            event = (const struct inotify_event *) ptr;
            int seq = -1, i;
            for(i = 0 ; i < log_ino.cnt; ++i)
            {
                if(event->wd == log_ino.wd[i])
                {
                    seq = i;
                    break;
                }
            }
            if(-1 == seq)
            {
                //printf("\nNot found wd : %d. name : %s\n\n", event->wd, event->name);
                continue;
            }
            char temp_filename[256] = "";
            //temp_filename[0] = '\0';
            strcpy(temp_filename, root_ino.filePath);
            strcat(temp_filename, app_ino.names[log_ino.parentNum[seq]]);
            strcat(temp_filename, log_ino.names[seq]);
            strcat(temp_filename, "/syslog");
            if(event->mask & IN_CREATE)
            {
                if (!(event->mask & IN_ISDIR))
                {

                    if(0 ==strcmp("syslog", event->name))
                    {
                        //printf("**Log EVENT**\n[file] %s was created.\n", event->name);
                        inotify_rm_watch(log_ino.fd, log_ino.wd[seq]);
                        int wd = inotify_add_watch(log_ino.fd, temp_filename, IN_MODIFY);
                        if(-1 == wd)
                        {
                            cout<<temp_filename<<endl;
                            perror("handle_log_events, add_watch");
                            exit(EXIT_FAILURE);
                        }
                        log_ino.wd[seq] = wd;
                        log_ino.offset[seq] = 0;
                        printf("**Log EVENT**\n[file] %s was created.\n", event->name);
                        printf("[handle_log_events]\naddName : %s\npath : %s\n\n",
                               log_ino.names[seq], temp_filename);

#ifdef LOG_running

                        sprintf(Log_Message, "[handle_log_events]\ntotalNum : %d\t cnt : %d\naddName : %s\nadd_watch : %d\npath : %s",
                                log_ino.totalNum, log_ino.cnt, log_ino.names[seq],
                                log_ino.wd[seq],temp_filename);
                        writeLogMessage_INFO(fp_log, Log_Message);

#endif

                    }
                }
            }
            ///analyze the log and pick out reduce task to watch.
            else if(event->mask & IN_MODIFY)
            {
                /// Deal with log(FILE) MODIFY events.
                /// classify the log ,pick out reduce log and add to logTable_modify
                char line[256] = "";
                FILE * fp = NULL;
                if(NULL == (fp = fopen(temp_filename, "r")))
                {
                    cout<<"\n!!!   Can not open \""<<temp_filename<<"\"\n"<<endl;
                    continue;
                }
                fseek(fp, log_ino.offset[seq], SEEK_SET);
                while(fgets(line, 256, fp))
                {
                    line[strlen(line) - 1] = '\0';
                    //cout<<line<<endl;


                    log_ino.offset[seq] += strlen(line);
                    if(strstr(line + CLASSIFY_PREFIX_NUM, "Master"))
                    {
                        printf("[CLASSIFY]\n%s is running a [MASTER TAST].\n\n", log_ino.names[seq]);

#ifdef LOG_running

                        sprintf(Log_Message, "[handle_log_events]\n[CLASSIFY]\n%s is running a [MASTER TAST].",
                                log_ino.names[seq]);
                        writeLogMessage_INFO(fp_log, Log_Message);

#endif

#ifdef  NETWORK
                        /// application was create.
                        send_len = 0;

                        send_len = sendMessageToServer(app_CREATE, app_ino.names[log_ino.parentNum[seq]], fd_socket);
                        if(send_len < 0)
                        {
                            perror("handle_log_modify_events (app CREATE) ");
                        }

#endif

                        removeFromLogTable_seq(log_ino, seq);
                        break;
                    }
                    else if(strstr(line + CLASSIFY_PREFIX_NUM, "MapTask"))
                    {
                        printf("[CLASSIFY]\n%s is running a [MAP TASK].\n\n", log_ino.names[seq]);

#ifdef LOG_running

                        sprintf(Log_Message, "[handle_log_events]\n[CLASSIFY]\n%s is running a [MAP TASK].",
                                log_ino.names[seq]);
                        writeLogMessage_INFO(fp_log, Log_Message);

#endif

                        removeFromLogTable_seq(log_ino, seq);
                        break;
                    }
                    else if(strstr(line + CLASSIFY_PREFIX_NUM, "ReduceTask"))
                    {
                        printf("[CLASSIFY]\n%s is running a [READUCE TASK].\n%s\n\n", log_ino.names[seq], temp_filename);

#ifdef LOG_running

                        sprintf(Log_Message, "[handle_log_events]\n[CLASSIFY]\n%s is running a [READUCE TASK].\n%s",
                                log_ino.names[seq], temp_filename);
                        writeLogMessage_INFO(fp_log, Log_Message);

#endif

                        addToModi_logTable(modi_log_ino, temp_filename, log_ino.offset[seq],
                                           log_ino.parentNum[seq],log_ino.names[seq]);
                        removeFromLogTable_seq(log_ino, seq);
                    }
                }
                fclose(fp);

                //printf("[file] %d was modified. parentDir : %s\n", ++modify_num);
                //printf("[file] %s was modified. modify num : %d\n", log_ino->names[seq], ++log_ino->offset[seq]);
            }
        }
    }

}

/// Watch reduce log and deal with log MODIFY events.
void handle_log_modify_events(inotify_logStruct_modify &modi_log_ino)
{

    void removeFromModiLogTable_seq(inotify_logStruct_modify &modi_ino, int seq);

    char buffer[256 * (EVENT_SIZE)];
    int len;
    char * ptr;
    const struct inotify_event * event;
    while(1)
    {
        len = read(modi_log_ino.fd, buffer, sizeof(buffer));
        if (len == -1 && errno != EAGAIN)
        {
            perror("read() in handle_log_modify_events()");
            exit(EXIT_FAILURE);
        }

        /* If the nonblocking read() found no events to read, then
           it returns -1 with errno set to EAGAIN. In that case,
           we exit the loop. */

        if (len <= 0)
            break;
        /// Loop over all events in the buffer
        for (ptr = buffer; ptr < buffer + len;
                ptr += sizeof(struct inotify_event) + event->len)
        {
            event = (const struct inotify_event *) ptr;

            ///analyze the log and pick out reduce task to watch.
            if(event->mask & IN_MODIFY)
            {
                /// Deal with reduce log(FILE) MODIFY events.
                /// pick out when start a flow and volumn and when end and when shuffle is over
                int seq = -1, i;
                for(i = 0 ; i < modi_log_ino.cnt; ++i)
                {
                    if(event->wd == modi_log_ino.wd[i])
                    {
                        seq = i;
                        break;
                    }
                }
                if(-1 == seq)
                {
                    //printf("\nNot found wd : %d. name : %s\n\n", event->wd, event->name);
                    continue;
                }
                char line[512] = "";
                FILE * fp = modi_log_ino.fpArray[seq];
                char * p = NULL;
                char attempt[48] = "";
                unsigned long long flow_raw = 0, flow_length = 0;
                fseek(fp, modi_log_ino.offset[seq], SEEK_SET);
                while(fgets(line, 512, fp))
                {
                    //cout<<strlen(line)<<endl;
                    line[strlen(line) - 1] = '\0';
                    //cout<<line<<endl;
                    modi_log_ino.offset[seq] += strlen(line);
                    ///about to create a flow to shuffle
                    if(NULL != (p = strstr(line + MODIFY_PREFIX_NUM, "about to shuffle")))
                    {
                        p = strstr(p, "attempt_");
                        p = strtok(p, " ");
                        strcpy(attempt, p);
                        p = strtok(NULL, " ");
                        p = strtok(NULL, " ");
                        sscanf(p, "%llu", &flow_raw);
                        p = strtok(NULL, " ");
                        p = strtok(NULL, " ");
                        sscanf(p, "%llu", &flow_length);
                        p = strtok(NULL, " ");
                        p = strtok(NULL, " ");

                        printf("\n[[[MESSAGE]]]\n[REDUCE TASK] %s\n[FLOW START]\ndst : %s\nraw : %llu\nlength : %llu\n\n",
                               modi_log_ino.names[seq], attempt, flow_raw, flow_length);

#ifdef  NETWORK
                        if(flow_raw >= COMMUNICATION_THREAD || flow_length >= COMMUNICATION_THREAD)
                        {
                            /// flow start
                            send_len = 0;
                            //char message[LEN_message] = "";
                            /// not == "MEMORY"
                            /// == "MEMORY\n"   each log item end with a '\n'
                            if(0 == strcmp(p, "MEMORY"))
                            {
                                sprintf(message, "%s;%llu", attempt, flow_raw);
                            }
                            else
                            {
                                sprintf(message, "%s;%llu", attempt, flow_length);
                            }
                            send_len = sendMessageToServer(flow_START, message, fd_socket);
                            if(send_len < 0)
                            {
                                perror("handle_log_modify_events (FLOW START) ");
                            }
                        }


#endif

                    }
                    else if(NULL != (p = strstr(line + MODIFY_PREFIX_NUM, "Read")))
                    {
                        p = strtok(p, " ");
                        p = strtok(NULL, " ");
                        sscanf(p, "%llu", &flow_raw);
                        p = strtok(NULL, " ");
                        p = strtok(NULL, " ");
                        p = strtok(NULL, " ");
                        p = strtok(NULL, " ");
                        p = strtok(NULL, " ");
                        strcpy(attempt, p);
                        //int l = strlen(attempt);
                        //attempt[l-1] = '\0';
                        printf("\n[[[MESSAGE]]]\n[REDUCE TASK] %s\n[FLOW OVER]\ndst : %s\nraw : %llu\n\n",
                               modi_log_ino.names[seq], attempt, flow_raw);

#ifdef  NETWORK
                        if(flow_raw >= COMMUNICATION_THREAD)
                        {
                            /// flow end
                            //sendMessageToServer();
                            send_len = 0;
                            //char message[LEN_message] = "";
                            sprintf(message, "%s;%llu", attempt, flow_raw);
                            send_len = sendMessageToServer(flow_OVER, message, fd_socket);
                            if(send_len < 0)
                            {
                                perror("handle_log_modify_events (FLOW OVER) ");
                            }
                        }
#endif

                    }
                    else if(NULL != (p = strstr(line + MODIFY_PREFIX_NUM, "EventFetcher is interrupted")))
                    {
                        printf("\n[[[MESSAGE]]]\n[REDUCE TASK] %s is over.\n\n", modi_log_ino.names[seq]);
#ifdef  NETWORK
                        /// reduce over
                        /// 后期异常处理可能会用到
                        /**
                        //sendMessageToServer();
                        unsigned int send_len = 0;
                        send_len = sendMessageToServer(reduce_OVER, modi_log_ino->names[seq], fd_socket);
                        if(send_len < 0)
                        {
                            perror("handle_log_modify_events (reduce over) ");
                        }
                        **/
#endif

#ifdef LOG_running


                        sprintf(Log_Message, "[[[MESSAGE]]]\n[REDUCE TASK] %s is over.", modi_log_ino.names[seq]);
                        writeLogMessage_INFO(fp_log, Log_Message);

#endif

                        removeFromModiLogTable_seq(modi_log_ino, seq);
                    }
                }
                //printf("[file] %d was modified. parentDir : %s\n", ++modify_num);
                //printf("[file] %s was modified. modify num : %d\n", log_ino->names[seq], ++log_ino->offset[seq]);
            }
        }
    }

}


/// Watch mid_rootPath and receive appliaction CREATE events.
void handle_midapp_events(inotify_rootStruct &mid_root_ino, inotify_appStruct &mid_app_ino)
{

    char buffer[256 * (EVENT_SIZE)];
    int len;
    char * ptr;
    const struct inotify_event * event;
    while(1)
    {
        len = read(mid_root_ino.fd, buffer, sizeof(buffer));
        if (len == -1 && errno != EAGAIN)
        {
            perror("read() in \"handle_midapp_events()\".");
            exit(EXIT_FAILURE);
        }

        /* If the nonblocking read() found no events to read, then
           it returns -1 with errno set to EAGAIN. In that case,
           we exit the loop. */

        if (len <= 0)
            break;
        /// Loop over all events in the buffer
        for (ptr = buffer; ptr < buffer + len;
                ptr += sizeof(struct inotify_event) + event->len)
        {
            event = (const struct inotify_event *) ptr;
            if(event->mask & IN_CREATE)
            {
                if (event->mask & IN_ISDIR)
                {
                    /// Deal with application(DIR) CREATE events.
                    //getCurrentTime_Print();
                    //printf("**MIDDLE**\n[directory] %s was created.\n", event->name);
                    if((mid_app_ino.cnt + 1) >= mid_app_ino.totalNum)
                    {
                        if(!realloc_inotify_appStruct(mid_app_ino, (ADD_appStruct_NUM) * 2))
                        {
                            perror("realloc_inotify_appStruct in \"handle_midapp_events()\".");
                            exit(EXIT_FAILURE);
                        }
                    }
                    int seq = mid_app_ino.cnt;
                    mid_app_ino.names[seq][0] = '/';
                    mid_app_ino.names[seq][1] = '\0';
                    strcat(mid_app_ino.names[seq], event->name);
                    char temp_filename[256] = "";
                    //temp_filename[0] = '\0';
                    strcpy(temp_filename, mid_root_ino.filePath);
                    strcat(temp_filename, mid_app_ino.names[seq]);
                    int wd = inotify_add_watch(mid_app_ino.fd, temp_filename, IN_DELETE_SELF);
                    if(-1 == wd)
                    {
                        cout<<temp_filename<<endl;
                        perror("handle_midapp_events, add_watch");
                        exit(EXIT_FAILURE);
                    }
                    mid_app_ino.wd[seq++] = wd;
                    strcat(temp_filename, "/output");
                    if(-1 == access(temp_filename, 0))
                    {
                        mkdir(temp_filename,S_IRWXU);
                    }
                    wd = inotify_add_watch(mid_app_ino.fd, temp_filename, IN_CREATE);
                    mid_app_ino.wd[seq] = wd;
                    mid_app_ino.cnt = + 2;
                    printf("**MIDDLE**\n[directory] %s was created.\n", event->name);
                    printf("[handle_midapp_events]\naddName : %s\npath : %s\n\n",
                           mid_app_ino.names[seq], temp_filename);

#ifdef LOG_running

                    sprintf(Log_Message, "[handle_midapp_events]\ntotalNum : %d\t cnt : %d\naddName : %s\nadd_watch : %d\npath : %s",
                            mid_app_ino.totalNum, mid_app_ino.cnt, mid_app_ino.names[seq],
                            mid_app_ino.wd[seq], temp_filename);
                    writeLogMessage_INFO(fp_log, Log_Message);

#endif

                }
            }
        }
    }

}

void handle_midapp_del_create_events(  inotify_rootStruct &mid_root_ino,
                                       inotify_appStruct &mid_app_ino,
                                       inotify_rootStruct &root_ino,
                                       inotify_appStruct &app_ino,
                                       inotify_logStruct &log_ino,
                                       inotify_logStruct_modify &modi_ino)
{
    void removeFromAppTable(inotify_appStruct &app_ino, char * appName,
                            inotify_logStruct &log_ino,inotify_logStruct_modify &modi_ino);


    char buffer[256 * (EVENT_SIZE)];
    int len;
    char * ptr;
    const struct inotify_event * event;
    while(1)
    {
        len = read(mid_app_ino.fd, buffer, sizeof(buffer));
        if (len == -1 && errno != EAGAIN)
        {
            perror("read() in \"handle_midapp_del_events()\".");
            exit(EXIT_FAILURE);
        }

        /* If the nonblocking read() found no events to read, then
           it returns -1 with errno set to EAGAIN. In that case,
           we exit the loop. */

        if (len <= 0)
            break;
        /// Loop over all events in the buffer
        for (ptr = buffer; ptr < buffer + len;
                ptr += sizeof(struct inotify_event) + event->len)
        {
            event = (const struct inotify_event *) ptr;
            if(event->mask & IN_DELETE_SELF)
            {
                //cout<<"h1"<<endl;
                //cout<<"h2"<<endl;
                /// Deal with application(DIR) DELETE_SELF events.

                if(mid_app_ino.cnt < 0)
                {
                    perror("Error cnt num in \"handle_midapp_del_events()\".");
                    exit(EXIT_FAILURE);
                }
                int i,seq = -1,last = mid_app_ino.cnt -2;
                for(i = 0; i < mid_app_ino.cnt; ++i)
                {
                    if(event->wd == mid_app_ino.wd[i])
                    {
                        seq = i;
                        break;
                    }
                }
                if(seq == -1)
                {
                    continue;
                }
                //printf("**MIDDLE**\n[directory] %s was deleted.\n", mid_app_ino.names[seq]);
                char appName[64];
                char temp_filename[256] = "";
                //temp_filename[0] = '\0';
                strcpy(temp_filename, mid_root_ino.filePath);
                strcat(temp_filename, mid_app_ino.names[seq]);
                strcpy(appName, mid_app_ino.names[seq]);
                inotify_rm_watch(mid_app_ino.fd, event->wd);
                inotify_rm_watch(mid_app_ino.fd, mid_app_ino.wd[++seq]);
                if(seq != last)
                {
                    mid_app_ino.wd[seq] = mid_app_ino.wd[last];
                    strcpy(mid_app_ino.names[seq], mid_app_ino.names[last]);
                    ++last;
                    ++seq;
                    mid_app_ino.wd[seq] = mid_app_ino.wd[last];
                    strcpy(mid_app_ino.names[seq], mid_app_ino.names[last]);
                }

                mid_app_ino.cnt -= 2;
                printf("**MIDDLE**\n[directory] %s was deleted.\n", mid_app_ino.names[seq]);
                printf("[handle_midapp_del_create_events]  [IN_DELETE_SELF]\ndeleteName : %s\npath : %s\n\n",
                       mid_app_ino.names[seq], temp_filename);

#ifdef LOG_running

                sprintf(Log_Message, "[handle_midapp_del_create_events]  [IN_DELETE_SELF]\ntotalNum : %d\t cnt : %d\ndeleteName : %s\npath : %s",
                        mid_app_ino.totalNum, mid_app_ino.cnt,
                        mid_app_ino.names[seq], temp_filename);
                writeLogMessage_INFO(fp_log, Log_Message);

#endif

#ifdef  NETWORK
                /// application is over and middle data was deleted.
                send_len = 0;
                send_len = sendMessageToServer(app_OVER, mid_app_ino.names[seq], fd_socket);
                if(send_len < 0)
                {
                    perror("handle_midapp_del_create_events (IN_DELETE_SELF)");
                }
#endif

                removeFromAppTable(app_ino, appName, log_ino, modi_ino);

            }

            else if(event->mask & IN_CREATE)
            {
                if(strstr(event->name, "attempt"))
                {
                    char attemptDir[64] = "", * p;
                    int i,seq = -1;
                    for(i = 0; i < mid_app_ino.cnt; ++i)
                    {
                        if(event->wd == mid_app_ino.wd[i])
                        {
                            seq = i;
                            break;
                        }
                    }
                    if(seq == -1)
                    {
                        continue;
                    }
                    char temp_filename[256] = "";
                    strcpy(temp_filename, mid_root_ino.filePath);
                    strcat(temp_filename, mid_app_ino.names[seq]);
                    printf("**MIDDLE**\n[directory] %s was created.\n", event->name);
                    printf("[handle_midapp_del_create_events]  [IN_CREATE]\ncreateName : %s\npath : %s\n\n",
                           event->name, temp_filename);
#ifdef LOG_running

                    sprintf(Log_Message, "[handle_midapp_del_create_events]  [IN_CREATE]\ntotalNum : %d\t cnt : %d\ncreateName : %s\npath : %s",
                            mid_app_ino.totalNum, mid_app_ino.cnt, event->name, temp_filename);
                    writeLogMessage_INFO(fp_log, Log_Message);

#endif

                    strcpy(attemptDir, event->name);
                    if(NULL != (p = index(attemptDir + 8, 'm')))
                    {
                        printf("**MIDDLE**\nIt is a MAP TASK.\n\n");
#ifdef  NETWORK
                        /// send the attempt create to server.
                        send_len = 0;
                        send_len = sendMessageToServer(attempt_CREATE, attemptDir, fd_socket);
                        if(send_len < 0)
                        {
                            perror("handle_midapp_del_create_events (IN_CREATE)");
                        }
#endif
                    }

                }
            }
            //if(event->mask & IN_DELETE)
            //{
            //if(strstr(event->name, "attempt"))
            //{
            //printf("**MIDDLE**\n[directory] %s was deleted.\n\n", event->name);
            //}
            //}
        }
    }

}


void removeFromLogTable(inotify_logStruct &log_ino, int parentNum)
{

    int num = log_ino.cnt;
    if(0 == num )   return ;
    int i, last = num-1;
    for(i = num -1; i >= 0; --i)
    {
        if(parentNum == log_ino.parentNum[i])
        {
            inotify_rm_watch(log_ino.fd, log_ino.wd[i]);
            printf("[removeFromLogTable]\nremoveName : %s\n\n", log_ino.names[i]);

#ifdef LOG_running

            sprintf(Log_Message, "[removeFromLogTable]\ntotalNum : %d\t cnt : %d\nremoveName : %s",
                    log_ino.totalNum, last, log_ino.names[i]);
            writeLogMessage_INFO(fp_log, Log_Message);

#endif

            if(i != last)
            {
                log_ino.wd[i] = log_ino.wd[last];
                log_ino.parentNum[i] = log_ino.parentNum[last];
                log_ino.offset[i] = log_ino.offset[last];
                strcpy(log_ino.names[i], log_ino.names[last]);
            }
            --last;
        }
    }
    log_ino.cnt = last + 1;



    return ;
}

void removeFromLogTable_seq(inotify_logStruct &log_ino, int seq)
{

    if(seq < 0 || seq > log_ino.cnt)
    {
        perror("Wrong SEQ in removeFromLogTable_seq().");
        return ;
    }
    int num = log_ino.cnt;
    if(0 == num )   return ;
    int last = num-1;
    inotify_rm_watch(log_ino.fd, log_ino.wd[seq]);
    printf("[removeFromLogTable_seq]\nremoveName : %s\n\n", log_ino.names[seq]);

#ifdef LOG_running

    sprintf(Log_Message, "[removeFromLogTable_seq]\ntotalNum : %d\t cnt : %d\nremoveName : %s",
            log_ino.totalNum, last, log_ino.names[seq]);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    if(seq != last)
    {
        log_ino.wd[seq] = log_ino.wd[last];
        log_ino.parentNum[seq] = log_ino.parentNum[last];
        log_ino.offset[seq] = log_ino.offset[last];
        strcpy(log_ino.names[seq], log_ino.names[last]);
    }
    --log_ino.cnt;


    return ;
}

void removeFromAppTable(inotify_appStruct &app_ino, char * appName,
                        inotify_logStruct &log_ino, inotify_logStruct_modify &modi_ino)
{
    void removeFromModiLogTable(inotify_logStruct_modify &modi_ino, int parentNum);


    int i, num = app_ino.cnt,seq = -1;
    if(0 == num )   return ;
    for(i = 0; i < num; ++i)
    {
        if(!strcasecmp(appName, app_ino.names[i]))
        {
            seq = i;
            break;
        }
    }
    if(-1 == seq)   return;
    int last = num -1;
    inotify_rm_watch(app_ino.fd, app_ino.wd[seq]);
    printf("[removeFromAppTable]\nremoveName : %s\n\n", app_ino.names[seq]);

#ifdef LOG_running

    sprintf(Log_Message, "[removeFromAppTable]\ntotalNum : %d\t cnt : %d\nremoveName : %s",
            app_ino.totalNum, last, app_ino.names[seq]);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    if(seq != last)
    {
        app_ino.wd[seq] = app_ino.wd[last];
        strcpy(app_ino.names[seq], app_ino.names[last]);
    }
    else
    {
        ;
    }
    --app_ino.cnt;
    removeFromLogTable(log_ino, seq);
    removeFromModiLogTable(modi_ino, seq);


    return ;
}

void removeFromModiLogTable(inotify_logStruct_modify &modi_ino, int parentNum)
{

    int num = modi_ino.cnt;
    if(0 == num )   return ;
    int i, last = num-1;
    for(i = num -1; i >= 0; --i)
    {
        if(parentNum == modi_ino.parentNum[i])
        {
            inotify_rm_watch(modi_ino.fd, modi_ino.wd[i]);
            if(i != last)
            {
                modi_ino.wd[i] = modi_ino.wd[last];
                modi_ino.parentNum[i] = modi_ino.parentNum[last];
                modi_ino.offset[i] = modi_ino.offset[last];
                fclose(modi_ino.fpArray[i]);
                strcpy(modi_ino.names[i], modi_ino.names[last]);
            }
            --last;
        }
    }
    modi_ino.cnt = last + 1;
    printf("[removeFromModiLogTable]\ntotalNum : %d\t cnt : %d\n\n",
                modi_ino.totalNum, modi_ino.cnt);

#ifdef LOG_running

    sprintf(Log_Message, "[removeFromModiLogTable]\ntotalNum : %d\t cnt : %d",
                modi_ino.totalNum, modi_ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif


    return ;
}

void removeFromModiLogTable_seq(inotify_logStruct_modify &modi_ino, int seq)
{

    if(seq < 0 || seq > modi_ino.cnt)
    {
        perror("Wrong SEQ in removeFromModiLogTable_seq().");
        return ;
    }
    int num = modi_ino.cnt;
    if(0 == num)    return ;
    int last = num-1;
    inotify_rm_watch(modi_ino.fd, modi_ino.wd[seq]);
    if(seq != last)
    {
        modi_ino.wd[seq] = modi_ino.wd[last];
        modi_ino.parentNum[seq] = modi_ino.parentNum[last];
        modi_ino.offset[seq] = modi_ino.offset[last];
        fclose(modi_ino.fpArray[seq]);
        strcpy(modi_ino.names[seq], modi_ino.names[last]);
    }
    --modi_ino.cnt;
    printf("[removeFromModiLogTable_seq]\ntotalNum : %d\t cnt : %d\n\n",
                modi_ino.totalNum, modi_ino.cnt);

#ifdef LOG_running

    sprintf(Log_Message, "[removeFromModiLogTable_seq]\ntotalNum : %d\t cnt : %d",
                modi_ino.totalNum, modi_ino.cnt);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

    return ;
}


void addToModi_logTable(inotify_logStruct_modify &modi_ino,
                        char *fileName, int fileOffset,
                        int parentNum, char *names)
{
    //cout<<"\nwelcome\n"<<endl;
    if(NULL == fileName)
    {
        perror("null pointer, addToModi_logTable ");
        return ;
    }
    FILE *fp_tmp = fopen(fileName, "r");
    if(NULL == fp_tmp)
    {
        perror("can not open, addToModi_logTable ");
        return ;
    }
    if(modi_ino.cnt >= modi_ino.totalNum)
    {
        if(!realloc_inotify_logStruct_modify(modi_ino, ADD_logStruct_NUM))
        {
            perror("realloc_inotify_logStruct_modify in addToModi_logTable ");
            exit(EXIT_FAILURE);
        }
    }

    int seq = modi_ino.cnt;
    int wd = inotify_add_watch(modi_ino.fd, fileName, IN_MODIFY);
    if(-1 == wd)
    {
        cout<<fileName<<endl;
        perror("addToModi_logTable, add_watch");
        exit(EXIT_FAILURE);
    }
    modi_ino.wd[seq] = wd;
    modi_ino.offset[seq] = fileOffset;
    modi_ino.parentNum[seq] = parentNum;
    modi_ino.fpArray[seq] = fp_tmp;
    strcpy(modi_ino.names[seq], names);
    ++modi_ino.cnt;
    printf("[addToModi_logTable]\nname : %s\n\n", fileName);

#ifdef LOG_running

    sprintf(Log_Message, "[addToModi_logTable]\ntotalNum : %d\t cnt : %d\nname : %s\n\n",
                modi_ino.totalNum, modi_ino.cnt, fileName);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif

}

bool createTcpConnection(char * servIPFile, int port, struct sockaddr_in &serv_addr, int &fd_sock)
{
    if(NULL == servIPFile)
    {
        printf("servIPFile is a NULL pointer.\n\n");
        return false;
    }

    FILE *fp = fopen(servIPFile, "r");
    if(NULL == fp)
    {
        printf("Can not open [FILE] : %s.\n\n", servIPFile);
        return false;
    }
    char IP[24] = "";
    fgets(IP, 24, fp);
    IP[strlen(IP) - 1] = '\0';
    if(!ipFormatCheck(IP))
    {
        printf("wrong IP address : Format Error\n%s\n\n", IP);
        return false;
    }

    memset(&serv_addr, 0, sizeof(struct sockaddr_in));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(serv_PORT);                ///set server port
    serv_addr.sin_addr.s_addr = inet_addr(IP); ///set server IP

    if((fd_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror("socket ");
        cout<<endl;
        return false;
    }

    if(connect(fd_sock, (struct sockaddr *) &serv_addr, sizeof(struct sockaddr)) < 0)
    {
        perror("cant not connect to server ");
        cout<<endl;
        return false;
    }

    printf("[createTcpConnection]\nsocket : %d\nPort : %d\nserverIP : %s\n\n",
                fd_sock, port, IP);

#ifdef LOG_running

    sprintf(Log_Message, "[createTcpConnection]\nsocket : %d\nPort : %d\nserverIP : %s",
                fd_sock, port, IP);
    writeLogMessage_INFO(fp_log, Log_Message);

#endif


    return true;
}



int main(int argc, char *argv[])
{


    if(!MY_getHostName(HOST_Name, HOST_LEN))
    {
        cout<<"Error in gethostname().\n"<<endl;
        exit(EXIT_FAILURE);
    }
    fprintf(stdout, "host : %s\n", HOST_Name);

    if(!MY_getHostIpAddress(HOST_IpAddr, IP_LEN))
    {
        cout<<"Error in getHostIpAddress().\n"<<endl;
        exit(EXIT_FAILURE);
    }
    fprintf(stdout, "eth0 : %s\n\n", HOST_IpAddr);

#ifdef LOG_running

    char logPath[] = "./HW_AGENT_LOG.txt";
    char logPath_message[] = "./HW_AGENT_LOG_MESSAGE.txt";

    fp_log = fopen(logPath, "w");
    if(NULL == fp_log)
    {
        perror("error in create log ");
        cout<<endl;
        exit(EXIT_FAILURE);
    }
    fclose(fp_log);
    fp_log = fopen(logPath, "a");

    message_log = fopen(logPath_message, "w");
    if(NULL == message_log)
    {
        perror("error in create message log ");
        cout<<endl;
        exit(EXIT_FAILURE);
    }
    fclose(message_log);
    message_log = fopen(logPath_message, "a");

    sprintf(Log_Message, "**Agent START**");
    writeLogMessage_INFO(fp_log, Log_Message);

#endif


#ifdef  NETWORK

    struct sockaddr_in serv_addr;
    char ipFile[] = "./servip.txt";
    if(!createTcpConnection(ipFile, serv_PORT, serv_addr, fd_socket))
    {
        cout<<"Error in createTcpConnection().\n"<<endl;
        close(fd_socket);
        exit(EXIT_FAILURE);
    }

    /// Register this host to server.
    sendMessageToServer(host_REGISTER, HOST_IpAddr, fd_socket);

#endif


    inotify_rootStruct rootTable;
    inotify_appStruct appTable;
    inotify_logStruct logTable;

    inotify_rootStruct mid_rootTable;
    inotify_appStruct mid_appTable;
    inotify_logStruct_modify logTable_modi;


    char buf;


    const char rootPath[] = "/usr/local/hadoop/logs/userlogs";
    const char mid_rootPath[] = "/usr/local/hadoop/tmp/nm-local-dir/usercache/hadoop/appcache";

    char mid_rootPath_t[256] = "/usr/local/hadoop/tmp/nm-local-dir/usercache";
    if(-1 == access(mid_rootPath, 0))
    {
        strcat(mid_rootPath_t, "/hadoop");
        mkdir(mid_rootPath_t,S_IRWXU);
        strcat(mid_rootPath_t, "/appcache");
        mkdir(mid_rootPath_t,S_IRWXU);
    }

    /// Initial rootTable
    if (!Init_inotifyTable_root( rootTable, rootPath))
    {
        cout<<"Error in Init_inotifyTable_root().\n"<<endl;
        exit(EXIT_FAILURE);
    }

    /// Initial appTable
    if (!Init_inotifyTable_app( appTable, INIT_appStruct_NUM))
    {
        cout<<"Error in Init_inotifyTable_app().\n"<<endl;
        exit(EXIT_FAILURE);
    }

    /// Initial logTable
    if (!Init_inotifyTable_log( logTable, INIT_logStruct_NUM))
    {
        cout<<"Error in Init_inotifyTable_log().\n"<<endl;
        exit(EXIT_FAILURE);
    }

    /// Initial middle part.
    if(!Init_inotifyTable_root( mid_rootTable, mid_rootPath))
    {
        cout<<"Error in Init_inotifyTable_root (middle).\n"<<endl;
        exit(EXIT_FAILURE);
    }

    /// Initial mid_appTable
    if (!Init_inotifyTable_app( mid_appTable, (INIT_appStruct_NUM) * 2))
    {
        cout<<"Error in Init_inotifyTable_app (middle).\n"<<endl;
        exit(EXIT_FAILURE);
    }

    /// Initial logTable_modify
    if (!Init_inotifyTable_log_modify( logTable_modi, INIT_logStruct_NUM))
    {
        cout<<"Error in Init_inotifyTable_log_modify().\n"<<endl;
        exit(EXIT_FAILURE);
    }

    /* Prepare for polling */

    nfds_t nfds = 8;
    struct pollfd fds[nfds];
    int poll_num;           /// receive the return of poll();


    /// Console input
    fds[0].fd = STDIN_FILENO;
    fds[0].events = POLLIN;

    /// Inotify input

    fds[1].fd = rootTable.fd;
    fds[1].events = POLLIN;

    fds[2].fd = appTable.fd;
    fds[2].events = POLLIN;

    fds[3].fd = logTable.fd;
    fds[3].events = POLLIN;

    fds[4].fd = mid_rootTable.fd;
    fds[4].events = POLLIN;

    fds[5].fd = mid_appTable.fd;
    fds[5].events = POLLIN;

    fds[6].fd = logTable_modi.fd;
    fds[6].events = POLLIN;

#ifdef NETWORK

    fds[7].fd = fd_socket;
    fds[7].events = POLLIN;

#endif

    printf("Press ENTER key to terminate.\n\n");
    /// Wait for events and/or terminal input
    printf("root path : %s\n",rootTable.filePath);
    printf("root path (mid): %s\n",mid_rootTable.filePath);
    printf("\nListening for EVENTS...\n\n");



    while (1)
    {
        /* Poll the file descriptors described by the NFDS structures starting at
        FDS.  If TIMEOUT is nonzero and not -1, allow TIMEOUT milliseconds for
        an event to occur; if TIMEOUT is -1, block until an event occurs.
        Returns the number of file descriptors with events, zero if timed out,
        or -1 for errors.

        This function is a cancellation point and therefore not marked with
        __THROW.  */
        poll_num = poll(fds, nfds, -1);
        if (poll_num == -1)
        {
            if (errno == EINTR)
                continue;
            perror("poll");
            exit(EXIT_FAILURE);
        }

        if (poll_num > 0)
        {

            if (fds[0].revents & POLLIN)
            {

                /* Console input is available. Empty stdin and quit */
                while (read(STDIN_FILENO, &buf, 1) > 0 && buf != '\n')
                    continue;
                break;
            }
            ///handle app events
            else if (fds[1].revents & POLLIN)
            {

                /* Inotify application events are available */
                handle_app_events( rootTable, appTable);
            }
            ///handle container events
            else if (fds[2].revents & POLLIN)
            {

                /* Inotify container events are available */
                handle_con_events( rootTable, appTable, logTable);
            }
            ///handle log events
            else if (fds[3].revents & POLLIN)
            {

                /* Inotify log events are available */
                handle_log_events( rootTable, appTable, logTable, logTable_modi);
            }

            else if (fds[4].revents & POLLIN)
            {


                handle_midapp_events( mid_rootTable, mid_appTable);
            }

            else if (fds[5].revents & POLLIN)
            {


                handle_midapp_del_create_events(mid_rootTable, mid_appTable, rootTable, appTable, logTable, logTable_modi);
            }

            else if (fds[6].revents & POLLIN)
            {

                handle_log_modify_events( logTable_modi);
            }

#ifdef NETWORK

            /// deal with the message from server
            else if (fds[7].revents & POLLIN)
            {
                char                recv_Message[256];
                int nread = read(fd_socket, recv_Message, sizeof(recv_Message));
                if(nread <= 0)      ///over or error
                {
                    printf("Disconnected with socket %d . so close it.\n", fd_socket);
                    close(fd_socket);
                    continue;
                }

                printf("Received : %s\n", recv_Message);

            }
#endif
        }
    } ///while

#ifdef NETWORK

    ///logout this host to server.
    sendMessageToServer(host_LOGOUT, HOST_IpAddr, fd_socket);
    close(fd_socket);

#endif




    /// Release space in tables and Close inotify file descriptor

    release_inotify_logStruct( logTable);
    release_inotify_appStruct( mid_appTable);
    release_inotify_logStruct_modify( logTable_modi);
    release_inotify_appStruct( appTable);

    close(mid_rootTable.fd);

    close(rootTable.fd);



    printf("Listening for events STOPPED.\n\n");


#ifdef LOG_running

    sprintf(Log_Message, "**Agent END**");
    writeLogMessage_INFO(fp_log, Log_Message);
    fclose(fp_log);
    fclose(message_log);

#endif

    return 0;
}
