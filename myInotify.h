#define EVENT_SIZE ( sizeof (struct inotify_event))
#define BUF_LEN (1024 * (EVENT_SIZE + 48))

//#define INIT_NUM 40


#define INIT_appStruct_NUM      20
#define ADD_appStruct_NUM       10

#define INIT_logStruct_NUM      20
#define ADD_logStruct_NUM       10

#define CLASSIFY_PREFIX_NUM     80
#define MODIFY_PREFIX_NUM       80


typedef char iString[48];

typedef struct
{
    int fd;
    int wd;
    char filePath[256];
}inotify_rootStruct;

typedef struct
{
    int fd;             ///add all wds to fd;
    int *wd;            ///wd array pointer
    int totalNum;       ///total num of the allocated wd;
    int cnt;            ///used num of the allocated wd;
    iString *names;      ///the name of what wd is watching;
}inotify_appStruct;

/*

typedef struct
{
    //int fd;             ///add all wds to fd;
    //int *wd;            ///wd array pointer
    int totalNum;       ///total num of the allocated wd;
    int cnt;            ///used num of the allocated wd;
    iString *names;      ///the name of what wd is watching;
    int *parentNum;     ///the sequence number of parent directory in appStruct.
}inotify_conStruct;

*/

typedef struct
{
    int fd;             ///add all wds to fd;
    int *wd;            ///wd array pointer
    int totalNum;       ///total num of the allocated wd;
    int cnt;            ///used num of the allocated wd;
    iString *names;      ///the name of what wd is watching,actually container's name.
    int *parentNum;     ///the sequence number of parent directory in conStruct.
    int *offset;
}inotify_logStruct;

typedef FILE * FILEPointer;

typedef struct
{
    int fd;             ///add all wds to fd;
    int *wd;            ///wd array pointer
    int totalNum;       ///total num of the allocated wd;
    int cnt;            ///used num of the allocated wd;
    int *parentNum;     ///the sequence number of parent directory in conStruct.
    int *offset;
    iString *names;      ///the name of what wd is watching,actually container's name.
    FILEPointer *fpArray;   ///file pointer Array;
}inotify_logStruct_modify ;


