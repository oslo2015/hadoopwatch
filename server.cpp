#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <sys/epoll.h>
#include <errno.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <arpa/inet.h>


#include "netForecastDS.h"
#include "sockConstants.h"


using namespace std;

#define MAXEPOLL    3000
#define MAXLINE     1024
#define PORT        1987
#define MAXBACK     1000

///#define LOG

/// global variables
int recv_Num = 0;
///

#define LOG_Running

#ifdef LOG_Running

FILE *fp_log = NULL;
char Log_Message[256] = "";

char * getCurrentTime()
{
    time_t rawtime;
    struct tm * timeinfo;

    time ( &rawtime );
    timeinfo = localtime ( &rawtime );

    return asctime(timeinfo);
}


void writeLogMessage(FILE *fp_log, char *log)
{
    time_t rawtime;
    struct tm * timeinfo;

    time ( &rawtime );
    timeinfo = localtime ( &rawtime );

    fprintf(fp_log, "\n%s[INFO]\n%s\n", asctime(timeinfo), log);

}

#endif


/// initial cluster
bool Init_HostOnCluster(struct HostOnCluster & hoc, int host_num, int attempt_num)
{
    if(host_num < 0 || attempt_num < 0)
    {
        cout<<"Wrong num.\n"<<endl;
        return false;
    }
    if(host_num > 100 || attempt_num >50)
    {
        cout<<"Num is too big for memory.\nPlease reset it.\n"<<endl;
        return false;
    }

    hoc.host = new struct NodeInfo[host_num];
    if(NULL == hoc.host)
    {
        perror("Null pointer---hoc.host ");
        return false;
    }
    hoc.totalNum_host = host_num;
    hoc.cnt_host = 0;

    int i;
    for(i = 0; i < host_num; ++i)
    {
        hoc.host[i].attemptOnNode       = NULL;
        hoc.host[i].totalNum_attempt    = 0;
        hoc.host[i].cnt_attempt         = 0;
        hoc.host[i].addr                = 0x00000000;
    }

#ifdef LOG_Running

    sprintf(Log_Message, "[Init_HostOnCluster]\n[Cluster]\ntotalNum : %d\ncnt : %d",
            hoc.totalNum_host, hoc.cnt_host);
    writeLogMessage(fp_log, Log_Message);

#endif



    return true;
}

bool Init_NetArray2D(struct NetArray2D & na2d, int host_num)
{
    if(host_num < 0)
    {
        cout<<"Wrong host_num.\n"<<endl;
        return false;
    }
    if(host_num > 200)
    {
        cout<<"Num is too big for memory.\nPlease reset it.\n"<<endl;
        return false;
    }

    na2d.totalNum = host_num;
    na2d.cnt = 0;

    na2d.fcstVolumn = new unsigned long long *[host_num];
    if(NULL == na2d.fcstVolumn)
    {
        perror("Null pointer---fcstVolumn ");
        return false;
    }
    int i;
    unsigned long long * tmp;
    for(i = 0; i < host_num; ++i)
    {
        tmp = new unsigned long long[host_num];
        if(NULL == tmp)
        {
            perror("Null pointer---fcstVolumn[] ");
            return false;
        }
        na2d.fcstVolumn[i] = tmp;
    }

#ifdef LOG_Running

    sprintf(Log_Message, "[Init_NetArray2D]\n[NetArray2D]\ntotalNum : %d\ncnt : %d",
            na2d.totalNum, na2d.cnt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return true;
}

bool Init_Cluster(struct HostOnCluster & hoc, struct NetArray2D & na2d, int host_num, int att_num)
{
    if(host_num < 0 || att_num < 0)
    {
        cout<<"Wrong num.\n"<<endl;
        return false;
    }
    if(host_num > 100 || att_num >50)
    {
        cout<<"Num is too big for memory.\nPlease reset it.\n"<<endl;
        return false;
    }

    return Init_HostOnCluster(hoc, host_num, att_num) && Init_NetArray2D(na2d, host_num);
}


/// release cluster
void release_HostOnCluster(struct HostOnCluster & hoc)
{

    /// free Nodeinfo on each host
    int cnt = hoc.cnt_host;
    int i;
    for(i = 0; i < cnt; ++i)
    {
        delete[] hoc.host[i].attemptOnNode;
    }

    /// free host array
    delete[] hoc.host;
    hoc.totalNum_host = 0;
    hoc.cnt_host = 0;

#ifdef LOG_Running

    sprintf(Log_Message, "[release_HostOnCluster]]\n[HostOnCluster]\ntotalNum : %d\ncnt : %d",
            hoc.totalNum_host, hoc.cnt_host);
    writeLogMessage(fp_log, Log_Message);

#endif

    return ;
}

void release_NetArray2D(struct NetArray2D & na2d)
{
    int cnt = na2d.cnt;
    int i;
    for(i = 0; i < cnt; ++i)
    {
        delete[] na2d.fcstVolumn[i];
    }
    delete[] na2d.fcstVolumn;
    na2d.cnt = 0;
    na2d.totalNum = 0;

#ifdef LOG_Running

    sprintf(Log_Message, "[release_NetArray2D]\n[NetArray2D]\ntotalNum : %d\ncnt : %d",
            na2d.totalNum, na2d.cnt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return ;
}

void release_Cluster(struct HostOnCluster & hoc, struct NetArray2D & na2d)
{
    release_HostOnCluster(hoc);
    release_NetArray2D(na2d);

}


/// realloc
bool realloc_HostOnCluster(struct HostOnCluster &hoc, int addNum)
{
    if(addNum <= 0)
    {
        cout<<"Wrong number in realloc_HostOnCluster().\n"<<endl;
        return false;
    }
    int cnt = hoc.cnt_host;
    int newNum = hoc.totalNum_host + addNum;
    struct NodeInfo *tmp_host = new struct NodeInfo[newNum];
    if(NULL == tmp_host)
    {
        perror("Null pointer---tmp_host ");
        return false;
    }

    int i;
    for(i = 0; i < cnt; ++i)
    {
        tmp_host[i].addr               = hoc.host[i].addr;
        tmp_host[i].attemptOnNode      = hoc.host[i].attemptOnNode;
        tmp_host[i].totalNum_attempt   = hoc.host[i].totalNum_attempt;
        tmp_host[i].cnt_attempt        = hoc.host[i].cnt_attempt;
    }
    for(; i < newNum; ++i)
    {
        tmp_host[i].addr                = 0x00000000;
        tmp_host[i].attemptOnNode       = NULL;
        tmp_host[i].totalNum_attempt    = 0;
        tmp_host[i].cnt_attempt         = 0;
    }

    delete[] hoc.host;
    hoc.host = tmp_host;
    hoc.totalNum_host = newNum;

#ifdef LOG_Running

    sprintf(Log_Message, "[realloc_HostOnCluster]\n[Cluster]\ntotalNum : %d\ncnt : %d",
            hoc.totalNum_host, hoc.cnt_host);
    writeLogMessage(fp_log, Log_Message);

#endif

    return true;
}

bool realloc_NetArray2D(struct NetArray2D &na2d, int addNum)
{
    if(addNum <= 0)
    {
        cout<<"Wrong number in realloc_NetArray2D().\n"<<endl;
        return false;
    }
    int cnt = na2d.cnt;
    int newOrder = na2d.cnt + addNum;
    int i, j;
    unsigned long long * line = NULL;
    unsigned long long **tmp_fcstVolunm = new unsigned long long *[newOrder];
    if(NULL == tmp_fcstVolunm)
    {
        perror("Null pointer---tmp_fcstVolunm ");
        return false;
    }


    for(i = 0; i < cnt; ++i)
    {
        line = new unsigned long long[newOrder];
        if(NULL == line)
        {
            perror("Null pointer---line ");
            return false;
        }
        for(j = 0; j < cnt; ++j)
        {
            line[j] = na2d.fcstVolumn[i][j];
        }
        delete[] na2d.fcstVolumn[i];
        tmp_fcstVolunm[i] = line;
    }
    delete[] na2d.fcstVolumn;

    for(i = cnt; i < newOrder; ++i)
    {
        line = new unsigned long long[newOrder];
        if(NULL == line)
        {
            perror("Null pointer---line ");
            return false;
        }
        tmp_fcstVolunm[i] = line;
    }


    na2d.fcstVolumn = tmp_fcstVolunm;
    na2d.totalNum = newOrder;


#ifdef LOG_Running

    sprintf(Log_Message, "[realloc_NetArray2D]\n[NetArray2D]\ntotalNum : %d\ncnt : %d",
            na2d.totalNum, na2d.cnt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return true;
}

bool realloc_Cluster(struct HostOnCluster &hoc, struct NetArray2D &na2d, int addNum)
{
    if(addNum <= 0)
    {
        cout<<"Wrong number in realloc_Cluster().\n"<<endl;
        return false;
    }

    return realloc_HostOnCluster(hoc, addNum) && realloc_NetArray2D(na2d, addNum);

}


bool realloc_NodeInfo(struct NodeInfo &ndif, int addNum)
{
    if(addNum <= 0)
    {
        cout<<"Wrong number in realloc_NodeInfo().\n"<<endl;
        return false;
    }
    int cnt = ndif.cnt_attempt;
    int newNum = ndif.totalNum_attempt + addNum;
    iString * tmp_iString = new iString[newNum];
    if(NULL == tmp_iString)
    {
        perror("Null pointer---tmp_iString ");
        return false;
    }

    int i;
    for(i = 0; i < cnt; ++i)
    {
        strcpy(tmp_iString[i], ndif.attemptOnNode[i]);
    }

    delete[] ndif.attemptOnNode;
    ndif.attemptOnNode = tmp_iString;
    ndif.totalNum_attempt = newNum;

#ifdef LOG_Running

    sprintf(Log_Message, "[realloc_NodeInfo]\n[NodeInfo]\ntotalNum : %d\ncnt : %d",
            ndif.totalNum_attempt, ndif.cnt_attempt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return true;
}



/// add host
int addHostToNetArray2D(struct NetArray2D &na2d, in_addr_t addr)
{
    if(na2d.cnt >= na2d.totalNum)
    {
        if(realloc_NetArray2D(na2d, ADD_HOST_NUM))
        {
            cout<<"error in realloc_NetArray2D().\n"<<endl;
            exit(EXIT_FAILURE);
        }
    }
    int seq = na2d.cnt;
    int i;
    for(i = 0; i < seq; ++i)
    {
        na2d.fcstVolumn[seq][i] = 0;
        na2d.fcstVolumn[i][seq] = 0;
    }
    na2d.fcstVolumn[seq][seq] = 0;
    ++na2d.cnt;

    return seq;
}

int addHostToHostOnCluster(struct HostOnCluster &hoc, in_addr_t addr)
{
    if(hoc.cnt_host >= hoc.totalNum_host)
    {
        if(realloc_HostOnCluster(hoc, ADD_HOST_NUM))
        {
            cout<<"error in realloc_HostOnCluster().\n"<<endl;
            exit(EXIT_FAILURE);
        }
    }
    int seq = hoc.cnt_host;
    hoc.host[seq].addr              = addr;
    hoc.host[seq].attemptOnNode     = NULL;
    hoc.host[seq].cnt_attempt       = 0;
    hoc.host[seq].totalNum_attempt  = 0;
    ++hoc.cnt_host;

    return seq;
}

int addHostToCluster(struct HostOnCluster &hoc, struct NetArray2D &na2d, in_addr_t addr)
{
    if(hoc.cnt_host != na2d.cnt || hoc.totalNum_host != na2d.totalNum)
    {
        cout<<"different num in HostOncluster and NetArray2D.\n"<<endl;
        exit(EXIT_FAILURE);
    }

    int seq_host = -1;
    int seq_net  = -1;

    seq_host = addHostToHostOnCluster(hoc, addr);
    seq_net  = addHostToNetArray2D(na2d, addr);

    if(seq_host != seq_net)
    {
        cout<<"error in addHostToCluster().\n"<<endl;
        exit(EXIT_FAILURE);
    }

#ifdef LOG_Running

    struct in_addr addHostIp;
    addHostIp.s_addr = addr;
    sprintf(Log_Message,
            "[addHostToCluster]\n[Cluster]\nadd host : %s\n[HostOnCluster]\ntotalNum : %d\ncnt : %d\n[NetArray2D]\ntotalNum : %d\ncnt : %d",
            inet_ntoa(addHostIp),
            hoc.totalNum_host, hoc.cnt_host,
            na2d.totalNum, na2d.cnt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return  seq_host;
}

/// add attempt
void addAttemptToAttemptArray(struct HostOnCluster &hoc, in_addr_t hostIp,
                              char * attempt, struct NetArray2D &na2d)
{
    int findHostSeqWithAddr(struct HostOnCluster &hoc, in_addr_t addr);
    int addHostToCluster(struct HostOnCluster &hoc, struct NetArray2D &na2d, in_addr_t addr);

    int seq_host = findHostSeqWithAddr(hoc, hostIp);
    if(-1 == seq_host)
    {
        struct in_addr notFound;
        notFound.s_addr = hostIp;
        printf("[WARNING] do not find host with %s\nAdd it to cluster.\n\n", inet_ntoa(notFound));

#ifdef LOG_Running

        sprintf(Log_Message, "[WARNING] do not find host with %s\nAdd it to cluster.\n\n",
                inet_ntoa(notFound));
        writeLogMessage(fp_log, Log_Message);

#endif
        /// add to Cluster
        seq_host = addHostToCluster(hoc, na2d, hostIp);

    }

    struct NodeInfo &tmp_nodeInfo = hoc.host[seq_host];

    /// first time to add attempt.
    if(NULL == tmp_nodeInfo.attemptOnNode)
    {
        iString * tmp = new iString[INIT_ATTEMPT_NUM_PER_HOST];
        if(NULL == tmp)
        {
            cout<<"Null pointer---iString in addAttemptToAttemptArray(first time add).\n"<<endl;
            exit(EXIT_FAILURE);
        }
        tmp_nodeInfo.attemptOnNode = tmp;
        tmp_nodeInfo.cnt_attempt = 0;
        tmp_nodeInfo.totalNum_attempt = INIT_ATTEMPT_NUM_PER_HOST;
    }

    /// not enough space to add
    if(tmp_nodeInfo.cnt_attempt >= tmp_nodeInfo.totalNum_attempt)
    {
        if(!realloc_NodeInfo(tmp_nodeInfo, ADD_ATTEMPT_NUM_PER_HOST))
        {
            cout<<"error in realloc_NodeInfo() from addAttemptToAttemptArray().\n"<<endl;
            exit(EXIT_FAILURE);
        }
    }

    int seq_att = tmp_nodeInfo.cnt_attempt;
    strcpy(tmp_nodeInfo.attemptOnNode[seq_att], attempt);
    //cout<<hoc.host[seq_host].attemptOnNode[seq_att]<<endl;
    ++tmp_nodeInfo.cnt_attempt;

#ifdef LOG_Running

    struct in_addr addHostIp;
    addHostIp.s_addr = hostIp;
    sprintf(Log_Message,
            "[addAttemptToAttemptArray]\n[NodeInfo]\nadd host : %s\nadd item : %s\ntotalNum : %d\ncnt : %d",
            inet_ntoa(addHostIp), attempt,
            tmp_nodeInfo.totalNum_attempt, tmp_nodeInfo.cnt_attempt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return ;
}



void removeHostFromNetArray2D(struct NetArray2D &na2d, int seq)
{
    int last = na2d.cnt - 1;
    int cnt = na2d.cnt;
    int i;

    if(seq != last)
    {
        for(i = 0; i < cnt; ++i)
        {
            na2d.fcstVolumn[seq][i] = na2d.fcstVolumn[last][i];
            na2d.fcstVolumn[i][seq] = na2d.fcstVolumn[i][last];
        }
        na2d.fcstVolumn[seq][seq] = 0;
    }

    for(i = 0; i < last; ++i)
    {
        na2d.fcstVolumn[last][i] = 0;
        na2d.fcstVolumn[i][last] = 0;
    }
    na2d.fcstVolumn[last][last] = 0;


    --na2d.cnt;
}

void removeHostFromHostOnCluster(struct HostOnCluster &hoc, int seq)
{
    int last = hoc.cnt_host -1;

    delete[] hoc.host[seq].attemptOnNode;

    if(seq != last)
    {
        hoc.host[seq].addr              = hoc.host[last].addr;
        hoc.host[seq].attemptOnNode     = hoc.host[last].attemptOnNode;
        hoc.host[seq].cnt_attempt       = hoc.host[last].cnt_attempt;
        hoc.host[seq].totalNum_attempt  = hoc.host[last].totalNum_attempt;
    }

    hoc.host[last].cnt_attempt = 0;
    hoc.host[last].totalNum_attempt = 0;
    hoc.host[last].addr = 0x00000000;
    hoc.host[last].attemptOnNode = NULL;


    --hoc.cnt_host;
}

bool removeHostFromCluster(struct HostOnCluster &hoc, struct NetArray2D &na2d, in_addr_t addr)
{
    int findHostSeqWithAddr(struct HostOnCluster &hoc, in_addr_t addr);


    int seq = findHostSeqWithAddr(hoc, addr);
    if(-1 == seq)
    {
        cout<<"error addr in removeHostFromCluster().\n"<<endl;
        return false;
    }
    removeHostFromHostOnCluster(hoc, seq);
    removeHostFromNetArray2D(na2d, seq);

#ifdef LOG_Running

    struct in_addr addHostIp;
    addHostIp.s_addr = addr;
    sprintf(Log_Message,
            "[removeHostFromCluster]\n[Cluster]\nremove host : %s\n[HostOnCluster]\ntotalNum : %d\ncnt : %d\n[NetArray2D]\ntotalNum : %d\ncnt : %d",
            inet_ntoa(addHostIp),
            hoc.totalNum_host, hoc.cnt_host,
            na2d.totalNum, na2d.cnt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return true;
}


void removeAttemptFromAttemptArray(struct HostOnCluster &hoc, char * appId)
{
    /// some wrong here.

    void removeAttemptFromAttemptArray_seq(struct NodeInfo & ndif, int seq);

    if(NULL == appId)
    {
        cout<<"Null pointer---appId in removeAttemptFromAttemptArray().\n"<<endl;
        return ;
    }
    char jobId[48] = "";
    strcpy(jobId, appId);
    int i,j;
    int cnt_hoc = hoc.cnt_host;

    struct NodeInfo * tmp = NULL;

    for(i = 0; i < cnt_hoc; ++i)
    {
        tmp = &hoc.host[i];
        for(j = 0; j < tmp->cnt_attempt; ++j)
        {
            //cout<<tmp->cnt_attempt<<endl;
            if(strstr(tmp->attemptOnNode[j], jobId))
            {

#ifdef LOG_Running

                struct in_addr removeHostIp;
                removeHostIp.s_addr = hoc.host[i].addr;
                sprintf(Log_Message,
                        "[removeAttemptFromAttemptArray]\n[NodeInfo]\nfrom host : %s\nremove item : %s\ntotalNum : %d\ncnt : %d",
                        inet_ntoa(removeHostIp), hoc.host[i].attemptOnNode[j],
                        tmp->totalNum_attempt, tmp->cnt_attempt - 1);
                writeLogMessage(fp_log, Log_Message);

#endif

                removeAttemptFromAttemptArray_seq(*tmp, j);
                --j;

            }
        }
    }




    return ;
}

void removeAttemptFromAttemptArray_seq(struct NodeInfo & ndif, int seq)
{
    if(seq < 0 || seq >= ndif.cnt_attempt)
    {
        cout<<"error seqNum in removeAttemptFromAttemptArray_seq().\n"<<endl;
        return ;
    }
    if(ndif.totalNum_attempt <= 0)
    {
        cout<<"error totalNum_attempt in removeAttemptFromAttemptArray_seq().\n"<<endl;
        return ;
    }

    int last = ndif.cnt_attempt - 1;
    if(seq != last)
    {
        //cout<<"not last\n"<<endl;
        strcpy(ndif.attemptOnNode[seq], ndif.attemptOnNode[last]);
    }
    ndif.attemptOnNode[last][0] = '\0';

    --ndif.cnt_attempt;

    return ;
}


/// return the sequence number of host for the given attempt in hoc.
/// return -1 if host on which the given attempt is does not exit.
int findHostSeqWithAttempt(struct HostOnCluster &hoc, char * attempt)
{
    int seq = -1;
    int i, j;
    int num = hoc.cnt_host;
    int num_att = 0;

    for(i = 0; i < num; ++i)
    {
        num_att = hoc.host[i].cnt_attempt;
        iString * tmp = hoc.host[i].attemptOnNode;
        for(j = 0; j < num_att; ++j)
        {
            if(0 == strcmp(tmp[j], attempt))
            {
                seq = i;
                return seq;
            }
        }
    }
    return seq;
}

/// return the sequence number for the given addr in hoc.
/// return -1 if host with the given addr does not exit.
int findHostSeqWithAddr(struct HostOnCluster &hoc, in_addr_t addr)
{
    int seq = -1;
    int i;
    int num = hoc.cnt_host;

    for(i = 0; i < num; ++i)
    {
        if(addr == hoc.host[i].addr)
        {
            seq = i;
            return seq;
        }
    }


    return seq;
}




void addVolumnToNetArray2D(struct NetArray2D &na2d, int src, int dst, unsigned long long vol)
{
    na2d.fcstVolumn[src][dst] += vol;

#ifdef LOG_Running

    sprintf(Log_Message, "[addVolumnToNetArray2D]\n[NetArray2D]\nsrc : %d\ndst : %d\naddVol : %llu",
            src, dst, vol);
    writeLogMessage(fp_log, Log_Message);

#endif

}

void removeVolumnFromNetArray2D(struct NetArray2D &na2d, int src, int dst, unsigned long long vol)
{
    unsigned long long tmp = na2d.fcstVolumn[src][dst];
    if(tmp < vol)
    {
        cout<<"error num in removeVolumnFromNetArray2D().\n"<<endl;
        return ;
    }

    na2d.fcstVolumn[src][dst] -= vol;

#ifdef LOG_Running

    sprintf(Log_Message, "[removeVolumnFromNetArray2D]\n[NetArray2D]\nsrc : %d\ndst : %d\nremoveVol : %llu",
            src, dst, vol);
    writeLogMessage(fp_log, Log_Message);

#endif

    return ;
}





bool Init_AppSequenceTable_L(struct AppSequenceTable_L &appSeqTL, int initNum)
{
    if(initNum <= 0)
    {
        cout<<"Wrong Initial Num in Init_AppSequenceTable.\n"<<endl;
        return false;
    }

    appSeqTL.appLinkHead = NULL;
    appSeqTL.appLinkRear = NULL;

    struct AppSequence_L * tmp = new struct AppSequence_L;
    if(NULL == tmp)
    {
        perror("Null Pointer in Init_AppSequenceTable_L ");
        return false;
    }
    appSeqTL.appLinkHead = tmp;
    struct AppSequence_L *last = appSeqTL.appLinkHead;

    int i;
    for(i = 1; i < initNum; ++i)
    {

        tmp = new struct AppSequence_L;
        if(NULL == tmp)
        {
            perror("Null Pointer in Init_AppSequenceTable_L ");
            return false;
        }

        tmp->flow = NULL;
        tmp->flow_cnt = 0;
        tmp->flow_totalNum = 0;
        tmp->masterSocket = -1;
        tmp->next = NULL;

        last->next = tmp;
        last = tmp;

    }

    appSeqTL.totalNum = initNum;
    appSeqTL.cnt = 0;

#ifdef LOG

    cout<<"[Init_AppSequenceTable_L]"<<endl;
    printf("totalNum : %d\ncnt : %d\n\n", appSeqTL.totalNum, appSeqTL.cnt);
#endif

#ifdef LOG_Running

    sprintf(Log_Message, "[Init_AppSequenceTable_L]\ntotalNum : %d\ncnt : %d\n\n",
                appSeqTL.totalNum, appSeqTL.cnt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return true;
}

void release_AppSequenceTable_L(struct AppSequenceTable_L &appSeqTL,struct NetArray2D &na2d)
{
    void removeAllFlowFromAppSequence_L(struct AppSequence_L &appSeq_L, struct NetArray2D &na2d);

    if(NULL == appSeqTL.appLinkHead)
    {
        return ;
    }

    int i;
    struct AppSequence_L * tmp = appSeqTL.appLinkHead;
    struct AppSequence_L * del = NULL;
    for(i = 0; NULL !=tmp ; ++i)
    {
        del = tmp;

        /// fix potential bug
        removeAllFlowFromAppSequence_L(*del, na2d);
        tmp = del->next;
        delete del;
    }

    if(i != appSeqTL.totalNum)
    {
        cout<<"[WARN]"<<endl;
        cout<<"Link is not all clean in release_AppSequenceTable_L.\n"<<endl;
    }

    appSeqTL.appLinkHead = NULL;
    appSeqTL.appLinkRear = NULL;
    appSeqTL.cnt = 0;
    appSeqTL.totalNum = 0;

#ifdef LOG
    cout<<"[release_AppSequenceTable_L]"<<endl;
    printf("totalNum : %d\ncnt : %d\n\n", appSeqTL.totalNum, appSeqTL.cnt);
#endif

#ifdef LOG_Running

    sprintf(Log_Message, "[release_AppSequenceTable_L]\ntotalNum : %d\ncnt : %d\n\n",
                appSeqTL.totalNum, appSeqTL.cnt);
    writeLogMessage(fp_log, Log_Message);

#endif

}

bool realloc_AppSequenceTable_L(struct AppSequenceTable_L &appSeqTL, int addNum)
{


    if(NULL == appSeqTL.appLinkHead || NULL == appSeqTL.appLinkRear)
    {
        perror("Null Pointer in realloc_AppSequenceTable_L ");
        return false;
    }
    if(addNum <= 0)
    {
        cout<<"Wrong Initial Num in Init_AppSequenceTable.\n"<<endl;
        return false;
    }

    struct AppSequence_L *last = appSeqTL.appLinkRear;
    struct AppSequence_L * tmp = NULL;

    int i;
    for(i = 0; i < addNum; ++i)
    {
        tmp = new struct AppSequence_L;
        if(NULL == tmp)
        {
            perror("Null Pointer in realloc_AppSequenceTable_L ");
            return false;
        }
        tmp->flow = NULL;
        tmp->flow_cnt = 0;
        tmp->flow_totalNum = 0;
        tmp->masterSocket = -1;
        tmp->next = NULL;

        last->next = tmp;
        last = tmp;
    }

    appSeqTL.totalNum +=addNum;

#ifdef LOG

    cout<<"[realloc_AppSequenceTable_L]"<<endl;
    printf("totalNum : %d\ncnt : %d\n\n", appSeqTL.totalNum, appSeqTL.cnt);
#endif

#ifdef LOG_Running

    sprintf(Log_Message, "[realloc_AppSequenceTable_L]\ntotalNum : %d\ncnt : %d\n\n",
                appSeqTL.totalNum, appSeqTL.cnt);
    writeLogMessage(fp_log, Log_Message);

#endif


    return true;
}

struct AppSequence_L * addAppToAppSeqTable_L(struct AppSequenceTable_L &appSeqTL, char *appName, int socket)
{
    if(NULL == appName)
    {
        perror("Null Pointer in addAppToAppSeqTable_L() ");
        return NULL;
    }

    if(-1 == socket)
    {
        perror("Error socket in addAppToAppSeqTable_L() ");
        return NULL;
    }

    if(appSeqTL.cnt >= appSeqTL.totalNum)
    {
        if(!realloc_AppSequenceTable_L(appSeqTL, ADD_AppSeqTable_L_NUM))
        {
            cout<<"Error in realloc_AppSequenceTable_L().\n"<<endl;
            exit(EXIT_FAILURE);
        }
    }

    struct AppSequence_L * addApp = NULL;

    /// link is null
    if(NULL == appSeqTL.appLinkRear)
    {
        addApp = appSeqTL.appLinkHead;
    }
    /// link is not null
    else
    {
        addApp = appSeqTL.appLinkRear->next;
        /// unique check
        struct AppSequence_L *tmp = appSeqTL.appLinkHead;
        struct AppSequence_L *firstBlank = appSeqTL.appLinkRear->next;

        while(tmp != firstBlank)
        {
            if(0 == strcmp(appName, tmp->appName))
            {
                printf("%s is already ADDED.\n\n", appName);
                return NULL;
            }
            tmp = tmp->next;
        }
    }


    strcpy(addApp->appName, appName);
    addApp->masterSocket = socket;
    appSeqTL.appLinkRear = addApp;


    ++appSeqTL.cnt;

#ifdef LOG

    cout<<"[addAppToAppSeqTable_L]"<<endl;
    printf("app : %s\ntotalNum : %d\ncnt : %d\n\n", addApp->appName, appSeqTL.totalNum, appSeqTL.cnt);

#endif

#ifdef LOG_Running

    sprintf(Log_Message, "[addAppToAppSeqTable_L]\napp : %s\ntotalNum : %d\ncnt : %d\n\n",
                addApp->appName, appSeqTL.totalNum, appSeqTL.cnt);
    writeLogMessage(fp_log, Log_Message);

#endif


    return addApp;
}

void removeAppToAppSeqTable_L(struct AppSequenceTable_L &appSeqTL, char *appName, struct NetArray2D &na2d)
{
    void removeAllFlowFromAppSequence_L(struct AppSequence_L &appSeq_L, struct NetArray2D &na2d);
    if(NULL == appName)
    {
        perror("Null Pointer in removeAppToAppSeqTable_L() ");
        return ;
    }

    if(NULL == appSeqTL.appLinkRear)
    {
        printf("Link is NULL.\n\n");
        return ;
    }

    int i;

    struct AppSequence_L *former = NULL;
    struct AppSequence_L *search_app = appSeqTL.appLinkHead;
    struct AppSequence_L *firstBlank = appSeqTL.appLinkRear->next;
    for(i = 0; search_app != firstBlank; ++i)
    {
        if(0 == strcmp(search_app->appName, appName))
        {
            break;
        }

        former = search_app;
        search_app = search_app->next;
    }
    if(i == appSeqTL.cnt)
    {
        printf("\"%s\" NOT found.\n\n", appName);
        return ;
    }

    /// fix potential bug
    /// Server may not receive a flow over message so a flow record may still
    /// exit in NetArray2D
    /// So when we remove an App, delete those flows from  NetArray2D
    ///
    removeAllFlowFromAppSequence_L(*search_app, na2d);


#ifdef LOG

    cout<<"[removeAppToAppSeqTable_L]"<<endl;
    printf("app : %s\ntotalNum : %d\ncnt : %d\n\n", search_app->appName, appSeqTL.totalNum, appSeqTL.cnt - 1);

#endif

#ifdef LOG_Running

    sprintf(Log_Message, "[removeAppToAppSeqTable_L]\napp : %s\ntotalNum : %d\ncnt : %d\n\n",
                search_app->appName, appSeqTL.totalNum, appSeqTL.cnt - 1);
    writeLogMessage(fp_log, Log_Message);

#endif

    /// only one app in the link
    if(1 == appSeqTL.cnt)
    {
        //cout<<"only one.\n"<<endl;
        appSeqTL.appLinkRear = NULL;
    }
    /// the app to remove is on the rear of the link
    else if(i == appSeqTL.cnt - 1)
    {
        //cout<<"rear.\n"<<endl;
        appSeqTL.appLinkRear = former;
    }
    /// the app to remove is on the head of the link
    else if(i == 0)
    {
        //cout<<"head.\n"<<endl;
        appSeqTL.appLinkHead = search_app->next;
        appSeqTL.appLinkRear->next = search_app;
        search_app->next = firstBlank;
    }
    else if(i < appSeqTL.cnt - 1)
    {
        former->next = search_app->next;
        appSeqTL.appLinkRear->next = search_app;
        search_app->next = firstBlank;
    }


    --appSeqTL.cnt;

}

struct AppSequence_L * findAppPointerinAppSeqTable_L(struct AppSequenceTable_L &appSeqTL, char *attempt)
{
    if(NULL == attempt)
    {
        perror("Null Pointer in findAppPointerinAppSeqTable_L() ");
        return NULL;
    }
    if(NULL == appSeqTL.appLinkRear)
    {
        printf("Link is NULL.\n\n");
        return NULL;
    }

    struct AppSequence_L * search_app = appSeqTL.appLinkHead;
    struct AppSequence_L * firstBlank = appSeqTL.appLinkRear->next;

    int i;
    for(i = 0; search_app != firstBlank; ++i)
    {
        if(NULL != strstr(attempt, search_app->appName + 13))
        {
            break;
        }
        search_app = search_app->next;
    }
    if(i == appSeqTL.cnt)
    {
        printf("App with [%s] is NOT found.\n\n", attempt);
        return NULL;
    }

    return search_app;

}


/// flow operations

int addFlowToAppSequence_L(struct AppSequence_L &appSeq_L, struct HWFlowStruct addFlow, struct NetArray2D &na2d)
{
    void addVolumnToNetArray2D(struct NetArray2D &na2d, int src, int dst, unsigned long long vol);

    /// first time to add a flow to an Application
    if(NULL == appSeq_L.flow)
    {
        struct HWFlowStruct *tmp_flow = new struct HWFlowStruct[INIT_FLOW_NUM];
        if(NULL == tmp_flow)
        {
            perror("Null Pointer in addFlowToAppSeqTable() ");
            return false;
        }
        appSeq_L.flow = tmp_flow;
        appSeq_L.flow_cnt = 0;
        appSeq_L.flow_totalNum = INIT_FLOW_NUM;
    }
    /// flow current num to an Application is about to run out, so realloc flowTable;
    else if(appSeq_L.flow_cnt >= appSeq_L.flow_totalNum)
    {
        int flow_newNum = appSeq_L.flow_totalNum + ADD_FLOW_NUM;
        struct HWFlowStruct *tmp_flow = new struct HWFlowStruct[flow_newNum];

        if(NULL == tmp_flow)
        {
            perror("Null Pointer in addFlowToAppSeqTable() ");
            return false;
        }

        int tmp_cnt = appSeq_L.flow_cnt;
        int i;
        for(i = 0; i < tmp_cnt; ++i)
        {
            tmp_flow[i] = appSeq_L.flow[i];
        }

        delete[] appSeq_L.flow;
        appSeq_L.flow_totalNum = flow_newNum;
        appSeq_L.flow = tmp_flow;

    }

    /// add flow
    int seq = appSeq_L.flow_cnt;
    appSeq_L.flow[seq] = addFlow;
    addVolumnToNetArray2D(na2d, addFlow.src, addFlow.dst, addFlow.vol);

    ++appSeq_L.flow_cnt;

#ifdef LOG

    cout<<"[addFlowToAppSequence_L]"<<endl;
    printf("app : %s\ntotalNum : %d\ncnt : %d\n\n", appSeq_L.appName, appSeq_L.flow_totalNum, appSeq_L.flow_cnt);

#endif

#ifdef LOG_Running

    sprintf(Log_Message, "[addFlowToAppSequence_L]\napp : %s\ntotalNum : %d\ncnt : %d\n\n",
                appSeq_L.appName, appSeq_L.flow_totalNum, appSeq_L.flow_cnt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return seq;
}

bool flowEquals(struct HWFlowStruct f1, struct HWFlowStruct f2)
{
    return f1.src == f2.src && f1.dst == f2.dst && f1.vol == f2.vol;
}

int removeFlowFromAppSequence_L(struct AppSequence_L &appSeq_L, struct HWFlowStruct removeFlow, struct NetArray2D & na2d)
{
    void removeVolumnFromNetArray2D(struct NetArray2D &na2d, int src, int dst, unsigned long long vol);

    int flowCnt = appSeq_L.flow_cnt;
    int last = flowCnt -1;
    int i;
    int seq = -1;

    for(i = 0; i < flowCnt; ++i)
    {
        if(flowEquals(removeFlow, appSeq_L.flow[i]))
        {
            seq = i;
            break;
        }
    }

    if(-1 == seq)
    {
        cout<<"[WARN]\nFlow is not found.\n"<<endl;
        return seq;
    }
    /// remove flow
    removeVolumnFromNetArray2D(na2d, removeFlow.src, removeFlow.dst, removeFlow.vol);
    if(seq != last)
    {
        appSeq_L.flow[seq] = appSeq_L.flow[last];
    }

    --appSeq_L.flow_cnt;

#ifdef LOG

    cout<<"[removeFlowFromAppSequence_L]"<<endl;
    printf("app : %s\ntotalNum : %d\ncnt : %d\n\n", appSeq_L.appName, appSeq_L.flow_totalNum, appSeq_L.flow_cnt);

#endif

#ifdef LOG_Running

    sprintf(Log_Message, "[removeFlowFromAppSequence_L]\napp : %s\ntotalNum : %d\ncnt : %d\n\n",
                appSeq_L.appName, appSeq_L.flow_totalNum, appSeq_L.flow_cnt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return seq;
}

void removeAllFlowFromAppSequence_L(struct AppSequence_L &appSeq_L, struct NetArray2D &na2d)
{
    if(0 == appSeq_L.flow_cnt)
        return ;
    int i;
    int cnt = appSeq_L.flow_cnt;
    for(i = 0; i < cnt; ++i)
    {
        removeVolumnFromNetArray2D(na2d, appSeq_L.flow[i].src, appSeq_L.flow[i].dst, appSeq_L.flow[i].vol);
    }

    delete[] appSeq_L.flow;
    appSeq_L.flow = NULL;
    appSeq_L.flow_cnt = 0;
    appSeq_L.flow_totalNum = 0;
    appSeq_L.masterSocket = -1;

#ifdef LOG_Running

    sprintf(Log_Message, "[removeAllFlowFromAppSequence_L]\napp : %s\ntotalNum : %d\ncnt : %d\n\n",
                appSeq_L.appName, appSeq_L.flow_totalNum, appSeq_L.flow_cnt);
    writeLogMessage(fp_log, Log_Message);

#endif

    return ;
}







bool dealWithMessage(char * message, in_addr_t fromAddr, struct HostOnCluster &hoc,
                        struct NetArray2D &na2d, struct AppSequenceTable_L &appSeqT_L, int conn_socket)
{
    if(NULL == message || 0 == strcmp("", message))
    {
        cout<<"[WARN] wrong message.\n"<<endl;
        return false;
    }
    char copy_message[LEN_message];
    //char flag[24];
    //char info[LEN_message];
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
        /// agent regists
    case host_REGISTER:
    {
        addHostToCluster(hoc, na2d, fromAddr);
        break;
    }
    /// agent log out
    case host_LOGOUT:
    {
        removeHostFromCluster(hoc, na2d, fromAddr);
        break;
    }
    /// about to shuffle a flow
    case flow_START:
    {
        int src = findHostSeqWithAddr(hoc, fromAddr);
        if(-1 == src)
        {
            cout<<"[WARN] can not find src in [cluster].\n"<<endl;
            break;
        }
        int dst = -1;
        unsigned long long vol = 0;

        /// get attempt name
        p = strtok(NULL, ";");
        dst = findHostSeqWithAttempt(hoc, p);

        if(-1 == dst)
        {
            //cout<<"[WARN] can not find dst : %s in [cluster].\n"<<endl;
            printf("[WARN] can not find dst : %s in [cluster].\n", p);
            break;
        }

        struct AppSequence_L *belongToApp = findAppPointerinAppSeqTable_L(appSeqT_L, p);
        if(NULL == belongToApp)
        {
            printf("[WARN] can not find belonging App : %s in [cluster].\n", p);
            break;
        }

        /// get volumn
        p = strtok(NULL, ";");
        sscanf(p,"%llu", &vol);
        struct HWFlowStruct addFlow;
        addFlow.src = src;
        addFlow.dst = dst;
        addFlow.vol = vol;

        addFlowToAppSequence_L(*belongToApp, addFlow, na2d);

        //addVolumnToNetArray2D(na2d, src, dst, vol);

        break;

    }
    /// flow is over
    case flow_OVER:
    {
        int src = findHostSeqWithAddr(hoc, fromAddr);
        if(-1 == src)
        {
            cout<<"[WARN] can not find src in [cluster].\n"<<endl;
            break;
        }
        int dst = -1;
        unsigned long long vol = 0;

        /// get attempt name
        p = strtok(NULL, ";");
        dst = findHostSeqWithAttempt(hoc, p);
        if(-1 == dst)
        {
            //cout<<"[WARN] can not find dst : %s in [cluster].\n"<<endl;
            printf("[WARN] can not find dst : %s in [cluster].\n", p);
            break;
        }

        struct AppSequence_L *belongToApp = findAppPointerinAppSeqTable_L(appSeqT_L, p);
        if(NULL == belongToApp)
        {
            printf("[WARN] can not find belonging App : %s in [cluster].\n", p);
            break;
        }
        /// get volumn
        p = strtok(NULL, ";");
        sscanf(p,"%llu", &vol);

        struct HWFlowStruct removeFlow;
        removeFlow.src = src;
        removeFlow.dst = dst;
        removeFlow.vol = vol;

        removeFlowFromAppSequence_L(*belongToApp, removeFlow, na2d);

        break;
    }
    case reduce_OVER:
    {

        break;
    }

    /// attempt is create
    case attempt_CREATE:
    {
        /// get attempt num
        p = strtok(NULL, ";");
        addAttemptToAttemptArray(hoc, fromAddr, p, na2d);
        break;

    }
    case app_CREATE:
    {
        p = strtok(NULL, ";");
        //cout<<p<<endl<<endl;
        addAppToAppSeqTable_L(appSeqT_L, p, conn_socket);

        break;
    }
    /// app is over and remove attempt
    case app_OVER:
    {
        p = strtok(NULL, ";");
        char appId[48] = "";
        strcpy(appId, p + 13);
        removeAttemptFromAttemptArray(hoc, appId);
        removeAppToAppSeqTable_L(appSeqT_L, p, na2d);
        break;

    }

    default:
    {
        cout<<"error Flag in message.\n"<<endl;
        break;
    }
    }




    return true;
}




bool Init_Server(   int &listen_fd, struct sockaddr_in  &servaddr,
                    int &epoll_fd,  struct epoll_event  &ev,
                    int &cur_fds,   struct rlimit       &rlt
                )
{

    rlt.rlim_max = rlt.rlim_cur = MAXEPOLL;

    if(-1 == setrlimit(RLIMIT_NOFILE, &rlt))
    {
        perror("setrlimit ");
        return false;
    }
    ///socket
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(serv_PORT);


    ///create socket
    if(-1 == (listen_fd = socket(AF_INET, SOCK_STREAM, 0)))
    {
        perror("socket ");
        return false;
    }

    ///set nonblocking
    if(-1 == fcntl(listen_fd, F_SETFL, fcntl(listen_fd, F_GETFD, 0) | O_NONBLOCK))
    {
        perror("fcntl ");
        return false;
    }

    ///reuse port
    int yes = 1;
    if(-1 == setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)))
    {
        perror("setsockopt ");
        return false;
    }

    ///bind
    if(-1 == bind(listen_fd, (struct sockaddr *)&servaddr, sizeof(struct sockaddr)))
    {
        perror("bind ");
        return false;
    }

    ///listen
    if(-1 == listen(listen_fd, MAXBACK))
    {
        perror("listen ");
        return false;
    }

    ///create epoll
    epoll_fd = epoll_create(MAXEPOLL);
    if(-1 == epoll_fd)
    {
        perror("epoll_fd ");
        return false;
    }
    /// listen
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = listen_fd;
    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0)
    {
        perror("epoll_ctl ");
        exit(EXIT_FAILURE);
    }
    /// listen the console
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = STDIN_FILENO;
    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, STDIN_FILENO, &ev) < 0)
    {
        perror("epoll_ctl ");
        exit(EXIT_FAILURE);
    }

    cur_fds = 2;

    cout<<"HadoopWatch server STARTED.\n"<<endl;
    cout<<"Press ENTER key to terminate.\n"<<endl;
    cout<<"Waiting for Information...\n"<<endl;

#ifdef LOG_Running

    sprintf(Log_Message, "[Init_Server]\nListen_fd : %d\nPort : %d\nepoll_fd : %d\ncur_fds : %d\n\n",
                listen_fd, ntohs(servaddr.sin_port), epoll_fd, cur_fds);
    writeLogMessage(fp_log, Log_Message);

#endif

    return true;
}




int main(int argc, char **argv)
{

    int     listen_fd;
    int     conn_fd;
    int     epoll_fd;
    int     nread;
    int     cur_fds;
    int     wait_fds;
    int     i;

    struct sockaddr_in      servaddr;
    struct sockaddr_in      cliaddr;

    struct epoll_event  ev;
    struct epoll_event  evs[MAXEPOLL];
    struct rlimit       rlt;
    char                recv_Message[MAXLINE];

    socklen_t sock_len = sizeof(struct sockaddr_in);

#ifdef LOG_Running

    char logPath[] = "./HW_SERVER_LOG.txt";

    fp_log = fopen(logPath, "w");
    if(NULL == fp_log)
    {
        perror("error in create log ");
        exit(EXIT_FAILURE);
    }
    fclose(fp_log);
    fp_log = fopen(logPath, "a");
    sprintf(Log_Message, "**Server START**");
    writeLogMessage(fp_log, Log_Message);

#endif

    if(!Init_Server(listen_fd, servaddr, epoll_fd, ev, cur_fds, rlt))
    {
        perror("Init_Server ");
        exit(EXIT_FAILURE);
    }


    struct HostOnCluster    hostOnCluster;
    struct NetArray2D       netForecastArray2D;

    struct AppSequenceTable_L   appSeqT_L;

    if(!Init_Cluster(hostOnCluster, netForecastArray2D, INIT_HOST_NUM, INIT_ATTEMPT_NUM_PER_HOST))
    {
        cout<<"error in Init_Cluster()\n"<<endl;
        exit(EXIT_FAILURE);
    }

    if(!Init_AppSequenceTable_L(appSeqT_L, INIT_AppSeqTable_L_NUM))
    {
        cout<<"error in Init_AppSequenceTable_L()\n"<<endl;
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in tmp_addr;

    while(1)
    {
        if(-1 == (wait_fds = epoll_wait(epoll_fd, evs, cur_fds, -1)))
        {
            perror("epoll_wait ");
            exit(EXIT_FAILURE);
        }

        for(i = 0; i < wait_fds; ++i)
        {
            if(evs[i].data.fd == listen_fd && cur_fds < MAXEPOLL)
            {
                /// new connection
                if(-1 == (conn_fd = accept(listen_fd, (struct sockaddr *)&cliaddr, &sock_len)))
                {
                    perror("accept ");
                    exit(EXIT_FAILURE);
                }

                printf("Get connection from client %s : %d, new socket : %d\n", (char *)inet_ntoa(cliaddr.sin_addr), cliaddr.sin_port, conn_fd);

                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = conn_fd;
                if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &ev) < 0)
                {
                    perror("epoll_ctl ");
                    exit(EXIT_FAILURE);
                }
                ++cur_fds;


                //char ss[24] = "registed!";
                //write(conn_fd, ss, 24);

                continue;

            }

            ///deal with console input
            if(evs[i].data.fd == STDIN_FILENO && cur_fds < MAXEPOLL)
            {
                char buf_console;
                while (read(STDIN_FILENO, &buf_console, 1) > 0 && buf_console != '\n')
                    continue;
                goto END;
            }

            ///deal with data
            memset(&tmp_addr, 0, sock_len);
            getpeername(evs[i].data.fd, (struct sockaddr *)&tmp_addr, &sock_len);
            memset(recv_Message, 0, sizeof(recv_Message));
            nread = read(evs[i].data.fd, recv_Message, sizeof(recv_Message));
            if(nread <= 0)      ///over or error
            {
                printf("Disconnected with socket %d : %s. so close it.\n", evs[i].data.fd, (char *)inet_ntoa(tmp_addr.sin_addr));
                close(evs[i].data.fd);
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, evs[i].data.fd, &ev);

                --cur_fds;
                continue;
            }

            printf("Received : %s\n", recv_Message);


            printf("From : %s\nsocket : %d\nnum : %d\n\n", (char *)inet_ntoa(tmp_addr.sin_addr), evs[i].data.fd, ++recv_Num);
            //write(evs[i].data.fd, buf, nread);


            dealWithMessage(recv_Message, tmp_addr.sin_addr.s_addr,
                            hostOnCluster, netForecastArray2D,
                            appSeqT_L, evs[i].data.fd);
        }
    }



END:
    ///end
    close(epoll_fd);
    close(listen_fd);
    close(STDIN_FILENO);

    /// end and clean
    /// watch out
    /// AppSequenceTable_L must be released before release_Cluster.
    /// because release_AppSequenceTable_L use netForecastArray2D during release.
    release_AppSequenceTable_L(appSeqT_L, netForecastArray2D);
    release_Cluster(hostOnCluster, netForecastArray2D);

    cout<<"HadoopWatch server STOPPED.\n"<<endl;

#ifdef LOG_Running

    sprintf(Log_Message, "**Server END**");
    writeLogMessage(fp_log, Log_Message);
    fclose(fp_log);

#endif

    return 0;
}
