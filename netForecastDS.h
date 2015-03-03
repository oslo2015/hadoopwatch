
typedef char iString[48];



#define INIT_HOST_NUM   40
#define INIT_ATTEMPT_NUM_PER_HOST   20

#define ADD_HOST_NUM    10
#define ADD_ATTEMPT_NUM_PER_HOST    10



struct NodeInfo
{
    in_addr_t   addr;
    iString     * attemptOnNode;
    int         cnt_attempt;
    int         totalNum_attempt;
    /**
    If cnt_attempt = i, it means that array[i] is available to add.
    cnt_attempt is the num how many items there are in this array.
    **/
};

struct HostOnCluster
{
    struct NodeInfo         *host;
    int                     cnt_host;
    int                     totalNum_host;
    /**
    If cnt_host = i, it means that array[i] is available to add.
    cnt_host is the num how many items there are in this array.
    **/
};

struct NetArray2D
{
    /// net[src][dst] = vol;
    unsigned long long  **fcstVolumn;
    int                 totalNum;  ///矩阵的阶
    int                 cnt;
    /**
    if cnt = i, it means that line i and column i are available to add.
    **/
};

#define INIT_AppSeqTable_L_NUM    15
#define ADD_AppSeqTable_L_NUM     7

#define INIT_FLOW_NUM    15
#define ADD_FLOW_NUM     7


struct HWFlowStruct
{
    int                     src;
    int                     dst;
    unsigned long long      vol;
};

struct AppSequence_L
{
    iString                 appName;
    struct HWFlowStruct     *flow;
    int                     flow_totalNum;
    int                     flow_cnt;
    int                     masterSocket;
    struct AppSequence_L    *next;
};

struct AppSequenceTable_L
{
    struct AppSequence_L  *appLinkHead;
    struct AppSequence_L  *appLinkRear;
    int                 totalNum;
    int                 cnt;
};


